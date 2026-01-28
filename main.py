import requests
import concurrent.futures
import os
import pandas as pd
import json
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
from cozepy import Coze, TokenAuth, COZE_CN_BASE_URL

# ================= 1. 配置与环境初始化 =================
print("🚀 [1/6] 正在初始化环境配置...")
if os.path.exists(".env"):
    load_dotenv()
    print("  - 已加载 .env 文件")

TOKEN = os.getenv("GITHUB_TOKEN")
FEISHU_WEBHOOK = os.getenv("FEISHU_WEBHOOK")
COZE_API_TOKEN = os.getenv("COZE_API_TOKEN")
COZE_WORKFLOW_ID = '7600384889276547126'

BASE_QUERY = "skills language:Python created:>2025-10-10 is:public stars:>100"
SPECIFIC_LICENSES = ["mit", "apache-2.0", "gpl-3.0", "0bsd", "cc0-1.0"]

DIR_TOTAL, DIR_CHANGES, DIR_LOGS = "Data_Total", "Data_Changes", "Logs"
MAJOR_TOTAL_CSV = os.path.join(DIR_TOTAL, "major_licenses_total.csv")
OTHER_TOTAL_CSV = os.path.join(DIR_TOTAL, "other_licenses_total.csv")
LOG_FILE = os.path.join(DIR_LOGS, "update_log.txt")

for folder in [DIR_TOTAL, DIR_CHANGES, DIR_LOGS]:
    if not os.path.exists(folder):
        os.makedirs(folder, exist_ok=True)
        print(f"  - 创建目录: {folder}")

# ================= 2. 核心工具函数 =================

def get_now_bj():
    return datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=8))).strftime("%Y-%m-%d %H:%M:%S")

def convert_to_bj_time(utc_str):
    if not utc_str: return ""
    try:
        utc_dt = datetime.strptime(utc_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        return utc_dt.astimezone(timezone(timedelta(hours=8))).strftime("%Y-%m-%d %H:%M:%S")
    except:
        return utc_str

def fetch_github_data(query_suffix):
    url = "https://api.github.com/search/repositories"
    full_query = f"{BASE_QUERY} {query_suffix}"
    params = {"q": full_query, "sort": "stars", "order": "desc", "per_page": 100}
    headers = {"Accept": "application/vnd.github+json", "User-Agent": "Monitor-Bot"}
    if TOKEN:
        headers["Authorization"] = f"Bearer {TOKEN}"
    try:
        res = requests.get(url, params=params, headers=headers, timeout=20)
        if res.status_code == 200:
            items = res.json().get('items', [])
            print(f"    - 查询 [{query_suffix}] 成功，获取到 {len(items)} 条数据")
            return items
        else:
            print(f"    - 查询 [{query_suffix}] 失败，状态码: {res.status_code}, 响应: {res.text}")
            return []
    except Exception as e:
        print(f"    - 查询 [{query_suffix}] 异常: {e}")
        return []

# ================= 3. Coze 工作流集成 (新增详细 Response 打印) =================

def run_coze_workflow(new_items):
    if not COZE_API_TOKEN or not new_items:
        print("⚠️ [Coze] 跳过触发: 未配置 Token 或无新增项目")
        return True

    # 初始化 Coze 客户端
    coze = Coze(auth=TokenAuth(token=COZE_API_TOKEN), base_url=COZE_CN_BASE_URL)
    repo_list_str = "\n".join([f"- {i['Name']}: {i['URL']}" for i in new_items])

    print(f"🤖 [Coze] 正在触发工作流，Workflow ID: {COZE_WORKFLOW_ID}")
    try:
        # 运行工作流
        resp = coze.workflows.runs.create(
            workflow_id=COZE_WORKFLOW_ID,
            parameters={"repo_info": repo_list_str}
        )
        
        # 打印详细返回内容
        # 这里的 resp 通常是 WorkflowRun 对象，包含 data 属性
        print("✅ [Coze] API 调用完成")
        if hasattr(resp, 'data'):
            print(f"  - 响应数据 (Data): {resp.data}")
        else:
            print(f"  - 原始响应: {resp}")
            
        return True
    except Exception as e:
        print(f"❌ [Coze] 触发失败")
        print(f"  - 错误详情: {str(e)}")
        # 如果是 HTTP 错误且能获取到 body，尝试打印出来
        if hasattr(e, 'response') and hasattr(e.response, 'text'):
            print(f"  - API 报错原文: {e.response.text}")
        return False

# ================= 4. 飞书推送逻辑 =================

def send_feishu_v2_card(new_major, new_other, update_count, total_major, total_other, all_logs, coze_success=True):
    if not FEISHU_WEBHOOK:
        print("⚠️ [飞书] 未配置 Webhook，跳过推送")
        return
    
    print("✉️ [飞书] 正在构建并发送推送卡片...")
    sync_content = "[已同步至飞书多维表格](https://bytedance.larkoffice.com/base/ObLQbDL5QaWfypsafgecLuhRn8f?from=from_copylink)" if coze_success else "❌ **同步失败 (Coze 工作流异常)**"

    total_new = len(new_major) + len(new_other)
    major_md = "\n".join([f"• [{i['Name']}]({i['URL']}) <font color='grey'>🐣{i['Created_At'][:10]}</font> **<font color='carmine'>★ {i['Stars']}</font>**" for i in new_major[:5]]) or "暂无新增"
    other_md = "\n".join([f"• [{i['Name']}]({i['URL']}) <font color='grey'>🐣{i['Created_At'][:10]}</font> <text_tag color='orange'>{i['License']}</text_tag>" for i in new_other[:5]]) or "暂无新增"
    log_preview = "\n".join([line.strip() for line in all_logs if line.strip()][:8])

    card_payload = {
        "msg_type": "interactive",
        "card": {
            "body": {
                "direction": "vertical",
                "elements": [
                    {"tag": "column_set", "flex_mode": "stretch", "horizontal_spacing": "12px",
                     "columns": [
                         {"tag": "column", "width": "weighted", "weight": 1, "background_style": "red-50", "padding": "12px",
                          "elements": [{"tag": "markdown", "content": "**<font color='red'>主流组</font>**"}, {"tag": "markdown", "content": major_md}]},
                         {"tag": "column", "width": "weighted", "weight": 1, "background_style": "orange-50", "padding": "12px",
                          "elements": [{"tag": "markdown", "content": "**<font color='orange'>非主流/无协议组</font>**"}, {"tag": "markdown", "content": other_md}]}
                     ]},
                    {"tag": "markdown", "content": f"🔄 **共有 {update_count} 个已知项目更新了指标**"},
                    {"tag": "markdown", "content" : sync_content},
                    {"tag": "markdown", "content": f"📝 **日志摘要：**\n{log_preview}"},
                    {"tag": "hr"},
                    {"tag": "markdown", "content": f"<font color='grey' size='small'>📊 累计项目：主流 {total_major} | 非主流 {total_other}\n📅 监控时刻：{get_now_bj()}</font>"}
                ]
            },
            "header": {
                "template": "red" if total_new > 0 else "blue",
                "title": {"content": f"GitHub 监控日报：发现 {total_new} 个新项目！", "tag": "plain_text"},
                "icon": {"tag": "standard_icon", "token": "code_outlined"}
            },
            "schema": "2.0"
        }
    }
    try:
        res = requests.post(FEISHU_WEBHOOK, json=card_payload, timeout=10)
        print(f"✅ [飞书] 推送完成，响应状态码: {res.status_code}")
    except Exception as e:
        print(f"❌ [飞书] 推送失败: {e}")

# ================= 5. 核心增量处理逻辑 =================

def process_incremental(new_list, file_path, label):
    print(f"🔍 [处理] 正在分析 {label} 分组数据...")
    now_bj = get_now_bj()
    new_df = pd.DataFrame([{
        'Repo_ID': i['id'], 'Name': i['full_name'], 'Stars': i['stargazers_count'],
        'License': i['license']['key'] if i['license'] else "None", 'URL': i['html_url'],
        'Created_At': convert_to_bj_time(i['created_at']), 'Updated_At': convert_to_bj_time(i['updated_at']),
        'Last_Grabbed_At': now_bj
    } for i in new_list])

    if not os.path.exists(file_path):
        print(f"  - [{label}] 首次初始化总表")
        new_df['First_Grabbed_At'] = now_bj
        new_df.to_csv(file_path, index=False, encoding='utf-8-sig')
        return [], 0, len(new_df), [f"[{now_bj}] {label} 首次初始化。"]

    old_df = pd.read_csv(file_path)
    old_df['Repo_ID'] = old_df['Repo_ID'].astype(int)

    new_mask = ~new_df['Repo_ID'].isin(old_df['Repo_ID'])
    new_items_df = new_df[new_mask].copy()
    if not new_items_df.empty:
        new_items_df['First_Grabbed_At'] = now_bj
        print(f"  - [{label}] 发现 {len(new_items_df)} 个新项目")

    merged = pd.merge(new_df, old_df, on='Repo_ID', suffixes=('_new', '_old'))
    changed_mask = (merged['Stars_new'] != merged['Stars_old']) | (merged['Updated_At_new'] != merged['Updated_At_old'])
    changed_items_raw = merged[changed_mask]
    if not changed_items_raw.empty:
        print(f"  - [{label}] 发现 {len(changed_items_raw)} 个项目有更新")

    first_grabbed_map = old_df.set_index('Repo_ID')['First_Grabbed_At'].to_dict()
    new_df['First_Grabbed_At'] = new_df['Repo_ID'].map(first_grabbed_map).fillna(now_bj)
    updated_total = pd.concat([new_df, old_df]).drop_duplicates('Repo_ID', keep='first')
    updated_total.to_csv(file_path, index=False, encoding='utf-8-sig')

    log_entries = [f"新增：{r['Name']} (★{r['Stars']})" for _, r in new_items_df.iterrows()]
    return new_items_df.to_dict('records'), len(changed_items_raw), len(updated_total), ([f"[{label}]"] + log_entries if log_entries else [])

# ================= 6. 主程序运行入口 =================

def main():
    print(f"--- 任务开始时间: {get_now_bj()} ---")
    if not TOKEN:
        print("❌ 错误: 未能在环境中找到 GITHUB_TOKEN")
        return

    print("🛰️ [2/6] 正在并发扫描 GitHub 数据...")
    spec_data, other_data = {}, {}
    tasks = {f"license:{lic}": "SPEC" for lic in SPECIFIC_LICENSES}
    tasks[" ".join([f"-license:{lic}" for lic in SPECIFIC_LICENSES])] = "OTHER"

    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
        f_to_q = {executor.submit(fetch_github_data, q): group for q, group in tasks.items()}
        for f in concurrent.futures.as_completed(f_to_q):
            group = f_to_q[f]
            for item in f.result():
                (spec_data if group == "SPEC" else other_data)[item['id']] = item

    print("📊 [3/6] 开始比对增量...")
    new_spec, upd_spec, tot_spec, logs_spec = process_incremental(list(spec_data.values()), MAJOR_TOTAL_CSV, "Major")
    new_other, upd_other, tot_other, logs_other = process_incremental(list(other_data.values()), OTHER_TOTAL_CSV, "Other")

    print("🤖 [4/6] 执行 Coze 流程...")
    all_new_items = new_spec + new_other
    coze_status = run_coze_workflow(all_new_items)

    print("📝 [5/6] 写入本地日志...")
    all_logs = logs_spec + logs_other
    if all_logs:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write("\n".join([l for l in all_logs if l.strip()]) + f"\n--- {get_now_bj()} ---\n\n")

    print("✉️ [6/6] 准备发送飞书通知...")
    send_feishu_v2_card(new_spec, new_other, upd_spec + upd_other, tot_spec, tot_other, all_logs, coze_success=coze_status)
    
    print(f"✨ 监控任务顺利完成！[时刻: {get_now_bj()}]")

if __name__ == "__main__":
    main()
