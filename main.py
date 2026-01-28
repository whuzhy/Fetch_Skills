import requests
import concurrent.futures
import os
import pandas as pd
import json
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

# 严格遵循你提供的 SDK 导入方式
from cozepy import Coze, TokenAuth, COZE_CN_BASE_URL

# ================= 1. 配置与环境初始化 =================
print("🚀 [1/6] 正在初始化环境配置...")
if os.path.exists(".env"):
    load_dotenv()

TOKEN = os.getenv("GITHUB_TOKEN")
FEISHU_WEBHOOK = os.getenv("FEISHU_WEBHOOK")
COZE_API_TOKEN = os.getenv("COZE_API_TOKEN")

# 你指定的 Workflow ID
workflow_id = '7600384889276547126'

# 搜索参数
BASE_QUERY = "skills language:Python created:>2025-10-10 is:public stars:>100"
SPECIFIC_LICENSES = ["mit", "apache-2.0", "gpl-3.0", "0bsd", "cc0-1.0"]

# 目录结构
DIR_TOTAL, DIR_CHANGES, DIR_LOGS = "Data_Total", "Data_Changes", "Logs"
MAJOR_TOTAL_CSV = os.path.join(DIR_TOTAL, "major_licenses_total.csv")
OTHER_TOTAL_CSV = os.path.join(DIR_TOTAL, "other_licenses_total.csv")
LOG_FILE = os.path.join(DIR_LOGS, "update_log.txt")

# 确保文件夹存在
for folder in [DIR_TOTAL, DIR_CHANGES, DIR_LOGS]:
    if not os.path.exists(folder):
        os.makedirs(folder, exist_ok=True)

# ================= 2. 核心工具函数 =================

def get_now_bj():
    """获取当前北京时间"""
    return datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=8))).strftime("%Y-%m-%d %H:%M:%S")

def convert_to_bj_time(utc_str):
    """GitHub UTC 时间转北京时间"""
    if not utc_str: return ""
    try:
        utc_dt = datetime.strptime(utc_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        return utc_dt.astimezone(timezone(timedelta(hours=8))).strftime("%Y-%m-%d %H:%M:%S")
    except:
        return utc_str

def save_daily_change(df, prefix, label, date_suffix):
    """保存每日变动到 Data_Changes 文件夹"""
    file_name = os.path.join(DIR_CHANGES, f"{prefix}_{label}_{date_suffix}.csv")
    if os.path.exists(file_name):
        existing_df = pd.read_csv(file_name)
        combined_df = pd.concat([existing_df, df]).drop_duplicates('Repo_ID', keep='last')
        combined_df.to_csv(file_name, index=False, encoding='utf-8-sig')
    else:
        df.to_csv(file_name, index=False, encoding='utf-8-sig')
    print(f"    💾 [文件已生成/更新] {file_name}")

def fetch_github_data(query_suffix):
    """请求 GitHub API 获取数据"""
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
            print(f"    - 查询 [{query_suffix}] 失败，状态码: {res.status_code}")
            return []
    except Exception as e:
        print(f"    - 查询 [{query_suffix}] 异常: {e}")
        return []

# ================= 3. 扣子工作流 (原封不动使用你的逻辑) =================

def run_coze_workflow(new_items):
    if not COZE_API_TOKEN or not new_items:
        print("⚠️ [Coze] 跳过触发: 未配置 Token 或无新增项目")
        return True

    print(f"🤖 [Coze] 正在使用官方 SDK 触发工作流...")
    try:
        # --- 你的原版代码开始 ---
        coze = Coze(auth=TokenAuth(token=COZE_API_TOKEN), base_url=COZE_CN_BASE_URL)
        repo_list_str = "\n".join([f"- {i['Name']}: {i['URL']}" for i in new_items])
        
        workflow = coze.workflows.runs.create(
            workflow_id=workflow_id,
            parameters={
                "repo_info": repo_list_str
            }
        )
        # --- 你的原版代码结束 ---

        print("✅ [Coze] workflow.data:", workflow.data)
        return True
    except Exception as e:
        print(f"❌ [Coze] 触发失败: {e}")
        return False

# ================= 4. 核心增量处理逻辑 (这里之前漏了保存调用) =================

def process_incremental(new_list, file_path, label):
    print(f"🔍 [处理] 正在分析 {label} 分组数据...")
    now_bj = get_now_bj()
    date_suffix = datetime.now().strftime('%m%d')

    new_df = pd.DataFrame([{
        'Repo_ID': i['id'], 'Name': i['full_name'], 'Stars': i['stargazers_count'],
        'License': i['license']['key'] if i['license'] else "None", 'URL': i['html_url'],
        'Created_At': convert_to_bj_time(i['created_at']), 'Updated_At': convert_to_bj_time(i['updated_at']),
        'Last_Grabbed_At': now_bj
    } for i in new_list])

    # 1. 首次初始化处理
    if not os.path.exists(file_path):
        print(f"  - [{label}] 首次运行，正在创建初始总表...")
        new_df['First_Grabbed_At'] = now_bj
        new_df.to_csv(file_path, index=False, encoding='utf-8-sig')
        return [], 0, len(new_df), [f"[{now_bj}] {label} 首次初始化。"]

    old_df = pd.read_csv(file_path)
    old_df['Repo_ID'] = old_df['Repo_ID'].astype(int)

    # 2. 识别新增项目并保存 (Data_Changes/New_...)
    new_mask = ~new_df['Repo_ID'].isin(old_df['Repo_ID'])
    new_items_df = new_df[new_mask].copy()
    if not new_items_df.empty:
        new_items_df['First_Grabbed_At'] = now_bj
        print(f"  - [{label}] 发现 {len(new_items_df)} 个新 Repo，正在保存增量文件...")
        save_daily_change(new_items_df, "New", label, date_suffix) # <--- 这里之前落下了

    # 3. 识别指标变更项目并保存 (Data_Changes/Update_...)
    merged = pd.merge(new_df, old_df, on='Repo_ID', suffixes=('_new', '_old'))
    changed_mask = (merged['Stars_new'] != merged['Stars_old']) | (merged['Updated_At_new'] != merged['Updated_At_old'])
    changed_items_raw = merged[changed_mask]
    if not changed_items_raw.empty:
        changed_items_df = new_df[new_df['Repo_ID'].isin(changed_items_raw['Repo_ID'])].copy()
        print(f"  - [{label}] 发现 {len(changed_items_df)} 个 Repo 指标有变动，正在保存变更文件...")
        save_daily_change(changed_items_df, "Update", label, date_suffix) # <--- 这里之前落下了

    # 4. 更新总表 (Data_Total/...)
    first_grabbed_map = old_df.set_index('Repo_ID')['First_Grabbed_At'].to_dict()
    new_df['First_Grabbed_At'] = new_df['Repo_ID'].map(first_grabbed_map).fillna(now_bj)
    updated_total = pd.concat([new_df, old_df]).drop_duplicates('Repo_ID', keep='first')
    updated_total.to_csv(file_path, index=False, encoding='utf-8-sig')

    log_entries = [f"新增：{r['Name']} (★{r['Stars']})" for _, r in new_items_df.iterrows()]
    return new_items_df.to_dict('records'), len(changed_items_raw), len(updated_total), ([f"[{label}]"] + log_entries if log_entries else [])

# ================= 5. 飞书推送逻辑 =================

def send_feishu_v2_card(new_major, new_other, update_count, total_major, total_other, all_logs, coze_success=True):
    if not FEISHU_WEBHOOK:
        print("⚠️ [飞书] 未配置 Webhook，跳过推送")
        return
    
    print("✉️ [飞书] 正在构建推送卡片...")
    sync_content = "[已同步至飞书多维表格](https://bytedance.larkoffice.com/base/ObLQbDL5QaWfypsafgecLuhRn8f?from=from_copylink)" if coze_success else "❌ **同步失败 (Coze 流程错误)**"

    total_new = len(new_major) + len(new_other)
    major_md = "\n".join([f"• [{i['Name']}]({i['URL']}) <font color='grey'>🐣{i['Created_At'][:10]}</font> **★ {i['Stars']}**" for i in new_major[:5]]) or "暂无新增"
    other_md = "\n".join([f"• [{i['Name']}]({i['URL']}) <font color='grey'>🐣{i['Created_At'][:10]}</font> **★ {i['Stars']}**" for i in new_other[:5]]) or "暂无新增"
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
                          "elements": [{"tag": "markdown", "content": "**<font color='orange'>非主流组</font>**"}, {"tag": "markdown", "content": other_md}]}
                     ]},
                    {"tag": "markdown", "content": f"🔄 **本次共有 {update_count} 个已知项目更新了数据**"},
                    {"tag": "markdown", "content" : "手动@ZHY，记得更新一下多维表格哈～"},
                    {"tag": "markdown", "content": f"📝 **更新摘要：**\n{log_preview}"},
                    {"tag": "hr"},
                    {"tag": "markdown", "content": f"<font color='grey' size='small'>📊 累计监控：主流 {total_major} | 非主流 {total_other}\n📅 监控时刻：{get_now_bj()}</font>"}
                ]
            },
            "header": {
                "template": "red" if total_new > 0 else "blue",
                "title": {"content": f"GitHub 监控：发现 {total_new} 个新项目！", "tag": "plain_text"},
                "icon": {"tag": "standard_icon", "token": "code_outlined"}
            },
            "schema": "2.0"
        }
    }
    try:
        res = requests.post(FEISHU_WEBHOOK, json=card_payload, timeout=10)
        print(f"✅ [飞书] 推送完成，响应状态: {res.status_code}")
    except Exception as e:
        print(f"❌ [飞书] 推送失败: {e}")

# ================= 6. 主程序运行入口 =================

def main():
    print(f"📅 --- 监控任务启动时刻: {get_now_bj()} ---")
    if not TOKEN:
        print("❌ 错误: GITHUB_TOKEN 未配置")
        return

    print("🛰️ [2/6] 正在扫描 GitHub...")
    spec_data, other_data = {}, {}
    tasks = {f"license:{lic}": "SPEC" for lic in SPECIFIC_LICENSES}
    tasks[" ".join([f"-license:{lic}" for lic in SPECIFIC_LICENSES])] = "OTHER"

    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
        f_to_q = {executor.submit(fetch_github_data, q): group for q, group in tasks.items()}
        for f in concurrent.futures.as_completed(f_to_q):
            group = f_to_q[f]
            for item in f.result():
                (spec_data if group == "SPEC" else other_data)[item['id']] = item

    print("📊 [3/6] 开始处理增量与指标分析...")
    new_spec, upd_spec, tot_spec, logs_spec = process_incremental(list(spec_data.values()), MAJOR_TOTAL_CSV, "Major")
    new_other, upd_other, tot_other, logs_other = process_incremental(list(other_data.values()), OTHER_TOTAL_CSV, "Other")

    print("🤖 [4/6] 准备触发 Coze 工作流...")
    all_new_items = new_spec + new_other
    coze_status = run_coze_workflow(all_new_items)

    print("📝 [5/6] 记录本地操作日志...")
    all_logs = logs_spec + logs_other
    if all_logs:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write("\n".join([l for l in all_logs if l.strip()]) + f"\n--- {get_now_bj()} ---\n\n")

    print("✉️ [6/6] 发送飞书日报卡片...")
    send_feishu_v2_card(new_spec, new_other, upd_spec + upd_other, tot_spec, tot_other, all_logs, coze_success=coze_status)
    
    print(f"✨ 监控任务顺利结束！[时刻: {get_now_bj()}]")

if __name__ == "__main__":
    main()
