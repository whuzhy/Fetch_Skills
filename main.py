import requests
import concurrent.futures
import os
import pandas as pd
import json
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
from cozepy import Coze, TokenAuth, COZE_CN_BASE_URL

# ================= 1. 配置与环境初始化 =================
if os.path.exists(".env"):
    load_dotenv()

# GitHub & 飞书配置
TOKEN = os.getenv("GITHUB_TOKEN")
FEISHU_WEBHOOK = os.getenv("FEISHU_WEBHOOK")

# Coze 配置
COZE_API_TOKEN = os.getenv("COZE_API_TOKEN")
COZE_WORKFLOW_ID = '7600384889276547126'

# 搜索参数
BASE_QUERY = "skills language:Python created:>2025-10-10 is:public stars:>100"
SPECIFIC_LICENSES = ["mit", "apache-2.0", "gpl-3.0", "0bsd", "cc0-1.0"]

# 目录结构
DIR_TOTAL, DIR_CHANGES, DIR_LOGS = "Data_Total", "Data_Changes", "Logs"
MAJOR_TOTAL_CSV = os.path.join(DIR_TOTAL, "major_licenses_total.csv")
OTHER_TOTAL_CSV = os.path.join(DIR_TOTAL, "other_licenses_total.csv")
LOG_FILE = os.path.join(DIR_LOGS, "update_log.txt")

for folder in [DIR_TOTAL, DIR_CHANGES, DIR_LOGS]:
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
        return res.json().get('items', []) if res.status_code == 200 else []
    except:
        return []


# ================= 3. Coze 工作流集成 =================

def run_coze_workflow(new_items):
    """
    当监测到新增项目时，触发 Coze 工作流
    """
    if not COZE_API_TOKEN or not new_items:
        print("⚠️ 跳过 Coze 触发: 未配置 Token 或无新增项目")
        return

    # 初始化 Coze 客户端
    coze = Coze(auth=TokenAuth(token=COZE_API_TOKEN), base_url=COZE_CN_BASE_URL)

    # 格式化输入数据（你可以根据工作流需求调整格式）
    repo_list_str = "\n".join([f"- {i['Name']}: {i['URL']}" for i in new_items])

    print(f"🤖 正在触发 Coze 工作流分析 {len(new_items)} 个新项目...")
    try:
        workflow = coze.workflows.runs.create(
            workflow_id=COZE_WORKFLOW_ID,
            # 注意：这里的 parameters 的 key 需与 Coze 工作流开始节点的变量名一致
            parameters={
                "repo_info": repo_list_str
            }
        )
        print("✅ Coze 工作流启动成功:", workflow.data)
    except Exception as e:
        print(f"❌ Coze 触发失败: {e}")


# ================= 4. 飞书推送逻辑 (保持原样) =================

def send_feishu_v2_card(new_major, new_other, update_count, total_major, total_other, all_logs):
    if not FEISHU_WEBHOOK: return
    total_new = len(new_major) + len(new_other)
    major_md = "\n".join([
                             f"• [{i['Name']}]({i['URL']}) <font color='grey'>🐣{i['Created_At'][:10]}</font> **<font color='carmine'>★ {i['Stars']}</font>**"
                             for i in new_major[:5]]) or "暂无新增"
    other_md = "\n".join([
                             f"• [{i['Name']}]({i['URL']}) <font color='grey'>🐣{i['Created_At'][:10]}</font> <text_tag color='orange'>{i['License']}</text_tag>"
                             for i in new_other[:5]]) or "暂无新增"
    cleaned_logs = [line.strip() for line in all_logs if line.strip()]
    log_preview = "\n".join(cleaned_logs[:8])

    card_payload = {
        "msg_type": "interactive",
        "card": {
            "body": {
                "direction": "vertical",
                "elements": [
                    {"tag": "column_set", "flex_mode": "stretch", "horizontal_spacing": "12px",
                     "columns": [
                         {"tag": "column", "width": "weighted", "weight": 1, "background_style": "red-50",
                          "padding": "12px",
                          "elements": [
                              {"tag": "markdown", "content": "**<font color='red'>主流组 (MIT/Apache等)</font>**"},
                              {"tag": "markdown", "content": major_md}]},
                         {"tag": "column", "width": "weighted", "weight": 1, "background_style": "orange-50",
                          "padding": "12px",
                          "elements": [
                              {"tag": "markdown", "content": "**<font color='orange'>非主流/无协议组</font>**"},
                              {"tag": "markdown", "content": other_md}]}
                     ]},
                    {"tag": "markdown", "content": f"🔄 **共有 {update_count} 个已知项目更新了内容或指标**"},
                    {"tag": "markdown", "content": f"📝 **日志摘要：**\n{log_preview}"},
                    {"tag": "hr"},
                    {"tag": "markdown",
                     "content": f"<font color='grey' size='small'>📊 累计项目：主流 {total_major} | 非主流 {total_other}\n📅 监控时刻：{get_now_bj()}</font>"}
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
        requests.post(FEISHU_WEBHOOK, json=card_payload, timeout=10)
    except:
        pass


# ================= 5. 核心增量处理逻辑 =================

def save_daily_change(df, prefix, label, date_suffix):
    """按天合并变动数据，同一天内重复项目保留最新一条"""
    file_name = os.path.join(DIR_CHANGES, f"{prefix}_{label}_{date_suffix}.csv")
    if os.path.exists(file_name):
        existing_df = pd.read_csv(file_name)
        combined_df = pd.concat([existing_df, df]).drop_duplicates('Repo_ID', keep='last')
        combined_df.to_csv(file_name, index=False, encoding='utf-8-sig')
    else:
        df.to_csv(file_name, index=False, encoding='utf-8-sig')


def process_incremental(new_list, file_path, label):
    now_bj = get_now_bj()
    date_suffix = datetime.now().strftime('%m%d')

    new_df = pd.DataFrame([{
        'Repo_ID': i['id'],
        'Name': i['full_name'],
        'Stars': i['stargazers_count'],
        'License': i['license']['key'] if i['license'] else "None",
        'URL': i['html_url'],
        'Created_At': convert_to_bj_time(i['created_at']),
        'Updated_At': convert_to_bj_time(i['updated_at']),
        'Last_Grabbed_At': now_bj
    } for i in new_list])

    log_entries = []

    if not os.path.exists(file_path):
        new_df['First_Grabbed_At'] = now_bj
        new_df.to_csv(file_path, index=False, encoding='utf-8-sig')
        return [], 0, len(new_df), [f"[{now_bj}] {label} 首次初始化。"]

    old_df = pd.read_csv(file_path)
    old_df['Repo_ID'] = old_df['Repo_ID'].astype(int)

    # 1. 识别真正的新增项目
    new_mask = ~new_df['Repo_ID'].isin(old_df['Repo_ID'])
    new_items_df = new_df[new_mask].copy()
    if not new_items_df.empty:
        new_items_df['First_Grabbed_At'] = now_bj
        for _, row in new_items_df.iterrows():
            log_entries.append(f"新增：{row['Name']} (★{row['Stars']})")
        save_daily_change(new_items_df, "New", label, date_suffix)

    # 2. 识别指标变更
    merged = pd.merge(new_df, old_df, on='Repo_ID', suffixes=('_new', '_old'))
    changed_mask = (merged['Stars_new'] != merged['Stars_old']) | (merged['Updated_At_new'] != merged['Updated_At_old'])
    changed_items_raw = merged[changed_mask]

    if not changed_items_raw.empty:
        changed_items_df = new_df[new_df['Repo_ID'].isin(changed_items_raw['Repo_ID'])].copy()
        first_map = old_df.set_index('Repo_ID')['First_Grabbed_At'].to_dict()
        changed_items_df['First_Grabbed_At'] = changed_items_df['Repo_ID'].map(first_map)

        for _, row in changed_items_raw.iterrows():
            details = []
            if row['Stars_new'] != row['Stars_old']:
                details.append(f"★ {row['Stars_old']} -> {row['Stars_new']}")
            if row['Updated_At_new'] != row['Updated_At_old']:
                details.append(f"内容更新")
            log_entries.append(f"变更：{row['Name_new']} | " + " | ".join(details))
        save_daily_change(changed_items_df, "Update", label, date_suffix)

    # 3. 更新总表 (保留首次抓取时间)
    first_grabbed_map = old_df.set_index('Repo_ID')['First_Grabbed_At'].to_dict()
    new_df['First_Grabbed_At'] = new_df['Repo_ID'].map(first_grabbed_map).fillna(now_bj)
    updated_total = pd.concat([new_df, old_df]).drop_duplicates('Repo_ID', keep='first')

    cols = ['Repo_ID', 'Name', 'Stars', 'License', 'URL', 'Created_At', 'Updated_At', 'First_Grabbed_At',
            'Last_Grabbed_At']
    updated_total[cols].to_csv(file_path, index=False, encoding='utf-8-sig')

    final_logs = [f"[{label}]"] + log_entries if log_entries else []
    return new_items_df.to_dict('records'), len(changed_items_raw), len(updated_total), final_logs


# ================= 6. 主程序运行入口 =================

def main():
    if not TOKEN:
        print("❌ 错误: 未能在环境中找到 GITHUB_TOKEN")
        return

    spec_data, other_data = {}, {}
    tasks = {f"license:{lic}": "SPEC" for lic in SPECIFIC_LICENSES}
    tasks[" ".join([f"-license:{lic}" for lic in SPECIFIC_LICENSES])] = "OTHER"

    print("🛰️ 正在扫描 GitHub...")
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
        f_to_q = {executor.submit(fetch_github_data, q): group for q, group in tasks.items()}
        for f in concurrent.futures.as_completed(f_to_q):
            group = f_to_q[f]
            for item in f.result():
                (spec_data if group == "SPEC" else other_data)[item['id']] = item

    # 增量处理
    new_spec, upd_spec, tot_spec, logs_spec = process_incremental(list(spec_data.values()), MAJOR_TOTAL_CSV, "Major")
    new_other, upd_other, tot_other, logs_other = process_incremental(list(other_data.values()), OTHER_TOTAL_CSV,
                                                                      "Other")

    # --- 关键逻辑：触发 Coze 工作流 ---
    all_new_items = new_spec + new_other
    if all_new_items:
        run_coze_workflow(all_new_items)

    # 写入本地日志
    all_logs = logs_spec + logs_other
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        clean_write = [line for line in all_logs if line.strip()]
        if clean_write:
            f.write("\n".join(clean_write) + f"\n--- {get_now_bj()} ---\n\n")

    # 推送飞书卡片
    send_feishu_v2_card(new_spec, new_other, upd_spec + upd_other, tot_spec, tot_other, all_logs)
    print("✨ 监控任务执行完毕。")


if __name__ == "__main__":
    main()
