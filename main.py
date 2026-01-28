import requests
import concurrent.futures
import os
import pandas as pd
import json
import base64
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

# ================= 1. 环境与路径配置 =================
if os.path.exists(".env"):
    load_dotenv()

TOKEN = os.getenv("GITHUB_TOKEN")
FEISHU_WEBHOOK = os.getenv("FEISHU_WEBHOOK")
# 静态网页访问密码（请根据需要修改）
WEB_PASSWORD = "你的访问密码"

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


# ================= 2. 辅助工具函数 =================

def get_now_bj_str():
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
    if TOKEN: headers["Authorization"] = f"Bearer {TOKEN}"
    try:
        res = requests.get(url, params=params, headers=headers, timeout=20)
        return res.json().get('items', []) if res.status_code == 200 else []
    except:
        return []


# ================= 3. 核心增量逻辑 =================

def process_incremental(new_list, file_path, label):
    now_bj = get_now_bj_str()
    file_ts = datetime.now().strftime('%m%d_%H%M')

    new_df = pd.DataFrame([{
        'Repo_ID': i['id'], 'Name': i['full_name'], 'Stars': i['stargazers_count'],
        'License': i['license']['key'] if i['license'] else "None",
        'URL': i['html_url'], 'Description': i['description'],
        'Created_At': convert_to_bj_time(i['created_at']),
        'Updated_At': convert_to_bj_time(i['updated_at']),
        'Grabbed_At': now_bj
    } for i in new_list])

    log_entries = []

    if not os.path.exists(file_path):
        new_df.to_csv(file_path, index=False, encoding='utf-8-sig')
        return [], 0, len(new_df), [f"[{label}] 库初始化入库 {len(new_df)} 条"]

    old_df = pd.read_csv(file_path)
    old_df['Repo_ID'] = old_df['Repo_ID'].astype(int)

    # 识别新增
    new_mask = ~new_df['Repo_ID'].isin(old_df['Repo_ID'])
    new_items_df = new_df[new_mask]
    for _, row in new_items_df.iterrows():
        log_entries.append(f"新增：{row['Name']} (★{row['Stars']})")

    # 识别更新
    merged = pd.merge(new_df, old_df, on='Repo_ID', suffixes=('_new', '_old'))
    changed_mask = (merged['Stars_new'] != merged['Stars_old']) | (merged['Updated_At_new'] != merged['Updated_At_old'])
    changed_items = merged[changed_mask]

    for _, row in changed_items.iterrows():
        details = []
        if row['Stars_new'] != row['Stars_old']:
            details.append(f"★{row['Stars_old']}->{row['Stars_new']}")
        if row['Updated_At_new'] != row['Updated_At_old']:
            details.append(f"内容更新")
        log_entries.append(f"变更：{row['Name_new']} | " + " | ".join(details))

    # 更新总表
    updated_total = pd.concat([new_df, old_df]).drop_duplicates('Repo_ID', keep='first')
    updated_total.to_csv(file_path, index=False, encoding='utf-8-sig')

    # 保存变更表
    if not new_items_df.empty:
        new_items_df.to_csv(os.path.join(DIR_CHANGES, f"New_{label}_{file_ts}.csv"), index=False, encoding='utf-8-sig')
    if not changed_items.empty:
        changed_items.to_csv(os.path.join(DIR_CHANGES, f"Update_{label}_{file_ts}.csv"), index=False,
                             encoding='utf-8-sig')

    final_logs = [f"[{label}]"] + log_entries if log_entries else []
    return new_items_df.to_dict('records'), len(changed_items), len(updated_total), final_logs


# ================= 4. 飞书卡片与静态网页 =================

def send_feishu_v2_card(new_major, new_other, update_count, total_major, total_other, all_logs):
    if not FEISHU_WEBHOOK: return
    total_new = len(new_major) + len(new_other)
    major_md = "\n".join([
                             f"• [{i['Name']}]({i['URL']}) <font color='grey'>🐣{i['Created_At'][:10]}</font> **<font color='carmine'>★ {i['Stars']}</font>**"
                             for i in new_major[:5]]) or "暂无新增"
    other_md = "\n".join([
                             f"• [{i['Name']}]({i['URL']}) <font color='grey'>🐣{i['Created_At'][:10]}</font> <text_tag color='orange'>{i['License']}</text_tag>"
                             for i in new_other[:5]]) or "暂无新增"
    cleaned_logs = [l.strip() for l in all_logs if l.strip()]
    log_preview = "\n".join(cleaned_logs[:10])

    card_payload = {
        "msg_type": "interactive",
        "card": {
            "body": {
                "elements": [
                    {"tag": "column_set", "flex_mode": "stretch", "columns": [
                        {"tag": "column", "width": "weighted", "weight": 1, "background_style": "red-50",
                         "padding": "12px",
                         "elements": [{"tag": "markdown", "content": "**<font color='red'>主流组新增</font>**"},
                                      {"tag": "markdown", "content": major_md}]},
                        {"tag": "column", "width": "weighted", "weight": 1, "background_style": "orange-50",
                         "padding": "12px",
                         "elements": [{"tag": "markdown", "content": "**<font color='orange'>非主流组新增</font>**"},
                                      {"tag": "markdown", "content": other_md}]}
                    ]},
                    {"tag": "markdown",
                     "content": f"🔄 **共有 {update_count} 个项目检测到活跃更新**\n📝 **日志摘要：**\n{log_preview}"},
                    {"tag": "hr"},
                    {"tag": "markdown",
                     "content": f"<font color='grey' size='small'>📊 存量：主流 {total_major} | 其他 {total_other}\n📅 监控时间：{get_now_bj_str()}</font>"}
                ]
            },
            "header": {
                "template": "red" if total_new > 0 else "blue",
                "title": {"content": f"发现 {total_new} 个新项目！GitHub 监控日报", "tag": "plain_text"},
                "icon": {"tag": "standard_icon", "token": "code_outlined"}
            }
        }
    }
    requests.post(FEISHU_WEBHOOK, json=card_payload, timeout=10)


# ================= 5. 主程序 =================

def main():
    spec_data, other_data = {}, {}
    tasks = {f"license:{l}": "SPEC" for l in SPECIFIC_LICENSES}
    tasks[" ".join([f"-license:{l}" for l in SPECIFIC_LICENSES])] = "OTHER"

    print("🛰️ 正在抓取数据...")
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
        f_to_q = {executor.submit(fetch_github_data, q): group for q, group in tasks.items()}
        for f in concurrent.futures.as_completed(f_to_q):
            for item in f.result(): (spec_data if f_to_q[f] == "SPEC" else other_data)[item['id']] = item

    new_spec, upd_spec, tot_spec, logs_spec = process_incremental(list(spec_data.values()), MAJOR_TOTAL_CSV, "主流")
    new_other, upd_other, tot_other, logs_other = process_incremental(list(other_data.values()), OTHER_TOTAL_CSV,
                                                                      "其他")

    all_logs = logs_spec + logs_other
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write("\n".join([l for l in all_logs if l.strip()]) + f"\n--- {get_now_bj_str()} ---\n")

    send_feishu_v2_card(new_spec, new_other, upd_spec + upd_other, tot_spec, tot_other, all_logs)
    print("✨ 执行成功！")


if __name__ == "__main__":
    main()