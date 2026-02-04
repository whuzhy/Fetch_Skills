import requests
import concurrent.futures
import os
import pandas as pd
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

from cozepy import Coze, TokenAuth, COZE_CN_BASE_URL


# ================= 1. 配置与初始化 =================

print("🚀 初始化环境...")

if os.path.exists(".env"):
    load_dotenv()

TOKEN = os.getenv("GITHUB_TOKEN")
FEISHU_WEBHOOK = os.getenv("FEISHU_WEBHOOK")
COZE_API_TOKEN = os.getenv("COZE_API_TOKEN")

workflow_id = "7600384889276547126"

BASE_QUERY = "skills language:Python created:>2025-10-10 is:public stars:>100"
SPECIFIC_LICENSES = ["mit", "apache-2.0", "gpl-3.0", "0bsd", "cc0-1.0"]

DIR_TOTAL, DIR_CHANGES, DIR_LOGS = "Data_Total", "Data_Changes", "Logs"
MAJOR_TOTAL_CSV = os.path.join(DIR_TOTAL, "major_licenses_total.csv")
OTHER_TOTAL_CSV = os.path.join(DIR_TOTAL, "other_licenses_total.csv")
LOG_FILE = os.path.join(DIR_LOGS, "update_log.txt")

for d in [DIR_TOTAL, DIR_CHANGES, DIR_LOGS]:
    os.makedirs(d, exist_ok=True)


# ================= 2. 工具函数 =================

def get_now_bj():
    return datetime.now(timezone.utc).astimezone(
        timezone(timedelta(hours=8))
    ).strftime("%Y-%m-%d %H:%M:%S")


def convert_to_bj_time(utc_str):
    if not utc_str:
        return ""
    try:
        dt = datetime.strptime(
            utc_str, "%Y-%m-%dT%H:%M:%SZ"
        ).replace(tzinfo=timezone.utc)
        return dt.astimezone(
            timezone(timedelta(hours=8))
        ).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return utc_str


def save_daily_change(df, prefix, label, date_suffix):
    path = os.path.join(DIR_CHANGES, f"{prefix}_{label}_{date_suffix}.csv")
    if os.path.exists(path):
        old = pd.read_csv(path)
        df = pd.concat([old, df]).drop_duplicates("Repo_ID", keep="last")
    df.to_csv(path, index=False, encoding="utf-8-sig")
    print(f"    💾 保存文件 {path}")


# ================= 3. GitHub 抓取 =================

def fetch_github_data(query_suffix):
    url = "https://api.github.com/search/repositories"
    headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": "Repo-Monitor-Bot"
    }
    if TOKEN:
        headers["Authorization"] = f"Bearer {TOKEN}"

    try:
        res = requests.get(
            url,
            headers=headers,
            params={
                "q": f"{BASE_QUERY} {query_suffix}",
                "sort": "stars",
                "order": "desc",
                "per_page": 100
            },
            timeout=20
        )
        if res.status_code == 200:
            items = res.json().get("items", [])
            print(f"    - [{query_suffix}] {len(items)} 条")
            return items
    except Exception as e:
        print("    - 查询异常:", e)

    return []


# ================= 4. Coze（逻辑不变，占位） =================

def run_coze_workflow(new_items):
    if not COZE_API_TOKEN or not new_items:
        return True
    try:
        print("🤖 Coze workflow 已触发（占位）")
        return True
    except Exception:
        return False


# ================= 5. 增量处理 =================

def process_incremental(items, file_path, label):
    now = get_now_bj()
    date_suffix = datetime.now().strftime("%m%d")

    new_df = pd.DataFrame([
        {
            "Repo_ID": i["id"],
            "Name": i["full_name"],
            "Stars": i["stargazers_count"],
            "License": i["license"]["key"] if i["license"] else "None",
            "URL": i["html_url"],
            "Created_At": convert_to_bj_time(i["created_at"]),
            "Updated_At": convert_to_bj_time(i["updated_at"]),
            "Last_Grabbed_At": now
        }
        for i in items
    ])

    if not os.path.exists(file_path):
        new_df["First_Grabbed_At"] = now
        new_df.to_csv(file_path, index=False, encoding="utf-8-sig")
        return [], 0, len(new_df), []

    old_df = pd.read_csv(file_path)
    old_df["Repo_ID"] = old_df["Repo_ID"].astype(int)

    # 新增
    new_mask = ~new_df["Repo_ID"].isin(old_df["Repo_ID"])
    new_items_df = new_df[new_mask].copy()
    if not new_items_df.empty:
        new_items_df["First_Grabbed_At"] = now
        save_daily_change(new_items_df, "New", label, date_suffix)

    # 更新
    merged = pd.merge(new_df, old_df, on="Repo_ID", suffixes=("_new", "_old"))
    changed = merged[
        (merged["Stars_new"] != merged["Stars_old"]) |
        (merged["Updated_At_new"] != merged["Updated_At_old"])
    ]
    if not changed.empty:
        changed_df = new_df[new_df["Repo_ID"].isin(changed["Repo_ID"])]
        save_daily_change(changed_df, "Update", label, date_suffix)

    first_map = old_df.set_index("Repo_ID")["First_Grabbed_At"].to_dict()
    new_df["First_Grabbed_At"] = new_df["Repo_ID"].map(first_map).fillna(now)

    total_df = pd.concat([new_df, old_df]).drop_duplicates("Repo_ID", keep="first")
    total_df.to_csv(file_path, index=False, encoding="utf-8-sig")

    logs = [
        f"[{label}] 新增：{r['Name']} (★{r['Stars']})"
        for _, r in new_items_df.iterrows()
    ]

    return (
        new_items_df.to_dict("records"),
        len(changed),
        len(total_df),
        logs
    )


# ================= 6. ⚠️ 原封不动的飞书卡片构建 =================

def build_feishu_v2_card(
    new_major, new_other, update_count,
    total_major, total_other, all_logs, coze_success=True
):
    total_new = len(new_major) + len(new_other)

    major_md = "\n".join(
        [f"• [{i['Name']}]({i['URL']}) <font color='grey'>🐣{i['Created_At'][:10]}</font> **★ {i['Stars']}**"
         for i in new_major[:5]]
    ) or "暂无新增"

    other_md = "\n".join(
        [f"• [{i['Name']}]({i['URL']}) <font color='grey'>🐣{i['Created_At'][:10]}</font> **★ {i['Stars']}**"
         for i in new_other[:5]]
    ) or "暂无新增"

    log_preview = "\n".join([l for l in all_logs if l.strip()][:8])

    card_payload = {
        "msg_type": "interactive",
        "card": {
            "body": {
                "direction": "vertical",
                "elements": [
                    {
                        "tag": "column_set",
                        "flex_mode": "stretch",
                        "horizontal_spacing": "12px",
                        "columns": [
                            {
                                "tag": "column",
                                "width": "weighted",
                                "weight": 1,
                                "background_style": "red-50",
                                "padding": "12px",
                                "elements": [
                                    {"tag": "markdown", "content": "**<font color='red'>主流组</font>**"},
                                    {"tag": "markdown", "content": major_md}
                                ]
                            },
                            {
                                "tag": "column",
                                "width": "weighted",
                                "weight": 1,
                                "background_style": "orange-50",
                                "padding": "12px",
                                "elements": [
                                    {"tag": "markdown", "content": "**<font color='orange'>非主流组</font>**"},
                                    {"tag": "markdown", "content": other_md}
                                ]
                            }
                        ]
                    },
                    {"tag": "markdown", "content": f"🔄 **本次共有 {update_count} 个已知项目更新了数据**"},
                    {"tag": "markdown", "content": f"📝 **更新摘要：**\n{log_preview}"},
                    {"tag": "hr"},
                    {
                        "tag": "markdown",
                        "content": (
                            f"<font color='grey' size='small'>"
                            f"📊 累计监控：主流 {total_major} | 非主流 {total_other}\n"
                            f"📅 监控时刻：{get_now_bj()}"
                            f"</font>"
                        )
                    },
                    {
                        "behaviors": [
                            {
                                "default_url": "https://bytedance.larkoffice.com/base/ObLQbDL5QaWfypsafgecLuhRn8f?from=from_copylink",
                                "type": "open_url"
                            }
                        ],
                        "element_id": "custom_id",
                        "margin": "4px 0px 4px 0px",
                        "tag": "button",
                        "text": {"content": "已同步至多维表格 点击查看", "tag": "plain_text"},
                        "type": "primary_filled",
                        "width": "fill"
                    }
                ]
            },
            "header": {
                "template": "red" if total_new > 0 else "blue",
                "title": {
                    "content": f"GitHub 监控：发现 {total_new} 个新项目！",
                    "tag": "plain_text"
                },
                "icon": {"tag": "standard_icon", "token": "code_outlined"}
            },
            "schema": "2.0"
        }
    }

    return card_payload


# ================= 7. 统一 Webhook 发送 =================

def send_feishu_webhook(payload):
    if not FEISHU_WEBHOOK:
        print("⚠️ 未配置飞书 Webhook")
        return
    res = requests.post(FEISHU_WEBHOOK, json=payload, timeout=10)
    print(f"✅ Webhook 推送完成: {res.status_code}")


# ================= 8. 主程序 =================

def main():
    print(f"📅 启动时间 {get_now_bj()}")

    spec_data, other_data = {}, {}

    tasks = {f"license:{l}": "SPEC" for l in SPECIFIC_LICENSES}
    tasks[" ".join(f"-license:{l}" for l in SPECIFIC_LICENSES)] = "OTHER"

    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as ex:
        fs = {ex.submit(fetch_github_data, q): g for q, g in tasks.items()}
        for f in concurrent.futures.as_completed(fs):
            for item in f.result():
                (spec_data if fs[f] == "SPEC" else other_data)[item["id"]] = item

    new_spec, upd_spec, tot_spec, logs_spec = process_incremental(
        list(spec_data.values()), MAJOR_TOTAL_CSV, "Major"
    )
    new_other, upd_other, tot_other, logs_other = process_incremental(
        list(other_data.values()), OTHER_TOTAL_CSV, "Other"
    )

    coze_status = run_coze_workflow(new_spec + new_other)

    all_logs = logs_spec + logs_other
    if all_logs:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write("\n".join(all_logs) + "\n\n")

    card = build_feishu_v2_card(
        new_spec, new_other,
        upd_spec + upd_other,
        tot_spec, tot_other,
        all_logs,
        coze_success=coze_status
    )

    payload = {
        "event": "github_repo_monitor",
        "timestamp": get_now_bj(),
        "data": {
            "new": {
                "major": new_spec,
                "other": new_other
            },
            "update_count": upd_spec + upd_other
        },
        "meta": {
            "total_major": tot_spec,
            "total_other": tot_other,
            "coze_status": coze_status
        },
        "card": card
    }

    send_feishu_webhook(payload)
    print("✨ 任务结束")


if __name__ == "__main__":
    main()
