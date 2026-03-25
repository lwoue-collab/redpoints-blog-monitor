"""
Red Points Blog Monitor: GSC → Asana + Slack
=============================================
Weekly script that:
  1. Pulls Google Search Console data for the past 7 days
  2. Compares it to the previous week's snapshot
  3. Creates an Asana task for each post that dropped
  4. Sends a Slack alert to #blog-monitor with a weekly summary

Setup:
  1. pip install google-auth google-auth-httplib2 google-api-python-client asana python-dotenv requests
  2. Create a .env file with the variables listed in CONFIG below
  3. Add your GSC service account JSON key file
  4. Run manually or deploy as a weekly GitHub Actions cron job
"""

import os
import json
import datetime
import logging
import requests
from pathlib import Path
from dotenv import load_dotenv

from google.oauth2 import service_account
from googleapiclient.discovery import build
import asana
from asana.rest import ApiException

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# CONFIG — set these in a .env file (never commit secrets to git)
# ---------------------------------------------------------------------------

GSC_SERVICE_ACCOUNT_FILE = os.getenv("GSC_SERVICE_ACCOUNT_FILE", "gsc_service_account.json")
GSC_SITE_URL             = os.getenv("GSC_SITE_URL", "https://www.redpoints.com/")

ASANA_TOKEN              = os.getenv("ASANA_TOKEN")
ASANA_PROJECT_GID        = os.getenv("ASANA_PROJECT_GID")
ASANA_WORKSPACE_GID      = os.getenv("ASANA_WORKSPACE_GID")

SLACK_WEBHOOK_URL        = os.getenv("SLACK_WEBHOOK_URL")   # Incoming webhook for #blog-monitor

SNAPSHOT_FILE            = os.getenv("SNAPSHOT_FILE", "gsc_snapshot.json")

POSITION_DROP_THRESHOLD  = float(os.getenv("POSITION_DROP_THRESHOLD", "5"))
TRAFFIC_DROP_THRESHOLD   = float(os.getenv("TRAFFIC_DROP_THRESHOLD",  "0.20"))


# ---------------------------------------------------------------------------
# Cluster → Asana assignee mapping
# Replace the placeholder GIDs with real ones from your Asana workspace
# ---------------------------------------------------------------------------

CLUSTER_ASSIGNEES: dict[str, str] = {
    "Website Takedown":           "ASANA_USER_GID_DANIEL",
    "Social Media Takedown":      "ASANA_USER_GID_DANIEL",
    "Marketplace Protection":     "ASANA_USER_GID_TEAM",
    "Copyright Infringement":     "ASANA_USER_GID_DANIEL",
    "Trademark Protection":       "ASANA_USER_GID_TEAM",
    "Brand Protection":           "ASANA_USER_GID_TEAM",
    # Add the rest of your clusters here...
}

TIER_DUE_DAYS: dict[str, int] = {
    "tier1": 7,
    "tier2": 14,
    "tier3": 30,
}

# ---------------------------------------------------------------------------
# KEEP posts — paste the full list from your audit CSV here
# ---------------------------------------------------------------------------

KEEP_POSTS: dict[str, dict] = {
    "/blog/how-to-take-down-a-fake-website/": {
        "title":   "How to take down a fake website before it destroys your brand",
        "cluster": "Website Takedown",
        "score":   14,
    },
    "/blog/how-to-legally-take-down-a-website/": {
        "title":   "How to legally take down a website: 5 expert-approved steps",
        "cluster": "Website Takedown",
        "score":   13,
    },
    "/blog/how-to-track-a-fake-instagram-account/": {
        "title":   "Smart way to track a fake Instagram account",
        "cluster": "Social Media Takedown",
        "score":   13,
    },
    "/blog/how-to-get-someones-tiktok-video-taken-down/": {
        "title":   "How to get someone else's TikTok video taken down",
        "cluster": "Social Media Takedown",
        "score":   13,
    },
    "/blog/legal-action-against-counterfeit-goods/": {
        "title":   "How to take legal action against counterfeit goods' sellers",
        "cluster": "Counterfeit Goods Protection",
        "score":   13,
    },
    # Add the rest of your KEEP posts here...
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_tier(score: int) -> str:
    if score >= 12:
        return "tier1"
    if score >= 8:
        return "tier2"
    return "tier3"


def due_date_for_tier(tier: str) -> str:
    days = TIER_DUE_DAYS.get(tier, 14)
    return (datetime.date.today() + datetime.timedelta(days=days)).isoformat()


# ---------------------------------------------------------------------------
# 1. Google Search Console
# ---------------------------------------------------------------------------

def build_gsc_service():
    creds = service_account.Credentials.from_service_account_file(
        GSC_SERVICE_ACCOUNT_FILE,
        scopes=["https://www.googleapis.com/auth/webmasters.readonly"],
    )
    return build("searchconsole", "v1", credentials=creds)


def fetch_gsc_data(service, start_date: str, end_date: str) -> dict[str, dict]:
    body = {
        "startDate":  start_date,
        "endDate":    end_date,
        "dimensions": ["page"],
        "rowLimit":   5000,
        "dimensionFilterGroups": [{
            "filters": [{
                "dimension":  "page",
                "operator":   "contains",
                "expression": "/blog/",
            }]
        }],
    }
    response = service.searchanalytics().query(siteUrl=GSC_SITE_URL, body=body).execute()
    rows = response.get("rows", [])

    result = {}
    for row in rows:
        path = row["keys"][0].replace(GSC_SITE_URL.rstrip("/"), "")
        result[path] = {
            "clicks":   row.get("clicks", 0),
            "position": round(row.get("position", 0), 2),
        }
    log.info(f"Fetched {len(result)} blog URLs from GSC ({start_date} → {end_date})")
    return result


def load_snapshot() -> dict:
    if Path(SNAPSHOT_FILE).exists():
        with open(SNAPSHOT_FILE) as f:
            return json.load(f)
    return {}


def save_snapshot(data: dict):
    with open(SNAPSHOT_FILE, "w") as f:
        json.dump(data, f, indent=2)
    log.info(f"Snapshot saved to {SNAPSHOT_FILE}")


# ---------------------------------------------------------------------------
# 2. Flagging logic
# ---------------------------------------------------------------------------

def find_regressions(current: dict, previous: dict) -> list[dict]:
    flags = []

    for path, meta in KEEP_POSTS.items():
        curr = current.get(path)
        prev = previous.get(path)

        if not curr:
            log.warning(f"No GSC data for {path} — skipping")
            continue

        reasons = []

        if prev and curr["position"] - prev["position"] >= POSITION_DROP_THRESHOLD:
            reasons.append(
                f"Position dropped from {prev['position']} → {curr['position']} "
                f"(−{round(curr['position'] - prev['position'], 1)} places)"
            )

        if prev and prev["clicks"] > 10:
            drop_pct = (prev["clicks"] - curr["clicks"]) / prev["clicks"]
            if drop_pct >= TRAFFIC_DROP_THRESHOLD:
                reasons.append(
                    f"Clicks dropped from {prev['clicks']} → {curr['clicks']} "
                    f"(−{round(drop_pct * 100)}%)"
                )

        if reasons:
            flags.append({
                "path":     path,
                "url":      GSC_SITE_URL.rstrip("/") + path,
                "title":    meta["title"],
                "cluster":  meta["cluster"],
                "score":    meta["score"],
                "tier":     get_tier(meta["score"]),
                "reasons":  reasons,
                "current":  curr,
                "previous": prev or {},
            })

    log.info(f"{len(flags)} posts flagged for review")
    return flags


# ---------------------------------------------------------------------------
# 3. Asana task creation
# ---------------------------------------------------------------------------

def build_task_notes(flag: dict, week_end: str) -> str:
    reasons_text = "\n".join(f"  • {r}" for r in flag["reasons"])
    prev = flag["previous"]
    curr = flag["current"]

    return f"""🚨 Blog post needs review — GSC drop detected (week ending {week_end})

URL: {flag["url"]}
Cluster: {flag["cluster"]}
Tier: {flag["tier"].upper()} (score {flag["score"]}/14)

What triggered this alert:
{reasons_text}

Last week:  {prev.get("clicks", "n/a")} clicks | position {prev.get("position", "n/a")}
This week:  {curr["clicks"]} clicks | position {curr["position"]}

--- Freshness checklist ---
[ ] Stats/data points still accurate?
[ ] Platform UI screenshots still current?
[ ] Year in title/meta is correct?
[ ] Internal links all pointing to live posts?
[ ] External links still live and authoritative?
[ ] Yoast score ≥ 70?
[ ] Meta description still matches content?
[ ] Resubmit to GSC after update
"""


def create_asana_tasks(flags: list[dict], week_end: str) -> list[str]:
    """Creates Asana tasks and returns list of task URLs for the Slack summary."""
    if not flags:
        return []

    configuration = asana.Configuration()
    configuration.access_token = ASANA_TOKEN
    client = asana.ApiClient(configuration)
    tasks_api = asana.TasksApi(client)
    task_urls = []

    for flag in flags:
        assignee_gid = CLUSTER_ASSIGNEES.get(flag["cluster"])
        due_on = due_date_for_tier(flag["tier"])

        task_body = {
            "data": {
                "name":     f"[Blog Review] {flag['title']}",
                "notes":    build_task_notes(flag, week_end),
                "projects": [ASANA_PROJECT_GID],
                "due_on":   due_on,
                **({"assignee": assignee_gid} if assignee_gid else {}),
            }
        }

        try:
            result = tasks_api.create_task(task_body, {})
            task_gid = result.gid
            task_url = f"https://app.asana.com/0/{ASANA_PROJECT_GID}/{task_gid}"
            task_urls.append(task_url)
            log.info(f"✅ Asana task created: {flag['title']} (due {due_on})")
        except ApiException as e:
            log.error(f"❌ Failed to create Asana task for {flag['path']}: {e}")

    return task_urls


# ---------------------------------------------------------------------------
# 4. Slack notifications
# ---------------------------------------------------------------------------

def send_slack_summary(flags: list[dict], task_urls: list[str], week_end: str):
    """Sends a weekly summary to #blog-monitor with all flagged posts."""
    if not SLACK_WEBHOOK_URL:
        log.warning("SLACK_WEBHOOK_URL not set — skipping Slack notification")
        return

    if not flags:
        # Send a green all-clear message when nothing needs attention
        payload = {
            "text": f"✅ *Blog Monitor — Week ending {week_end}*\nAll monitored posts are stable this week. No action needed."
        }
        requests.post(SLACK_WEBHOOK_URL, json=payload)
        log.info("Slack all-clear message sent")
        return

    # Build a rich Slack message with one block per flagged post
    header = {
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": (
                f"🚨 *Blog Monitor — Week ending {week_end}*\n"
                f"*{len(flags)} post(s) need attention.* "
                f"Asana tasks have been created automatically."
            )
        }
    }

    divider = {"type": "divider"}

    blocks = [header, divider]

    for i, flag in enumerate(flags):
        tier_emoji = {"tier1": "🔴", "tier2": "🟡", "tier3": "🟢"}.get(flag["tier"], "⚪")
        reasons_text = "\n".join(f"  • {r}" for r in flag["reasons"])
        asana_link = task_urls[i] if i < len(task_urls) else ""

        block = {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"{tier_emoji} *<{flag['url']}|{flag['title']}>*\n"
                    f"Cluster: {flag['cluster']} | Tier: {flag['tier'].upper()} (score {flag['score']}/14)\n"
                    f"{reasons_text}\n"
                    f"Last week: {flag['previous'].get('clicks', 'n/a')} clicks | "
                    f"pos {flag['previous'].get('position', 'n/a')}  →  "
                    f"This week: {flag['current']['clicks']} clicks | "
                    f"pos {flag['current']['position']}"
                    + (f"\n<{asana_link}|→ View Asana task>" if asana_link else "")
                )
            }
        }
        blocks.append(block)
        blocks.append(divider)

    payload = {"blocks": blocks}
    response = requests.post(SLACK_WEBHOOK_URL, json=payload)

    if response.status_code == 200:
        log.info(f"✅ Slack summary sent ({len(flags)} posts flagged)")
    else:
        log.error(f"❌ Slack notification failed: {response.status_code} {response.text}")


# ---------------------------------------------------------------------------
# 5. Main
# ---------------------------------------------------------------------------

def main():
    today      = datetime.date.today()
    week_end   = (today - datetime.timedelta(days=1)).isoformat()
    week_start = (today - datetime.timedelta(days=7)).isoformat()
    prev_start = (today - datetime.timedelta(days=14)).isoformat()
    prev_end   = (today - datetime.timedelta(days=8)).isoformat()

    log.info("=== Red Points Blog Monitor starting ===")
    log.info(f"Current week : {week_start} → {week_end}")
    log.info(f"Previous week: {prev_start} → {prev_end}")

    service = build_gsc_service()

    current_data  = fetch_gsc_data(service, week_start, week_end)
    previous_data = fetch_gsc_data(service, prev_start, prev_end)

    flags     = find_regressions(current_data, previous_data)
    task_urls = create_asana_tasks(flags, week_end)

    send_slack_summary(flags, task_urls, week_end)

    save_snapshot(current_data)
    log.info("=== Done ===")


if __name__ == "__main__":
    main()
