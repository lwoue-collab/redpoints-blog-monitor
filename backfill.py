"""
Red Points Blog Monitor — One-Time Historical Backfill
=======================================================
Run this script ONCE to populate 12 weeks of historical GSC + Omnia data.
This gives the weekly monitor an instant baseline so traffic flags fire
from the very next Monday run — no 4-week wait needed.

After running this script:
  - 12 JSON files will be saved to data/
  - Push them to GitHub: git add data/ && git commit -m "Add historical baseline" && git push
  - The dashboard will immediately show 12 weeks in the week switcher
  - The weekly monitor will have a 12-week average to compare against

Usage:
  pip install google-auth google-auth-httplib2 google-api-python-client requests python-dotenv
  python backfill.py
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

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# CONFIG — same as monitor.py, reads from .env
# ---------------------------------------------------------------------------

GSC_SERVICE_ACCOUNT_FILE = os.getenv("GSC_SERVICE_ACCOUNT_FILE", "gsc_service_account.json")
GSC_SITE_URL             = os.getenv("GSC_SITE_URL", "https://www.redpoints.com/")
OMNIA_TOKEN              = os.getenv("OMNIA_TOKEN")
OMNIA_BRAND_ID           = os.getenv("OMNIA_BRAND_ID", "03adaaca-5265-404e-b4b1-bbaea0ce73f9")
DATA_DIR                 = Path("data")
DATA_DIR.mkdir(exist_ok=True)

# How many weeks to backfill
BACKFILL_WEEKS = 12

# ---------------------------------------------------------------------------
# KEEP POSTS — same list as monitor.py
# ---------------------------------------------------------------------------

KEEP_POSTS = {
    "/blog/how-to-take-down-a-fake-website/": {
        "title": "How to take down a fake website before it destroys your brand",
        "cluster": "Website Takedown", "score": 14,
        "last_updated": "2025-02-04",
    },
    "/blog/report-infringement-amazon/": {
        "title": "How to report copyright and trademark infringement on Amazon",
        "cluster": "Marketplace Protection", "score": 14,
        "last_updated": "2025-02-06",
    },
    "/blog/unauthorized-sellers-on-walmart/": {
        "title": "How to remove unauthorized sellers on Walmart Marketplace",
        "cluster": "Marketplace Protection", "score": 14,
        "last_updated": "2023-03-27",
    },
    "/blog/cloudflare-dmca-takedown/": {
        "title": "How to effectively submit DMCA takedown request to Cloudflare",
        "cluster": "Copyright Infringement", "score": 13,
        "last_updated": "2025-01-28",
    },
    "/blog/how-to-track-a-fake-instagram-account/": {
        "title": "Smart way to track a fake Instagram account",
        "cluster": "Social Media Takedown", "score": 13,
        "last_updated": "2025-11-03",
    },
    "/blog/how-to-legally-take-down-a-website/": {
        "title": "How to legally take down a website: 5 expert-approved steps",
        "cluster": "Website Takedown", "score": 13,
        "last_updated": "2025-09-19",
    },
    "/blog/legal-action-against-counterfeit-goods/": {
        "title": "How to take legal action against counterfeit goods' sellers",
        "cluster": "Counterfeit Goods Protection", "score": 13,
        "last_updated": "2024-11-21",
    },
    "/blog/how-to-get-someones-tiktok-video-taken-down/": {
        "title": "How to get someone else's TikTok video taken down",
        "cluster": "Social Media Takedown", "score": 13,
        "last_updated": "2025-11-03",
    },
    "/blog/tiktok-dmca-takedown/": {
        "title": "How to successfully remove stolen content from TikTok with a DMCA takedown",
        "cluster": "Copyright Infringement", "score": 12,
        "last_updated": "2025-05-08",
    },
    "/blog/how-to-report-a-scammer-on-telegram/": {
        "title": "How to report a scammer on Telegram",
        "cluster": "Platform Scams", "score": 12,
        "last_updated": "2025-06-01",
    },
    "/blog/how-to-report-copyright-infringement-on-tiktok/": {
        "title": "How to report a copyright infringement on TikTok",
        "cluster": "Social Media Takedown", "score": 12,
        "last_updated": "2025-04-15",
    },
    "/blog/how-to-file-a-dmca-takedown-notice/": {
        "title": "How to file a DMCA takedown notice to Google to stop copyright infringement",
        "cluster": "Copyright Infringement", "score": 12,
        "last_updated": "2025-03-10",
    },
    "/blog/website-cloning/": {
        "title": "Website cloning: How to identify, prevent, and respond",
        "cluster": "Website Takedown", "score": 12,
        "last_updated": "2025-01-15",
    },
    "/blog/ban-fake-instagram-accounts/": {
        "title": "A step-by-step guide to banning fake Instagram accounts permanently",
        "cluster": "Social Media Takedown", "score": 12,
        "last_updated": "2025-08-20",
    },
    "/blog/alibaba-scams/": {
        "title": "9 tips to avoid Alibaba scams",
        "cluster": "Marketplace Protection", "score": 12,
        "last_updated": "2025-02-01",
    },
    "/blog/how-to-take-down-a-tiktok-account/": {
        "title": "How to take down a TikTok account",
        "cluster": "Social Media Takedown", "score": 12,
        "last_updated": "2025-05-20",
    },
    "/blog/how-to-report-and-take-down-a-fraud-website/": {
        "title": "How to report and take down a fraud website: a step-by-step guide",
        "cluster": "Website Takedown", "score": 12,
        "last_updated": "2025-03-01",
    },
    "/blog/best-brand-protection-software/": {
        "title": "7 Best Brand Protection Tools for 2026: Ranked & reviewed",
        "cluster": "Brand Protection", "score": 10,
        "last_updated": "2026-01-15",
    },
}


# ---------------------------------------------------------------------------
# DATE HELPERS
# ---------------------------------------------------------------------------

def week_bounds(weeks_ago: int) -> tuple[datetime.date, datetime.date]:
    """Returns (monday, sunday) for N weeks ago, respecting GSC 3-day lag."""
    today = datetime.date.today()
    days_since_sunday = (today.weekday() + 1) % 7
    last_sunday = today - datetime.timedelta(days=days_since_sunday)
    if (today - last_sunday).days < 3:
        last_sunday -= datetime.timedelta(weeks=1)
    last_monday = last_sunday - datetime.timedelta(days=6)
    monday = last_monday - datetime.timedelta(weeks=weeks_ago - 1)
    sunday = monday + datetime.timedelta(days=6)
    return monday, sunday


def week_key(monday: datetime.date) -> str:
    return monday.strftime("%Y-%m-%d")


# ---------------------------------------------------------------------------
# GSC
# ---------------------------------------------------------------------------

def build_gsc_service():
    creds = service_account.Credentials.from_service_account_file(
        GSC_SERVICE_ACCOUNT_FILE,
        scopes=["https://www.googleapis.com/auth/webmasters.readonly"],
    )
    return build("searchconsole", "v1", credentials=creds)


def fetch_gsc_clicks(service, start_date: str, end_date: str) -> dict:
    body = {
        "startDate": start_date,
        "endDate": end_date,
        "dimensions": ["page"],
        "rowLimit": 5000,
        "dimensionFilterGroups": [{"filters": [{
            "dimension": "page",
            "operator": "contains",
            "expression": "/blog/",
        }]}],
    }
    try:
        response = service.searchanalytics().query(
            siteUrl=GSC_SITE_URL, body=body
        ).execute()
        rows = response.get("rows", [])
        result = {}
        for row in rows:
            path = row["keys"][0].replace(GSC_SITE_URL.rstrip("/"), "")
            result[path] = row.get("clicks", 0)
        log.info(f"  GSC: {len(result)} URLs ({start_date} → {end_date})")
        return result
    except Exception as e:
        log.error(f"  GSC failed: {e}")
        return {}


# ---------------------------------------------------------------------------
# OMNIA
# ---------------------------------------------------------------------------

def fetch_omnia_citations(start_date: str, end_date: str) -> dict:
    if not OMNIA_TOKEN:
        log.warning("  OMNIA_TOKEN not set — skipping")
        return {}

    url = f"https://app.useomnia.com/api/v1/brands/{OMNIA_BRAND_ID}/citations/aggregates"
    headers = {"Authorization": f"Bearer {OMNIA_TOKEN}"}
    params = {
        "startDate": start_date,
        "endDate": end_date,
        "sourceType": "owned",
        "pageSize": 100,
        "sortBy": "total_citations",
        "sortDirection": "desc",
    }
    result = {}
    page = 1
    while True:
        params["page"] = page
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            for agg in data.get("data", {}).get("aggregates", []):
                post_url = agg.get("url", "")
                if "/blog/" in post_url:
                    path = "/" + post_url.split("redpoints.com/")[1] if "redpoints.com/" in post_url else post_url
                    result[path] = agg.get("totalCitations", 0)
            total = data.get("pagination", {}).get("totalItems", 0)
            if page * 100 >= total:
                break
            page += 1
        except Exception as e:
            log.error(f"  Omnia failed (page {page}): {e}")
            break

    log.info(f"  Omnia: {len(result)} blog URLs ({start_date} → {end_date})")
    return result


# ---------------------------------------------------------------------------
# STALE FLAG — same logic as monitor.py
# ---------------------------------------------------------------------------

def check_stale_flags(today: datetime.date) -> list[dict]:
    flagged = []
    for path, meta in KEEP_POSTS.items():
        last_updated_str = meta.get("last_updated")
        if last_updated_str and meta.get("score", 0) >= 10:
            last_updated = datetime.date.fromisoformat(last_updated_str)
            months_since = (today - last_updated).days / 30
            if months_since >= 6:
                flagged.append({
                    "path": path,
                    "url": GSC_SITE_URL.rstrip("/") + path,
                    "title": meta["title"],
                    "cluster": meta["cluster"],
                    "score": meta["score"],
                    "tier": "tier1" if meta["score"] >= 12 else "tier2",
                    "flags": [{
                        "type": "stale",
                        "label": "Stale content",
                        "detail": f"Not updated in {months_since:.0f} months (last: {last_updated_str}, score {meta['score']}/14)",
                        "months_since_update": round(months_since, 1),
                        "last_updated": last_updated_str,
                    }],
                    "gsc_clicks_this_week": 0,
                    "llm_citations_this_week": 0,
                })
    return sorted(flagged, key=lambda x: -x["score"])


# ---------------------------------------------------------------------------
# MAIN BACKFILL
# ---------------------------------------------------------------------------

def main():
    log.info("=== Red Points Blog Monitor — Historical Backfill ===")
    log.info(f"Backfilling {BACKFILL_WEEKS} weeks of data...")

    gsc_service = build_gsc_service()
    today = datetime.date.today()

    for weeks_ago in range(BACKFILL_WEEKS, 0, -1):
        monday, sunday = week_bounds(weeks_ago)
        wk = week_key(monday)
        week_end = sunday.strftime("%Y-%m-%d")
        output_path = DATA_DIR / f"week-{wk}.json"

        if output_path.exists():
            log.info(f"Week {wk} already exists — skipping")
            continue

        log.info(f"\nProcessing week {wk} → {week_end}")

        gsc_data   = fetch_gsc_clicks(gsc_service, wk, week_end)
        omnia_data = fetch_omnia_citations(wk, week_end)

        # For historical weeks use stale flag only
        # (traffic/LLM flags need week-over-week comparison which monitor.py handles)
        stale_flags = check_stale_flags(today)

        week_data = {
            "week_start": wk,
            "week_end": week_end,
            "season": None,
            "flagged": stale_flags,
            "summary": f"Historical week {wk}. Stale content flags shown. Traffic and LLM comparison flags will apply from next Monday's live run.",
            "gsc_data": gsc_data,
            "omnia_current": omnia_data,
            "generated_at": datetime.datetime.utcnow().isoformat(),
            "is_backfill": True,
        }

        with open(output_path, "w") as f:
            json.dump(week_data, f, indent=2)

        log.info(f"  Saved {output_path}")

    log.info(f"\n=== Backfill complete — {BACKFILL_WEEKS} weeks saved to data/ ===")
    log.info("\nNext steps:")
    log.info("  1. git add data/")
    log.info("  2. git commit -m 'Add 12-week historical baseline'")
    log.info("  3. git push")
    log.info("  4. Dashboard will immediately show all 12 weeks")
    log.info("  5. Traffic flags will fire from next Monday's run")


if __name__ == "__main__":
    main()
