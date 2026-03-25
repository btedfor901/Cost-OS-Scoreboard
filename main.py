#!/usr/bin/env python3
"""
Cost-OS Scoreboard Scraper
1. Reads Gmail for the latest Nextiva report email
2. Extracts the report ID from the link
3. Calls the Nextiva public API directly (no browser needed)
4. Saves data.json

Run once:              python main.py
Daemon (every 15 min): python main.py --daemon
"""

import base64
import json
import os
import re
import sys
import time
from datetime import datetime, timezone

import requests
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

# ── Config ────────────────────────────────────────────────────────────────────
SCOPES       = [
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive.readonly",
]
DATA_FILE    = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data.json")
INTERVAL     = 15 * 60  # seconds between daemon runs
NEXTIVA_API  = "https://analytics.nextiva.com/nextos/reports/public/{report_id}"
FALLBACK_ID  = "a2c5d0de-135f-11f1-8409-0050569d50ec"  # update if needed


# ── Gmail Auth ────────────────────────────────────────────────────────────────

def auth_gmail():
    token_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "token.json")
    creds_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "credentials.json")
    creds = None

    if os.path.exists(token_path):
        creds = Credentials.from_authorized_user_file(token_path, SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            if not os.path.exists(creds_path):
                print("ERROR: credentials.json not found.")
                sys.exit(1)
            flow = InstalledAppFlow.from_client_secrets_file(creds_path, SCOPES)
            creds = flow.run_local_server(port=0)
        with open(token_path, "w") as f:
            f.write(creds.to_json())

    return build("gmail", "v1", credentials=creds), creds


# ── Email helpers ─────────────────────────────────────────────────────────────

def decode_b64(data):
    data += "=" * (-len(data) % 4)
    return base64.urlsafe_b64decode(data).decode("utf-8", errors="ignore")


def get_html_body(payload):
    if payload.get("mimeType") == "text/html":
        return decode_b64(payload.get("body", {}).get("data", ""))
    for part in payload.get("parts", []):
        r = get_html_body(part)
        if r:
            return r
    return ""


def get_report_id_from_gmail(svc):
    """Find the most recent Nextiva report email and extract the report ID."""
    print("  Searching Gmail for Nextiva report emails…")
    results = svc.users().messages().list(
        userId="me", q="from:analytics@nextiva.com", maxResults=20
    ).execute()
    messages = results.get("messages", [])

    if not messages:
        print(f"  No Nextiva emails found — using fallback ID: {FALLBACK_ID}")
        return FALLBACK_ID

    for msg_info in messages:
        full = svc.users().messages().get(
            userId="me", id=msg_info["id"], format="full"
        ).execute()
        html = get_html_body(full.get("payload", {}))

        # Find ct.nextiva.com tracking links
        links = re.findall(
            r'href=["\']?(https://ct\.nextiva\.com/ls/click\?[^"\'>\s]+)["\']?', html
        )

        for link in links:
            link = link.replace("&amp;", "&")
            try:
                r = requests.get(link, allow_redirects=True, timeout=15)
                final_url = r.url
                # Extract report ID from URL hash: #6df8923a-14b6-11f1-8409-0050569d50ec
                match = re.search(
                    r'([a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})',
                    final_url
                )
                if match:
                    report_id = match.group(1)
                    print(f"  ✓ Report ID from email: {report_id}")
                    return report_id
            except Exception as e:
                print(f"  Link failed: {e}")
                continue

    print(f"  All email links expired — using fallback ID: {FALLBACK_ID}")
    return FALLBACK_ID


# ── Nextiva API fetch ─────────────────────────────────────────────────────────

def fetch_nextiva_data(report_id):
    """Call the Nextiva public report API directly and return rep data."""
    url = NEXTIVA_API.format(report_id=report_id)
    print(f"  Calling Nextiva API: {url}")

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json",
        "Referer": f"https://analytics.nextiva.com/external-reports.html#{report_id}",
    }

    try:
        r = requests.get(url, headers=headers, timeout=20)
        print(f"  HTTP {r.status_code}")
        if r.status_code != 200:
            print(f"  Response: {r.text[:300]}")
            return []
        data = r.json()
        print(f"  Raw response keys: {list(data.keys()) if isinstance(data, dict) else type(data)}")
        return parse_response(data)
    except Exception as e:
        print(f"  API call failed: {e}")
        return []


# ── Parse Nextiva API response ────────────────────────────────────────────────

def is_date_string(s):
    """Return True if the string looks like a date (e.g. '2026-03-25')."""
    return bool(re.match(r'^\d{4}-\d{2}-\d{2}$', str(s).strip()))


def find_rep_lists(obj, path="root"):
    """
    Recursively walk the API response and collect all lists whose items
    look like per-rep records (have a name that is NOT a date string).
    """
    results = []
    if isinstance(obj, list) and obj:
        first_item = obj[0]
        if isinstance(first_item, dict):
            # Check if any item has a non-date name field
            name_keys = ("name", "agentName", "agent", "user", "userName",
                         "displayName", "fullName", "repName", "category")
            for key in name_keys:
                if key in first_item:
                    val = str(first_item[key]).strip()
                    if not is_date_string(val):
                        results.append((path, obj))
                    break
            # Also recurse into nested dicts
            for item in obj[:3]:
                for k, v in item.items():
                    results.extend(find_rep_lists(v, f"{path}[].{k}"))
    elif isinstance(obj, dict):
        for k, v in obj.items():
            results.extend(find_rep_lists(v, f"{path}.{k}"))
    return results


def parse_talk_time_str(s):
    """Convert Nextiva talk time string like '1h 58m 38s' to seconds."""
    s = str(s).strip()
    total = 0
    for val, unit in re.findall(r'(\d+)\s*([hms])', s):
        val = int(val)
        if unit == 'h':   total += val * 3600
        elif unit == 'm': total += val * 60
        elif unit == 's': total += val
    return total


def parse_response(data):
    """
    Parse the Nextiva API response and return a dict of {date_str: [rep_dict]}
    covering ALL dates with data — not just today.
    """
    raw = json.dumps(data)
    print(f"  Raw response (first 400 chars):\n  {raw[:400]}")

    if not (isinstance(data, dict) and isinstance(data.get("results"), list)):
        print("  Unexpected response format — 'results' list not found.")
        return {}

    results_list = data["results"]
    user_emails  = data.get("filters", {}).get("users", [])

    # {date: {rep_name: (calls, talk_sec)}}
    date_map = {}

    for i, series in enumerate(results_list):
        if not isinstance(series, dict):
            continue

        # Get rep display name from meta.tableLabels
        meta = series.get("meta", {}) if isinstance(series.get("meta"), dict) else {}
        tl   = meta.get("tableLabels", [])
        if isinstance(tl, list) and tl:
            name = str(tl[0]).strip()
        else:
            name = str(series.get("name", series.get("agentName", ""))).strip()
        if not name and i < len(user_emails):
            name = user_emails[i].split("@")[0]
        if not name:
            name = f"Rep {i+1}"

        # Walk every date row — collect ALL of them
        for row in series.get("data", series.get("rows", [])):
            if not isinstance(row, dict):
                continue
            date = str(row.get("category", "")).strip()
            if not is_date_string(date):
                continue
            try:
                calls = int(row.get("Total", row.get("calls", 0)))
            except Exception:
                calls = 0
            if calls <= 0:
                continue
            t = row.get("Total talk time", row.get("talkTime",
                row.get("totalTalkTimeSec", 0)))
            if isinstance(t, str):
                talk_sec = parse_talk_time_str(t)
            else:
                try:
                    talk_sec = int(t)
                except Exception:
                    talk_sec = 0
            if date not in date_map:
                date_map[date] = {}
            date_map[date][name] = (calls, talk_sec)

    if not date_map:
        print("  No date rows with calls > 0 found.")
        return {}

    # Convert to {date: [rep_dicts]}
    result = {
        date: [make_rep(n, c, t) for n, (c, t) in reps.items()]
        for date, reps in date_map.items()
    }
    today_str  = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    today_reps = result.get(today_str, [])
    print(f"  Parsed {len(result)} date(s) with data  (today={today_str}: {len(today_reps)} rep(s))")
    for r in sorted(today_reps, key=lambda x: x["totalCalls"], reverse=True):
        print(f"     {r['name']:<25} {r['totalCalls']:>4} calls   {r['totalTalkTimeStr']:>10}")
    return result


# ── Helpers ───────────────────────────────────────────────────────────────────

def make_rep(name, calls, talk_sec):
    return {
        "name":             name,
        "date":             datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "totalCalls":       calls,
        "totalTalkTimeSec": talk_sec,
        "totalTalkTimeStr": fmt_dur(talk_sec),
        "avgTalkTimeSec":   round(talk_sec / calls) if calls else 0,
    }


def fmt_dur(secs):
    secs = int(secs or 0)
    h, rem = divmod(secs, 3600)
    m, s   = divmod(rem, 60)
    if h: return f"{h}h {m}m {s}s"
    if m: return f"{m}m {s}s"
    return f"{s}s"


# ── Google Sheets – Sales Demos ───────────────────────────────────────────────

def fetch_demos_sheet(creds):
    """Read 'Sales Demos' sheet and return {rep_name: count} dict."""
    try:
        from googleapiclient.discovery import build as _build
        drive_svc  = _build("drive",  "v3", credentials=creds)
        sheets_svc = _build("sheets", "v4", credentials=creds)

        res = drive_svc.files().list(
            q="name='Sales Demos' and mimeType='application/vnd.google-apps.spreadsheet' and trashed=false",
            fields="files(id)",
            pageSize=1,
        ).execute()
        files = res.get("files", [])
        if not files:
            print("  'Sales Demos' sheet not found in Drive")
            return {}

        sheet_id = files[0]["id"]
        vals = sheets_svc.spreadsheets().values().get(
            spreadsheetId=sheet_id, range="A:B"
        ).execute().get("values", [])

        demos = {}
        for row in vals[1:]:
            if not row or not str(row[0]).strip():
                continue
            name  = str(row[0]).strip()
            count = int(row[1]) if len(row) > 1 and str(row[1]).strip().isdigit() else 0
            demos[name] = count

        print(f"  Sales Demos: {demos}")
        return demos
    except Exception as e:
        print(f"  fetch_demos_sheet failed: {e}")
        return {}


# ── Save data ─────────────────────────────────────────────────────────────────

def save(all_dates, report_id, demos=None):
    """
    all_dates: {date_str: [rep_dicts]} — all dates returned by the API.
    Merges with existing history so no previously-saved days are lost.
    """
    existing = {}
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE) as f:
                existing = json.load(f)
        except Exception:
            pass

    today_str  = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    report_url = f"https://analytics.nextiva.com/external-reports.html#{report_id}"

    # Start from existing history, then overwrite with fresh API data
    hist_map = {s["date"]: s for s in existing.get("history", [])}
    for date, reps in all_dates.items():
        hist_map[date] = {"date": date, "reps": reps}

    history = sorted(hist_map.values(), key=lambda x: x["date"], reverse=True)[:90]

    today_reps   = all_dates.get(today_str, [])
    weekly_snaps = history[:7]
    weekly_reps  = {}
    for snap in weekly_snaps:
        for r in snap["reps"]:
            n = r["name"]
            if n not in weekly_reps:
                weekly_reps[n] = {"name": n, "totalCalls": 0,
                                  "totalTalkTimeSec": 0, "daysActive": 0}
            weekly_reps[n]["totalCalls"]       += r["totalCalls"]
            weekly_reps[n]["totalTalkTimeSec"] += r["totalTalkTimeSec"]
            weekly_reps[n]["daysActive"]       += 1
    for wr in weekly_reps.values():
        wr["avgTalkTimeSec"]   = (round(wr["totalTalkTimeSec"] / wr["totalCalls"])
                                  if wr["totalCalls"] else 0)
        wr["totalTalkTimeStr"] = fmt_dur(wr["totalTalkTimeSec"])

    demos       = demos or {}
    demos_total = sum(demos.values())
    demos_reps  = [{"name": k, "demos": v} for k, v in demos.items()]

    dates = sorted(s["date"] for s in weekly_snaps)
    data  = {
        "scrapedAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "reportUrl": report_url,
        "today":  {"reps": today_reps, "reportDate": today_str},
        "weekly": {
            "reps":      list(weekly_reps.values()),
            "daysCount": len(weekly_snaps),
            "dateRange": f"{dates[0]} to {dates[-1]}" if dates else "",
        },
        "history": history,
        "demos":   {"total": demos_total, "reps": demos_reps},
    }

    with open(DATA_FILE, "w") as f:
        json.dump(data, f, indent=2)

    print(f"  Saved {len(history)} days of history  (today: {len(today_reps)} reps)")


# ── Core scrape pass ──────────────────────────────────────────────────────────

def run_scrape(svc, creds):
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Starting scrape…")
    report_id = get_report_id_from_gmail(svc)
    all_dates = fetch_nextiva_data(report_id)
    demos     = fetch_demos_sheet(creds)
    if all_dates:
        save(all_dates, report_id, demos)
    else:
        print("  No data extracted")


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    daemon = "--daemon" in sys.argv
    print("=" * 50)
    print("  Cost-OS Scoreboard Scraper")
    if daemon:
        print(f"  Daemon mode — every {INTERVAL // 60} minutes")
    print("=" * 50)
    print("\nConnecting to Gmail…")
    svc, creds = auth_gmail()
    print("✓ Authenticated")

    if daemon:
        while True:
            try:
                run_scrape(svc, creds)
            except Exception as e:
                print(f"  ERROR: {e}")
            print(f"\nSleeping {INTERVAL // 60} min…")
            time.sleep(INTERVAL)
    else:
        run_scrape(svc, creds)


if __name__ == "__main__":
    main()
