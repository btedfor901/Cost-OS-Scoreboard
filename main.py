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
SCOPES       = ["https://www.googleapis.com/auth/gmail.readonly"]
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

    return build("gmail", "v1", credentials=creds)


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
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # Print raw structure for debugging
    raw = json.dumps(data)
    print(f"  Raw response (first 600 chars):\n  {raw[:600]}")

    # ── Strategy: Nextiva wrapped format with results + filters.users ──────────
    # {"results": [{"data": [{"category":"2026-03-25","Total":53,...}]}, ...],
    #  "filters": {"users": ["email1@nextiva.com", ...]},
    #  "labels": {"users": {"email1": "Galen Urbanski", ...}}}
    if isinstance(data, dict) and isinstance(data.get("results"), list):
        results_list = data["results"]
        user_emails  = data.get("filters", {}).get("users", [])
        labels_map   = data.get("labels", {})

        # labels may be {"users": {"email": "Name"}} or {"email": "Name"}
        user_labels = {}
        if isinstance(labels_map, dict):
            ul = labels_map.get("users", labels_map)
            if isinstance(ul, dict):
                user_labels = ul

        print(f"  results list length: {len(results_list)}")
        print(f"  user_emails: {user_emails}")
        print(f"  user_labels: {user_labels}")
        if results_list:
            print(f"  results[0] keys: {list(results_list[0].keys()) if isinstance(results_list[0], dict) else type(results_list[0])}")

        reps = []
        for i, series in enumerate(results_list):
            if not isinstance(series, dict):
                continue

            # Try to get name: series fields → meta.tableLabels → meta fields → email prefix
            meta = series.get("meta", {}) if isinstance(series.get("meta"), dict) else {}
            table_labels = meta.get("tableLabels", [])
            if isinstance(table_labels, list) and table_labels:
                name = str(table_labels[0]).strip()
            else:
                name = str(series.get("name", series.get("agentName",
                            series.get("label",
                            meta.get("name", meta.get("displayName",
                            meta.get("fullName", meta.get("agentName", "")))))))).strip()
            if not name and i < len(user_emails):
                email = user_emails[i]
                name = email.split("@")[0]
            if not name:
                name = f"Rep {i+1}"

            rows = series.get("data", series.get("rows", series.get("values", [])))
            calls = 0; talk_sec = 0

            # Find today's row, fall back to most recent non-zero
            today_row = None
            for row in rows:
                if isinstance(row, dict) and str(row.get("category","")).strip() == today_str:
                    today_row = row
                    break
            if not today_row:
                for row in reversed(rows):
                    if isinstance(row, dict):
                        c = row.get("Total", row.get("calls", 0))
                        try:
                            if int(c) > 0:
                                today_row = row
                                break
                        except Exception:
                            pass

            if today_row:
                c = today_row.get("Total", today_row.get("calls", 0))
                t = today_row.get("Total talk time", today_row.get("talkTime",
                    today_row.get("totalTalkTimeSec", 0)))
                try: calls = int(c)
                except: calls = 0
                if isinstance(t, str):
                    talk_sec = parse_talk_time_str(t)
                else:
                    try: talk_sec = int(t)
                    except: talk_sec = 0

            if calls > 0:
                reps.append(make_rep(name, calls, talk_sec))

        if reps:
            print(f"  Parsed {len(reps)} rep(s) (Nextiva results+filters format)")
            return reps
        else:
            print(f"  results format matched but 0 reps had calls today (results had {len(results_list)} series)")

    # ── Unwrap top-level wrapper keys ────────────────────────────────────────
    # The Nextiva API wraps data in keys like 'results', 'data', 'report', etc.
    if isinstance(data, dict):
        for wrapper_key in ("results", "data", "rows", "report", "records", "items"):
            inner = data.get(wrapper_key)
            if isinstance(inner, (list, dict)):
                print(f"  Unwrapping top-level key: '{wrapper_key}'")
                raw2 = json.dumps(inner)
                print(f"  Unwrapped '{wrapper_key}' (first 800 chars):\n  {raw2[:800]}")
                result = _try_parse(inner, today_str)
                if result:
                    return result
        # Also try the whole dict as-is (for dict-keyed-by-rep format)
        result = _try_parse(data, today_str)
        if result:
            return result
    elif isinstance(data, list):
        result = _try_parse(data, today_str)
        if result:
            return result

    print("  Could not match any known response format.")
    return []


def _try_parse(data, today_str):  # noqa: C901
    # ── Strategy 0: flat list of per-rep summary dicts ────────────────────────
    # [{"name": "Galen Urbanski", "totalCalls": 53, "talkTime": 3718}, ...]
    NAME_KEYS = ("name", "agentName", "agent", "user", "userName",
                 "displayName", "fullName", "repName", "label")
    CALL_KEYS = ("totalCalls", "calls", "Total", "total_calls",
                 "inboundCalls", "outboundCalls", "callCount")
    TALK_KEYS = ("totalTalkTimeSec", "talkTime", "talk_time", "duration",
                 "talkTimeSec", "Total talk time")

    def first_val(d, keys, default=0):
        for k in keys:
            if k in d:
                return d[k]
        return default

    if isinstance(data, list) and data and isinstance(data[0], dict):
        reps = []
        for rec in data:
            name = str(first_val(rec, NAME_KEYS, "")).strip()
            if not name or is_date_string(name):
                continue
            calls_raw = first_val(rec, CALL_KEYS, 0)
            talk_raw  = first_val(rec, TALK_KEYS, 0)
            try: calls = int(calls_raw)
            except: calls = 0
            if isinstance(talk_raw, str):
                talk_sec = parse_talk_time_str(talk_raw)
            else:
                try: talk_sec = int(talk_raw)
                except: talk_sec = 0
            if calls > 0:
                reps.append(make_rep(name, calls, talk_sec))
        if reps:
            print(f"  ✓ Parsed {len(reps)} rep(s) (flat list format)")
            return reps

    # ── Strategy 1: response is a dict keyed by rep name ─────────────────────
    # {"Galen Urbanski": [...], "Jake Dahlquist": [...], ...}
    if isinstance(data, dict):
        reps = []
        for key, val in data.items():
            if is_date_string(key) or not isinstance(val, list):
                continue
            # Each val is a list of date rows for this rep
            calls = 0
            talk_sec = 0
            for row in val:
                if not isinstance(row, dict):
                    continue
                cat = str(row.get("category", "")).strip()
                # Use today's row, or sum all if today not found
                if cat == today_str or not is_date_string(cat):
                    c = row.get("Total", row.get("calls", row.get("totalCalls", 0)))
                    t = row.get("Total talk time", row.get("talkTime", row.get("totalTalkTimeSec", 0)))
                    try: calls += int(c)
                    except: pass
                    if isinstance(t, str):
                        talk_sec += parse_talk_time_str(t)
                    else:
                        try: talk_sec += int(t)
                        except: pass
            if calls > 0:
                reps.append(make_rep(key, calls, talk_sec))
        if reps:
            print(f"  ✓ Parsed {len(reps)} rep(s) (dict-by-rep format)")
            return reps

    # ── Strategy 2: list where each item is a date row with rep name columns ──
    # [{"category":"2026-03-25","Galen Urbanski":53,"Jake Dahlquist":41,...}, ...]
    if isinstance(data, list):
        # Find today's row
        today_row = None
        for row in data:
            if isinstance(row, dict) and row.get("category") == today_str:
                today_row = row
                break
        # Fall back to most recent non-zero row
        if not today_row:
            for row in reversed(data):
                if isinstance(row, dict) and is_date_string(str(row.get("category",""))):
                    vals = [v for k, v in row.items() if k != "category"
                            and isinstance(v, (int, float)) and v > 0]
                    if vals:
                        today_row = row
                        break

        if today_row:
            reps = []
            for key, val in today_row.items():
                if key in ("category", "Total", "Total talk time"):
                    continue
                if not is_date_string(key) and isinstance(val, (int, float)) and val > 0:
                    reps.append(make_rep(key, int(val), 0))
            if reps:
                print(f"  ✓ Parsed {len(reps)} rep(s) (date-row format, date={today_row.get('category')})")
                return reps

    # ── Strategy 3: nested series list ───────────────────────────────────────
    # {"series": [{"name": "Galen", "data": [{"category":"2026-03-25","Total":53},...]},...]}
    series = None
    if isinstance(data, dict):
        for k in ("series", "agents", "users", "reps", "data"):
            v = data.get(k)
            if isinstance(v, list):
                series = v
                break

    if series:
        reps = []
        for s in series:
            if not isinstance(s, dict):
                continue
            name = str(s.get("name", s.get("agentName", ""))).strip()
            if not name or is_date_string(name):
                continue
            rows = s.get("data", s.get("rows", s.get("values", [])))
            calls = 0; talk_sec = 0
            for row in rows:
                if not isinstance(row, dict):
                    continue
                cat = str(row.get("category", "")).strip()
                if cat == today_str:
                    c = row.get("Total", row.get("calls", 0))
                    t = row.get("Total talk time", row.get("talkTime", 0))
                    try: calls += int(c)
                    except: pass
                    if isinstance(t, str):
                        talk_sec += parse_talk_time_str(t)
                    else:
                        try: talk_sec += int(t)
                        except: pass
            if calls > 0:
                reps.append(make_rep(name, calls, talk_sec))
        if reps:
            print(f"  ✓ Parsed {len(reps)} rep(s) (series format)")
            return reps

    print("  Could not match any known response format.")
    return []


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


# ── Save data ─────────────────────────────────────────────────────────────────

def save(reps, report_id):
    existing = {}
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE) as f:
                existing = json.load(f)
        except Exception:
            pass

    today_str  = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    report_url = f"https://analytics.nextiva.com/external-reports.html#{report_id}"
    history    = [s for s in existing.get("history", []) if s.get("date") != today_str]
    history.append({"date": today_str, "reps": reps})
    history = sorted(history, key=lambda x: x["date"], reverse=True)[:30]

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

    dates = sorted(s["date"] for s in weekly_snaps)
    data  = {
        "scrapedAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "reportUrl": report_url,
        "today":  {"reps": reps, "reportDate": today_str},
        "weekly": {
            "reps":      list(weekly_reps.values()),
            "daysCount": len(weekly_snaps),
            "dateRange": f"{dates[0]} to {dates[-1]}" if dates else "",
        },
        "history": history,
    }

    with open(DATA_FILE, "w") as f:
        json.dump(data, f, indent=2)

    print(f"  ✓ Saved data.json  ({len(reps)} reps, {len(history)} days)")
    for r in sorted(reps, key=lambda x: x["totalCalls"], reverse=True):
        print(f"     {r['name']:<25} {r['totalCalls']:>4} calls   "
              f"{r['totalTalkTimeStr']:>10}   avg {r['avgTalkTimeSec']}s")


# ── Core scrape pass ──────────────────────────────────────────────────────────

def run_scrape(svc):
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Starting scrape…")
    report_id = get_report_id_from_gmail(svc)
    reps      = fetch_nextiva_data(report_id)
    if reps:
        save(reps, report_id)
    else:
        print("  ✗ No data extracted")


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    daemon = "--daemon" in sys.argv
    print("=" * 50)
    print("  Cost-OS Scoreboard Scraper")
    if daemon:
        print(f"  Daemon mode — every {INTERVAL // 60} minutes")
    print("=" * 50)
    print("\nConnecting to Gmail…")
    svc = auth_gmail()
    print("✓ Authenticated")

    if daemon:
        while True:
            try:
                run_scrape(svc)
            except Exception as e:
                print(f"  ERROR: {e}")
            print(f"\nSleeping {INTERVAL // 60} min…")
            time.sleep(INTERVAL)
    else:
        run_scrape(svc)


if __name__ == "__main__":
    main()
