#!/usr/bin/env python3
"""
Cost-OS Scoreboard – Railway Web Server
Serves index.html + data.json and runs the Gmail scraper every 15 minutes.

Environment variables (set in Railway):
  GMAIL_TOKEN        – contents of token.json (JSON string)
  GMAIL_CREDENTIALS  – contents of credentials.json (JSON string)
  PORT               – automatically set by Railway
  INTERVAL_MINUTES   – scrape interval (default: 15)
"""

import os
import json
import re
import base64
import threading
import time
import logging
from datetime import datetime, timezone

import requests
from flask import Flask, send_file, jsonify, abort
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────

SCOPES   = ["https://www.googleapis.com/auth/gmail.readonly"]
DIR      = os.path.dirname(os.path.abspath(__file__))
DATA_FILE = os.path.join(DIR, "data.json")
INTERVAL  = int(os.environ.get("INTERVAL_MINUTES", "15")) * 60  # seconds

# ── Flask app ─────────────────────────────────────────────────────────────────

app = Flask(__name__, static_folder=DIR)


@app.route("/")
def index():
    return send_file(os.path.join(DIR, "index.html"))


@app.route("/data.json")
def data():
    if not os.path.exists(DATA_FILE):
        return jsonify({"error": "not ready yet"}), 503
    return send_file(DATA_FILE, mimetype="application/json")


@app.route("/health")
def health():
    return jsonify({"status": "ok"})


# ── Gmail Auth (env-var first, fallback to files) ─────────────────────────────

def auth_gmail():
    token_env = os.environ.get("GMAIL_TOKEN")
    creds_env = os.environ.get("GMAIL_CREDENTIALS")

    token_path = os.path.join(DIR, "token.json")
    creds_path = os.path.join(DIR, "credentials.json")

    # Write env vars to temp files if provided
    if token_env:
        with open(token_path, "w") as f:
            f.write(token_env)
    if creds_env:
        with open(creds_path, "w") as f:
            f.write(creds_env)

    creds = None
    if os.path.exists(token_path):
        creds = Credentials.from_authorized_user_file(token_path, SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
            with open(token_path, "w") as f:
                f.write(creds.to_json())
            # Update env var cache
            os.environ["GMAIL_TOKEN"] = creds.to_json()
        else:
            raise RuntimeError(
                "GMAIL_TOKEN env var missing or expired, and no credentials.json available. "
                "Run `python main.py` locally first to authenticate, then copy token.json "
                "contents into the GMAIL_TOKEN Railway environment variable."
            )

    return build("gmail", "v1", credentials=creds)


# ── Email / Nextiva logic (same as main.py) ───────────────────────────────────

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


def extract_tracking_links(html):
    return re.findall(
        r'href=["\']?(https://ct\.nextiva\.com/ls/click\?[^"\'>\s]+)["\']?', html
    )


def resolve_redirect(url):
    try:
        r = requests.get(url, allow_redirects=True, timeout=15)
        return r.url
    except Exception as e:
        log.warning("Redirect failed: %s", e)
        return ""


NEXTIVA_API  = "https://analytics.nextiva.com/nextos/reports/public/{report_id}"
FALLBACK_ID  = "a2c5d0de-135f-11f1-8409-0050569d50ec"


def is_date_string(s):
    return bool(re.match(r'^\d{4}-\d{2}-\d{2}$', str(s).strip()))


def parse_talk_time_str(s):
    s = str(s).strip(); total = 0
    for val, unit in re.findall(r'(\d+)\s*([hms])', s):
        val = int(val)
        if unit == 'h':   total += val * 3600
        elif unit == 'm': total += val * 60
        elif unit == 's': total += val
    return total


def fetch_nextiva_data(report_id):
    url = NEXTIVA_API.format(report_id=report_id)
    log.info("Calling Nextiva API: %s", url)
    headers = {
        "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/120.0.0.0 Safari/537.36"),
        "Accept": "application/json",
        "Referer": f"https://analytics.nextiva.com/external-reports.html#{report_id}",
    }
    try:
        r = requests.get(url, headers=headers, timeout=20)
        log.info("HTTP %d", r.status_code)
        if r.status_code != 200:
            log.error("Response: %s", r.text[:300])
            return []
        return parse_response(r.json())
    except Exception as e:
        log.error("API call failed: %s", e)
        return []


def parse_response(data):
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # Nextiva wrapped format: results list + filters.users + meta.tableLabels
    if isinstance(data, dict) and isinstance(data.get("results"), list):
        results_list = data["results"]
        user_emails  = data.get("filters", {}).get("users", [])
        reps = []
        for i, series in enumerate(results_list):
            if not isinstance(series, dict):
                continue
            meta = series.get("meta", {}) if isinstance(series.get("meta"), dict) else {}
            table_labels = meta.get("tableLabels", [])
            if isinstance(table_labels, list) and table_labels:
                name = str(table_labels[0]).strip()
            else:
                name = str(series.get("name", series.get("agentName", ""))).strip()
            if not name and i < len(user_emails):
                name = user_emails[i].split("@")[0]
            if not name:
                name = f"Rep {i+1}"

            rows = series.get("data", series.get("rows", []))
            today_row = None
            for row in rows:
                if isinstance(row, dict) and str(row.get("category","")).strip() == today_str:
                    today_row = row; break
            if not today_row:
                for row in reversed(rows):
                    if isinstance(row, dict):
                        try:
                            if int(row.get("Total", row.get("calls", 0))) > 0:
                                today_row = row; break
                        except Exception:
                            pass

            if today_row:
                try: calls = int(today_row.get("Total", today_row.get("calls", 0)))
                except: calls = 0
                t = today_row.get("Total talk time", today_row.get("talkTime",
                    today_row.get("totalTalkTimeSec", 0)))
                if isinstance(t, str):
                    talk_sec = parse_talk_time_str(t)
                else:
                    try: talk_sec = int(t)
                    except: talk_sec = 0
                if calls > 0:
                    reps.append(make_rep(name, calls, talk_sec))

        if reps:
            log.info("Parsed %d rep(s)", len(reps))
            return reps

    log.warning("Could not parse Nextiva response")
    return []


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
    secs = int(secs)
    h, rem = divmod(secs, 3600)
    m, s   = divmod(rem, 60)
    if h: return f"{h}h {m}m {s}s"
    if m: return f"{m}m {s}s"
    return f"{s}s"


# ── Core scrape pass ──────────────────────────────────────────────────────────

def get_report_id_from_gmail(svc):
    results  = svc.users().messages().list(
        userId="me", q="from:analytics@nextiva.com", maxResults=20
    ).execute()
    messages = results.get("messages", [])
    if not messages:
        log.warning("No Nextiva emails found — using fallback ID")
        return FALLBACK_ID

    for msg_info in messages:
        full = svc.users().messages().get(userId="me", id=msg_info["id"], format="full").execute()
        html = get_html_body(full.get("payload", {}))
        for link in extract_tracking_links(html):
            link = link.replace("&amp;", "&")
            try:
                r = requests.get(link, allow_redirects=True, timeout=15)
                m = re.search(
                    r'([a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})',
                    r.url
                )
                if m:
                    log.info("Report ID from email: %s", m.group(1))
                    return m.group(1)
            except Exception as e:
                log.warning("Link failed: %s", e)

    log.warning("All links expired — using fallback ID: %s", FALLBACK_ID)
    return FALLBACK_ID


def run_scrape():
    log.info("Starting scrape pass…")
    try:
        svc = auth_gmail()
    except Exception as e:
        log.error("Gmail auth failed: %s", e)
        return

    report_id = get_report_id_from_gmail(svc)
    reps = fetch_nextiva_data(report_id)

    # Load + update history
    existing = {}
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE) as f:
                existing = json.load(f)
        except Exception:
            pass

    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    history   = [s for s in existing.get("history", []) if s.get("date") != today_str]
    if reps:
        history.append({"date": today_str, "reps": reps})
    history = sorted(history, key=lambda x: x["date"], reverse=True)[:30]

    weekly_snaps = history[:7]
    weekly_reps  = {}
    for snap in weekly_snaps:
        for r in snap["reps"]:
            name = r["name"]
            if name not in weekly_reps:
                weekly_reps[name] = {"name": name, "totalCalls": 0,
                                     "totalTalkTimeSec": 0, "daysActive": 0}
            weekly_reps[name]["totalCalls"]       += r["totalCalls"]
            weekly_reps[name]["totalTalkTimeSec"] += r["totalTalkTimeSec"]
            weekly_reps[name]["daysActive"]       += 1
    for wr in weekly_reps.values():
        wr["avgTalkTimeSec"]  = (round(wr["totalTalkTimeSec"] / wr["totalCalls"])
                                 if wr["totalCalls"] else 0)
        wr["totalTalkTimeStr"] = fmt_dur(wr["totalTalkTimeSec"])

    dates = sorted(s["date"] for s in weekly_snaps) if weekly_snaps else []
    data  = {
        "scrapedAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "reportUrl": f"https://analytics.nextiva.com/external-reports.html#{report_id}",
        "today":  {"reps": reps,  "reportDate": today_str},
        "weekly": {"reps": list(weekly_reps.values()),
                   "daysCount": len(weekly_snaps),
                   "dateRange": f"{dates[0]} to {dates[-1]}" if dates else ""},
        "history": history,
    }
    with open(DATA_FILE, "w") as f:
        json.dump(data, f, indent=2)
    log.info("Saved data.json (%d reps today, %d days history)", len(reps), len(history))


# ── Background scheduler ──────────────────────────────────────────────────────

def scheduler_loop():
    # Initial scrape on startup
    run_scrape()
    while True:
        log.info("Sleeping %d min until next scrape…", INTERVAL // 60)
        time.sleep(INTERVAL)
        run_scrape()


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))

    # Start background scraper thread
    t = threading.Thread(target=scheduler_loop, daemon=True)
    t.start()

    # Start Flask
    log.info("Server starting on port %d", port)
    app.run(host="0.0.0.0", port=port)
