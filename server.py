#!/usr/bin/env python3
"""
Cost-OS Scoreboard – Railway Web Server
Serves index.html + data.json and runs the Gmail scraper every 15 minutes.

Environment variables (set in Railway):
  GMAIL_TOKEN        – contents of token.json (JSON string)
  GMAIL_CREDENTIALS  – contents of credentials.json (JSON string)
  PORT               – automatically set by Railway
  (scraper runs every 5 min 8 AM–5 PM CST Mon–Fri, hourly outside those hours)
"""

import os
import json
import re
import base64
import threading
import time
import logging
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import requests
from flask import Flask, send_file, jsonify, abort, request as flask_request
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────

SCOPES    = [
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive.readonly",
]
DIR       = os.path.dirname(os.path.abspath(__file__))
DATA_FILE = os.path.join(DIR, "data.json")
INTERVAL_ACTIVE = 5 * 60    # 5 min  — 8 AM–5 PM CST Mon–Fri
INTERVAL_OFF    = 60 * 60   # 1 hour — outside business hours
ADMIN_PIN = os.environ.get("ADMIN_PIN", "costos2026")

# ── Display toggle state ──────────────────────────────────────────────────────

_display_on   = True
_display_lock = threading.Lock()
_creds        = None          # saved after auth so Sheets/Drive can reuse it

# ── Scraper health tracking ───────────────────────────────────────────────────

ALERT_AFTER_FAILURES = 3   # email alert after this many consecutive failures
ALERT_EMAIL          = os.environ.get("ALERT_EMAIL", "treyt@cost-os.com")

_health_lock = threading.Lock()
_health = {
    "last_success":          None,
    "last_attempt":          None,
    "last_error":            None,
    "consecutive_failures":  0,
    "total_runs":            0,
    "total_failures":        0,
    "scheduler_alive":       False,
    "start_time":            datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
}

_scheduler_thread = None

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


@app.route("/manage")
def manage():
    return send_file(os.path.join(DIR, "index.html"))


@app.route("/admin")
def admin():
    return send_file(os.path.join(DIR, "admin.html"))


@app.route("/static/<path:filename>")
def static_files(filename):
    return send_file(os.path.join(DIR, "static", filename))


@app.route("/health")
def health():
    with _health_lock:
        h = dict(_health)
    h["scheduler_alive"] = bool(_scheduler_thread and _scheduler_thread.is_alive())
    h["status"] = "degraded" if h["consecutive_failures"] >= ALERT_AFTER_FAILURES else "ok"
    return jsonify(h)


@app.route("/api/display-status")
def display_status():
    return jsonify({"on": _display_on})


@app.route("/api/toggle", methods=["POST"])
def toggle_display():
    global _display_on
    body = flask_request.get_json(silent=True) or {}
    with _display_lock:
        _display_on = bool(body.get("on", not _display_on))
    log.info("Display toggled → %s", _display_on)
    return jsonify({"on": _display_on})


_scrape_running = False

@app.route("/api/scrape-now", methods=["POST"])
def scrape_now():
    global _scrape_running
    if _scrape_running:
        return jsonify({"started": False, "reason": "Scrape already in progress"})

    def _run():
        global _scrape_running
        _scrape_running = True
        try:
            run_scrape()
        finally:
            _scrape_running = False

    threading.Thread(target=_run, daemon=True).start()
    return jsonify({"started": True})


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

    global _creds
    _creds = creds
    return build("gmail", "v1", credentials=creds)


# ── Google Sheets – Sales Demos ───────────────────────────────────────────────

def fetch_demos_sheet():
    """Read 'Sales Demos' sheet and return {rep_name: count} dict."""
    if not _creds:
        log.warning("No creds available for Sheets API")
        return {}
    try:
        drive_svc  = build("drive",  "v3", credentials=_creds)
        sheets_svc = build("sheets", "v4", credentials=_creds)

        res = drive_svc.files().list(
            q="name='Sales Demos' and mimeType='application/vnd.google-apps.spreadsheet' and trashed=false",
            fields="files(id)",
            pageSize=1,
        ).execute()
        files = res.get("files", [])
        if not files:
            log.warning("'Sales Demos' sheet not found in Drive")
            return {}

        sheet_id = files[0]["id"]
        vals = sheets_svc.spreadsheets().values().get(
            spreadsheetId=sheet_id, range="A:B"
        ).execute().get("values", [])

        demos = {}
        for row in vals[1:]:          # skip header row
            if not row or not str(row[0]).strip():
                continue
            name  = str(row[0]).strip()
            count = int(row[1]) if len(row) > 1 and str(row[1]).strip().isdigit() else 0
            demos[name] = count

        log.info("Sales Demos: %s", demos)
        return demos
    except Exception as e:
        log.error("fetch_demos_sheet failed: %s", e)
        return {}


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
            return {}
        return parse_response(r.json())
    except Exception as e:
        log.error("API call failed: %s", e)
        return {}


def parse_response(data):
    """Returns {date_str: [rep_dicts]} for ALL dates with data."""
    if not (isinstance(data, dict) and isinstance(data.get("results"), list)):
        log.warning("Unexpected response format")
        return {}

    results_list = data["results"]
    user_emails  = data.get("filters", {}).get("users", [])
    date_map     = {}  # {date: {name: (calls, talk_sec)}}
    all_rep_names = []  # every rep seen in the API response, in order

    for i, series in enumerate(results_list):
        if not isinstance(series, dict):
            continue
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

        if name not in all_rep_names:
            all_rep_names.append(name)

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

    # Always include every known rep for today, even with 0 calls
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if all_rep_names:
        if today_str not in date_map:
            date_map[today_str] = {}
        for name in all_rep_names:
            if name not in date_map[today_str]:
                date_map[today_str][name] = (0, 0)

    if not date_map:
        log.warning("No rep data found in response")
        return {}

    result = {
        date: [make_rep(n, c, t) for n, (c, t) in reps.items()]
        for date, reps in date_map.items()
    }
    log.info("Parsed %d dates (%d reps today)", len(result), len(result.get(today_str, [])))
    return result


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


def _send_alert_email(subject, body):
    """Send an alert email using the existing Gmail credentials."""
    if not _creds:
        return
    try:
        import base64 as b64
        from email.mime.text import MIMEText
        svc = build("gmail", "v1", credentials=_creds)
        profile = svc.users().getProfile(userId="me").execute()
        to_addr = ALERT_EMAIL or profile["emailAddress"]
        msg = MIMEText(body)
        msg["to"]      = to_addr
        msg["from"]    = profile["emailAddress"]
        msg["subject"] = subject
        raw = b64.urlsafe_b64encode(msg.as_bytes()).decode()
        svc.users().messages().send(userId="me", body={"raw": raw}).execute()
        log.info("Alert email sent to %s", to_addr)
    except Exception as e:
        log.warning("Alert email failed: %s", e)


def run_scrape():
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    with _health_lock:
        _health["last_attempt"] = now_str
        _health["total_runs"]  += 1
    log.info("Starting scrape pass…")
    try:
        svc = auth_gmail()
    except Exception as e:
        log.error("Gmail auth failed: %s", e)
        _record_failure(f"Gmail auth failed: {e}")
        return

    report_id = get_report_id_from_gmail(svc)
    all_dates = fetch_nextiva_data(report_id)
    if not all_dates:
        log.warning("No data extracted")
        _record_failure("Nextiva API returned no data")
        return

    today_str  = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    today_reps = all_dates.get(today_str, [])

    # Merge with existing history
    existing = {}
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE) as f:
                existing = json.load(f)
        except Exception:
            pass

    # Build intraday time-series for today
    existing_intraday = existing.get("intraday", {}) if os.path.exists(DATA_FILE) else {}
    if existing_intraday.get("date") == today_str:
        intraday_points = existing_intraday.get("points", [])
    else:
        intraday_points = []
    current_time = datetime.now(timezone.utc).strftime("%H:%M")
    if not intraday_points or intraday_points[-1]["time"] != current_time:
        intraday_points.append({
            "time": current_time,
            "reps": {r["name"]: r["totalCalls"] for r in today_reps},
        })
    intraday_data = {"date": today_str, "points": intraday_points}

    hist_map = {s["date"]: s for s in existing.get("history", [])}
    for date, reps in all_dates.items():
        hist_map[date] = {"date": date, "reps": reps}
    history = sorted(hist_map.values(), key=lambda x: x["date"], reverse=True)[:90]

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
        wr["avgTalkTimeSec"]   = (round(wr["totalTalkTimeSec"] / wr["totalCalls"])
                                  if wr["totalCalls"] else 0)
        wr["totalTalkTimeStr"] = fmt_dur(wr["totalTalkTimeSec"])

    demos       = fetch_demos_sheet()
    demos_total = sum(demos.values())
    demos_reps  = [{"name": k, "demos": v} for k, v in demos.items()]

    dates = sorted(s["date"] for s in weekly_snaps) if weekly_snaps else []
    data  = {
        "scrapedAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "reportUrl": f"https://analytics.nextiva.com/external-reports.html#{report_id}",
        "today":  {"reps": today_reps, "reportDate": today_str},
        "weekly": {"reps": list(weekly_reps.values()),
                   "daysCount": len(weekly_snaps),
                   "dateRange": f"{dates[0]} to {dates[-1]}" if dates else ""},
        "history": history,
        "demos":   {"total": demos_total, "reps": demos_reps},
        "intraday": intraday_data,
    }
    with open(DATA_FILE, "w") as f:
        json.dump(data, f, indent=2)
    log.info("Saved %d days of history (%d reps today)", len(history), len(today_reps))
    _record_success()


# ── Health helpers ────────────────────────────────────────────────────────────

def _record_success():
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    with _health_lock:
        _health["last_success"]         = now_str
        _health["last_error"]           = None
        _health["consecutive_failures"] = 0
        _health["total_runs"]          += 0  # already incremented


def _record_failure(msg):
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    with _health_lock:
        _health["last_error"]            = f"[{now_str}] {msg}"
        _health["consecutive_failures"] += 1
        _health["total_failures"]       += 1
        failures = _health["consecutive_failures"]
    log.error("Scrape failure #%d: %s", failures, msg)
    if failures == ALERT_AFTER_FAILURES:
        _send_alert_email(
            f"⚠️ Cost-OS Scraper: {failures} consecutive failures",
            f"The Cost-OS scraper has failed {failures} times in a row.\n\n"
            f"Last error: {msg}\n\n"
            f"Check the admin panel at /admin for details.",
        )


# ── Background scheduler ──────────────────────────────────────────────────────

_CST = ZoneInfo("America/Chicago")

def _scrape_interval() -> int:
    """Return the appropriate wait time based on CST time-of-day and day-of-week."""
    now  = datetime.now(_CST)
    hour = now.hour        # 0–23
    dow  = now.weekday()   # 0=Mon … 6=Sun
    if dow < 5 and 8 <= hour < 17:  # Mon–Fri, 8 AM–5 PM CST
        return INTERVAL_ACTIVE
    return INTERVAL_OFF

def scheduler_loop():
    run_scrape()
    while True:
        with _health_lock:
            failures = _health["consecutive_failures"]
        # On failure retry in 5 min regardless of time-of-day
        wait = 5 * 60 if failures > 0 else _scrape_interval()
        log.info("Sleeping %d min until next scrape…", wait // 60)
        time.sleep(wait)
        run_scrape()


def start_scheduler():
    global _scheduler_thread
    _scheduler_thread = threading.Thread(target=scheduler_loop, daemon=True, name="scheduler")
    _scheduler_thread.start()
    log.info("Scheduler thread started (pid-like id: %s)", _scheduler_thread.ident)


def watchdog_loop():
    """Checks every 60 s that the scheduler thread is alive; restarts if not."""
    while True:
        time.sleep(60)
        if not _scheduler_thread or not _scheduler_thread.is_alive():
            log.error("⚠️  Scheduler thread died — restarting now")
            _record_failure("Scheduler thread died unexpectedly")
            start_scheduler()
        else:
            with _health_lock:
                _health["scheduler_alive"] = True


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))

    start_scheduler()
    threading.Thread(target=watchdog_loop, daemon=True, name="watchdog").start()

    log.info("Server starting on port %d", port)
    app.run(host="0.0.0.0", port=port)
