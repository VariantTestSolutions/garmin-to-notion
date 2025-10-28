"""
Garmin → Google Sheets Daily Rollup — v4.1.0
Change: Updated columns to match user request (Oct 28, 2025).
Using garth for most wellness metrics for reliability.
"""

from datetime import date, datetime, timedelta, timezone
from collections import defaultdict, Counter
import os, sys, time, calendar, re, json, base64, tarfile, io
from dotenv import load_dotenv
from garminconnect import Garmin # Still needed for Activities and basic Sleep DTO
import garth # Used for reliable wellness stats
from zoneinfo import ZoneInfo

# Google Sheets
import gspread
from google.oauth2 import service_account

# -----------------------------
# Timezone config
# -----------------------------
def get_local_tz():
    tzname = os.getenv("LOCAL_TZ") or os.getenv("TIMEZONE") or os.getenv("TZ")
    if tzname:
        try:
            return ZoneInfo(tzname)
        except Exception:
            pass
    try:
        return datetime.now().astimezone().tzinfo  # fallback to system tz
    except Exception:
        return timezone.utc

# -----------------------------
# Helpers (Unchanged from original)
# -----------------------------
def iso_date(d: date) -> str:
    return d.isoformat()

def today_local() -> date:
    tz = get_local_tz()
    return datetime.now(tz).date()

def daterange(start: date, end_exclusive: date):
    d = start
    while d < end_exclusive:
        yield d
        d += timedelta(days=1)

def ms_to_local_iso(ms: int | None) -> str | None:
    if not ms:
        return None
    try:
        dt_utc = datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
        tz = get_local_tz()
        dt_local = dt_utc.astimezone(tz)
        dt_local = dt_local.replace(second=0, microsecond=0)
        return dt_local.isoformat()
    except Exception:
        return None

def to_tokens(value):
    if value is None:
        return []
    if isinstance(value, list):
        return [str(v).strip() for v in value if str(v).strip()]
    s = str(value).strip()
    if not s:
        return []
    if any(sep in s for sep in [",", ";", "|"]):
        parts = re.split(r"[,\;\|]+", s)
    else:
        parts = s.split()
    return [p.strip() for p in parts if p.strip()]

def _first_present(dct, keys, default=None):
    for k in keys:
        if k in dct and dct.get(k) not in (None, ""):
            return dct.get(k)
    return default

# CHANGED: Added helper to convert seconds to hours
def seconds_to_hours(seconds):
    """Safely converts seconds (int) to hours (float, 2 decimal places)."""
    if isinstance(seconds, (int, float)):
        return round(seconds / 3600.0, 2)
    return ""
# --- END CHANGE ---

# -----------------------------
# Garmin login (CI-safe) (Unchanged from original)
# -----------------------------
def _ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def _maybe_restore_token_dir_from_tgz(token_dir: str):
    b64 = os.getenv("GARMIN_TOKEN_STORE_TGZ_B64")
    if not b64:
        return
    needs_restore = (not os.path.exists(token_dir)) or (os.path.isdir(token_dir) and not os.listdir(token_dir))
    if not needs_restore:
        return
    try:
        _ensure_dir(token_dir)
        data = base64.b64decode(b64)
        with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as tar:
            tar.extractall(token_dir)
        print(f"[garmin] Restored token-store from GARMIN_TOKEN_STORE_TGZ_B64 into {token_dir}")
    except Exception as e:
        print(f"[garmin] Failed to restore token-store from GARMIN_TOKEN_STORE_TGZ_B64: {e}")

def login_to_garmin():
    garmin_email = os.getenv("GARMIN_EMAIL")
    garmin_password = os.getenv("GARMIN_PASSWORD")
    token_store = os.getenv("GARMIN_TOKEN_STORE", "~/.garmin_tokens")
    token_store = os.path.expanduser(token_store).rstrip("/")
    mfa_code = os.getenv("GARMIN_MFA_CODE")

    if not garmin_email or not garmin_password:
        print("Missing GARMIN_EMAIL or GARMIN_PASSWORD")
        sys.exit(1)

    if os.path.exists(token_store) and not os.path.isdir(token_store):
        print(f"[garmin] GARMIN_TOKEN_STORE points to a file: {token_store}. Expected a directory.")
        sys.exit(1)

    _maybe_restore_token_dir_from_tgz(token_store)

    # Initialize garth first
    try:
        garth.resume(token_store)
        print(f"[garmin] Resumed tokens from {token_store} for garth")
    except Exception as resume_err:
        print(f"[garmin] No usable tokens for garth, will login: {resume_err}")
        if mfa_code:
            print("[garmin] Performing non-interactive MFA login for garth")
            client_state = garth.login(garmin_email, garmin_password, return_on_mfa=True)
            if client_state: garth.resume_login(client_state, mfa_code)
        else:
            garth.login(garmin_email, garmin_password)
        _ensure_dir(token_store)
        garth.save(token_store)
        print(f"[garmin] Saved new garth tokens to {token_store}")

    # Now initialize Garmin object
    g = Garmin(garmin_email, garmin_password)
    try:
        g.login(tokenstore=token_store)
        print(f"[garmin] Garmin object login successful using tokens from {token_store}")
        return g, token_store
    except Exception as e:
        print(f"[garmin] Garmin object login error: {e}")
        try: # Fallback login attempt for the Garmin object
            g.login()
            print(f"[garmin] Garmin object login successful on fallback.")
            return g, token_store
        except Exception as e2:
            print(f"[garmin] Full login error: {e2}")
            sys.exit(1)

# -----------------------------
# Column map
# -----------------------------
# CHANGED: Added keys for new columns
P = {
    "Date": "Date",
    "weekday": "weekday",
    "WeightLb": "Weight (lb)",
    "TrainingReadiness": "Training Readiness (0-100)",
    "TrainingStatus": "Training Status",
    "RestingHR": "Resting HR",
    "HRV": "HRV",
    "RespirationRateAvg": "Respiration Rate Avg (BPM)",
    "SleepScoreOverall": "Sleep Score (0-100)", # New field for overall score
    "SleepTotalH": "Sleep Total (h)",
    "SleepLightH": "Sleep Light (h)",
    "SleepDeepH": "Sleep Deep (h)",
    "SleepRemH": "Sleep REM (h)",
    "SleepAwakeH": "Sleep Awake (h)",
    "SleepStart": "Sleep Start (local)",
    "SleepEnd": "Sleep End (local)",
    "SS_overall": "Sleep Overall (q)",
    "SS_total_duration": "Sleep Duration (q)",
    "SS_stress": "Sleep Stress (q)",
    "SS_awake_count": "Sleep Awake Count (q)",
    "SS_rem_percentage": "Sleep REM % (q)",
    "SS_restlessness": "Sleep Restlessness (q)",
    "SS_light_percentage": "Sleep Light % (q)",
    "SS_deep_percentage": "Sleep Deep % (q)",
    "StressAvg": "Stress Avg",
    "StressMax": "Stress Max",
    "StressRestH": "Rest Stress Duration(h)", # New
    "StressLowH": "Low Stress Duration (h)", # New
    "StressMediumH": "Medium Stress Duration (h)", # New
    "StressHighH": "High Stress Duration (h)", # New
    "StressUncatH": "Uncategorized Stress Duration (h)", # New
    "BodyBatteryAvg": "Body Battery Avg",
    "BodyBatteryMax": "Body Battery Max", # New
    "BodyBatteryMin": "Body Battery Min",
    "Steps": "Steps",
    "StepGoal": "Step Goal",
    "WalkDistanceMi": "Walk Distance (mi)",
    "ActivityCount": "Activities (#)",
    "ActivityDistanceMi": "Activity Distance (mi)",
    "ActivityDurationMin": "Activity Duration (min)",
    "ActivityCalories": "Activity Calories",
    "ActivityNames": "Activity Names",
    "ActivityTypes": "Activity Types",
    "PrimarySport": "primary_sport",
    "ActivityTypesUnique": "activity_types_unique",
    "ActTrainingEff": "Training Effect (list)",
    "ActAerobicEff": "Aerobic Effect (list)",
    "ActAnaerobicEff": "Anaerobic Effect (list)",
    "IntensityMin": "Intensity Minutes",
    "IntensityMod": "Intensity Moderate (min)",
    "IntensityVig": "Intensity Vigorous (min)",
}
# --- END CHANGE ---

# CHANGED: New header list based on user request
SHEET_HEADERS = [
    P["Date"], P["weekday"], P["WeightLb"], P["TrainingReadiness"], P["TrainingStatus"],
    P["RestingHR"], P["HRV"], P["RespirationRateAvg"], P["SleepScoreOverall"],
    P["SleepTotalH"], P["SleepLightH"], P["SleepDeepH"], P["SleepRemH"], P["SleepAwakeH"],
    P["SleepStart"], P["SleepEnd"], P["SS_overall"], P["SS_total_duration"], P["SS_stress"],
    P["SS_awake_count"], P["SS_rem_percentage"], P["SS_restlessness"], P["SS_light_percentage"],
    P["SS_deep_percentage"], P["StressAvg"], P["StressMax"], P["StressRestH"], P["StressLowH"],
    P["StressMediumH"], P["StressHighH"], P["StressUncatH"], P["BodyBatteryAvg"],
    P["BodyBatteryMax"], P["BodyBatteryMin"], P["Steps"], P["StepGoal"], P["WalkDistanceMi"],
    P["ActivityCount"], P["ActivityDistanceMi"], P["ActivityDurationMin"], P["ActivityCalories"],
    P["ActivityNames"], P["ActivityTypes"], P["PrimarySport"], P["ActivityTypesUnique"],
    P["ActTrainingEff"], P["ActAerobicEff"], P["ActAnaerobicEff"], P["IntensityMin"],
    P["IntensityMod"], P["IntensityVig"]
]
# --- END CHANGE ---

# -----------------------------
# Google Sheets helpers (Unchanged from original)
# -----------------------------
def _gspread_client():
    file_path = os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE")
    json_inline = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")

    if file_path and os.path.exists(file_path):
        creds = service_account.Credentials.from_service_account_file(file_path, scopes=[
            "https://www.googleapis.com/auth/spreadsheets"
        ])
    elif json_inline:
        try: data = json.loads(json_inline)
        except Exception as e: print("GOOGLE_SERVICE_ACCOUNT_JSON could not be parsed:", e); sys.exit(1)
        creds = service_account.Credentials.from_service_account_info(data, scopes=[
            "https://www.googleapis.com/auth/spreadsheets"
        ])
    else: print("Provide GOOGLE_SERVICE_ACCOUNT_FILE or GOOGLE_SERVICE_ACCOUNT_JSON"); sys.exit(1)
    return gspread.authorize(creds)

def _open_or_create_worksheet(gc, spreadsheet_id: str, title: str):
    sh = gc.open_by_key(spreadsheet_id)
    try: ws = sh.worksheet(title)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=title, rows=2000, cols=max(50, len(SHEET_HEADERS)))
        ws.append_row(SHEET_HEADERS, value_input_option="RAW")
        return ws

    # Ensure headers exist & in expected order (and shrink extra columns)
    try: existing = ws.row_values(1)
    except Exception: existing = []
    if existing != SHEET_HEADERS:
        if ws.col_count < len(SHEET_HEADERS): ws.add_cols(len(SHEET_HEADERS) - ws.col_count)
        elif ws.col_count > len(SHEET_HEADERS):
            try: ws.resize(cols=len(SHEET_HEADERS))
            except Exception: pass
        ws.update(range_name=f"A1:{gspread.utils.rowcol_to_a1(1, len(SHEET_HEADERS))}",
                  values=[SHEET_HEADERS])
    return ws

def _read_date_index(ws):
    try: col = ws.col_values(1) # includes header
    except Exception: col = []
    idx = {}
    for i, v in enumerate(col[1:], start=2): # skip header
        if v: idx[v] = i
    return idx

# -----------------------------
# Garmin data fetchers
# -----------------------------

# CHANGED: Updated fetch_steps to use garth directly for reliability
def fetch_steps_for_date(d: date):
    """Fetches steps, goal, and distance using garth."""
    try:
        steps_data = garth.DailySteps.get(iso_date(d))
        if not steps_data: return None, None, None
        
        total_distance_m = getattr(steps_data, "total_distance", 0) or 0
        miles = round(total_distance_m / 1609.34, 2)
        steps = getattr(steps_data, "total_steps", None)
        goal = getattr(steps_data, "step_goal", None)
        return steps, goal, miles
    except Exception:
        return None, None, None
# --- END CHANGE ---

# Unchanged from original
def _format_score_value(source: dict, key_src: str):
    if not isinstance(source, dict): return None
    item = source.get(key_src) or {}
    if not isinstance(item, dict): return None
    score = item.get("score")
    if score is None: score = item.get("value", item.get("percentage"))
    qual = item.get("qualifierKey", item.get("qualifier"))
    score_str = "None" if score is None else str(score)
    return f"{score_str}({qual})" if qual is not None else score_str

# Unchanged from original
def _sleep_scores_from(data: dict) -> dict:
    scores = {}
    source = data.get("sleepScores") or data.get("dailySleepDTO", {}).get("sleepScores") or {}

    def qual(key):
        v = source.get(key) or {}
        return v.get("qualifierKey") or v.get("qualifier") or None

    scores["overall"] = qual("overall")
    scores["total_duration"] = qual("totalDuration")
    scores["stress"] = qual("stress")
    scores["restlessness"] = qual("restlessness")
    scores["awake_count_fmt"] = _format_score_value(source, "awakeCount")
    scores["rem_percentage_fmt"] = _format_score_value(source, "remPercentage")
    scores["light_percentage_fmt"] = _format_score_value(source, "lightPercentage")
    scores["deep_percentage_fmt"] = _format_score_value(source, "deepPercentage")
    if scores["light_percentage_fmt"] is None: scores["light_percentage_fmt"] = _format_score_value(source, "light_percentage")
    if scores["deep_percentage_fmt"] is None: scores["deep_percentage_fmt"] = _format_score_value(source, "deep_percentage")

    # CHANGED: Extract the overall score value
    scores["overall_score_value"] = (source.get("overall") or {}).get("score")
    # --- END CHANGE ---

    return scores

# Unchanged from original except using the Garmin object `g` passed in
def fetch_sleep_for_date(g: Garmin, d: date): # Pass 'g'
    try:
        data = g.get_sleep_data(iso_date(d)) or {} # Use 'g'
        daily = data.get("dailySleepDTO") or {}
        total = sum((daily.get(k) or 0) for k in ["deepSleepSeconds","lightSleepSeconds","remSleepSeconds"])
        start_ms = daily.get("sleepStartTimestampGMT") or data.get("sleepStartTimestampGMT") or daily.get("sleepStartTimestampLocal")
        end_ms = daily.get("sleepEndTimestampGMT") or data.get("sleepEndTimestampGMT") or daily.get("sleepEndTimestampLocal")
        start_local_iso = ms_to_local_iso(start_ms)
        end_local_iso = ms_to_local_iso(end_ms)
        scores = _sleep_scores_from(data)

        return {
            "total_h": round(total / 3600, 2),
            "light_h": round((daily.get("lightSleepSeconds") or 0) / 3600, 2),
            "deep_h": round((daily.get("deepSleepSeconds") or 0) / 3600, 2),
            "rem_h":  round((daily.get("remSleepSeconds") or 0) / 3600, 2),
            "awake_h":round((daily.get("awakeSleepSeconds") or 0) / 3600, 2),
            "resting_hr": data.get("restingHeartRate") or daily.get("restingHeartRate"),
            "start_local": start_local_iso,
            "end_local": end_local_iso,
            "scores": scores,
        }
    except Exception:
        return {}

# Unchanged from original
def fetch_activities_bulk(g: Garmin, start_d: date):
    try: acts = g.get_activities(0, 500) or []
    except Exception: acts = []
    keep = []
    for a in acts:
        dt_str = (a.get("startTimeLocal") or a.get("startTimeGMT") or "")[:10]
        try:
            if dt_str and datetime.strptime(dt_str, "%Y-%m-%d").date() >= start_d: keep.append(a)
        except Exception: pass
    return keep

# Unchanged from original
def aggregate_activities_by_date(activities):
    by_date = defaultdict(lambda: {"count":0,"dist_mi":0.0,"dur_min":0.0,"cal":0.0, "names": [], "types": [], "te": [], "ae": [], "ane": []})
    for a in activities:
        dt = (a.get("startTimeLocal") or a.get("startTimeGMT") or "")[:10]
        if not dt: continue
        entry = by_date[dt]
        entry["count"] += 1
        entry["dist_mi"] += (a.get("distance") or 0) / 1609.34
        entry["dur_min"] += (a.get("duration") or 0) / 60.0
        entry["cal"] += float(a.get("calories") or 0)
        name = _first_present(a, ["activityName","activityId"], "")
        tdict = a.get("activityType") or {}
        atype = tdict.get("typeKey") if isinstance(tdict, dict) else ""
        te_label = _first_present(a, ["trainingEffectLabel","overallTrainingEffectMessage","trainingEffectMessage"])
        ae_msg = _first_present(a, ["aerobicTrainingEffectMessage","aerobicTrainingEffectLabel"])
        ane_msg = _first_present(a, ["anaerobicTrainingEffectMessage","anaerobicTrainingEffectLabel"])
        if name: entry["names"].append(str(name))
        if atype: entry["types"].append(str(atype))
        if te_label: entry["te"].append(str(te_label))
        if ae_msg: entry["ae"].append(str(ae_msg))
        if ane_msg: entry["ane"].append(str(ane_msg))

    for dt, v in by_date.items():
        v["dist_mi"] = round(v["dist_mi"], 2)
        v["dur_min"] = round(v["dur_min"], 2)
        v["cal"] = round(v["cal"], 0)
        type_counts = Counter(v["types"])
        primary = type_counts.most_common(1)[0][0] if type_counts else ""
        unique_types = " ".join(sorted(set(v["types"])))
        v["primary"] = primary
        v["types_unique"] = unique_types
        v["names"] = " ".join(v["names"])
        v["types"] = " ".join(v["types"])
        v["te"]    = " ".join(v["te"])
        v["ae"]    = " ".join(v["ae"])
        v["ane"]   = " ".join(v["ane"])
    return by_date

# Unchanged from original
def map_intensity_last_n(n_days=50):
    out = {}
    try:
        rows = garth.DailyIntensityMinutes.list(period=n_days) or []
        for r in rows:
            d = r.calendar_date.isoformat()
            mod = getattr(r, "moderate_value", None)
            vig = getattr(r, "vigorous_value", None)
            total = None
            if mod is not None or vig is not None: total = (mod or 0) + 2 * (vig or 0)
            out[d] = {"total": total, "mod": mod, "vig": vig}
    except Exception: pass
    return out

# Unchanged from original
def map_hrv_last_n(n_days=50):
    out = {}
    try:
        rows = garth.DailyHRV.list(period=n_days) or []
        for r in rows:
            d = r.calendar_date.isoformat()
            out[d] = getattr(r, "last_night_avg", None) or getattr(r, "weekly_avg", None)
    except Exception: pass
    return out

# -----------------------------
# Main
# -----------------------------
def main():
    load_dotenv()

    tz = get_local_tz()
    print(f"[tz] Using timezone: {tz}")
    print(f"[tz] Today in tz: {datetime.now(tz).date()}")

    spreadsheet_id = os.getenv("GOOGLE_SHEETS_SPREADSHEET_ID")
    if not spreadsheet_id: print("Missing GOOGLE_SHEETS_SPREADSHEET_ID"); sys.exit(1)
    worksheet_title = os.getenv("GOOGLE_SHEETS_WORKSHEET_TITLE2", "Garmin Daily Expanded") # Use the correct env var

    end_d_inclusive = today_local() if os.getenv('INCLUDE_TODAY', '1') != '0' else today_local() - timedelta(days=1)
    window_days = int(os.getenv('WINDOW_DAYS', '5'))
    start_d = end_d_inclusive - timedelta(days=window_days - 1)

    g, token_store = login_to_garmin() # 'g' is the Garmin object

    gc = _gspread_client()
    ws = _open_or_create_worksheet(gc, spreadsheet_id, worksheet_title)
    date_index = _read_date_index(ws)

    # Pre-fetch bulk data (unchanged)
    intensity_map = map_intensity_last_n(window_days)
    hrv_map = map_hrv_last_n(window_days)
    activities = fetch_activities_bulk(g, start_d)
    act_by_date = aggregate_activities_by_date(activities)

    updates = 0
    appends = 0

    for d in daterange(start_d, end_d_inclusive + timedelta(days=1)):
        d_iso = iso_date(d)
        print(f"Processing date: {d_iso}") # Added for debugging

        # --- Fetch Data ---
        # CHANGED: Using garth directly for most wellness, Garmin object for others
        steps, step_goal, walk_mi = fetch_steps_for_date(d) # Uses garth now
        sleep = fetch_sleep_for_date(g, d) or {} # Uses Garmin object

        bb_avg = bb_min = bb_max = stress_avg = stress_max = None
        rest_stress_s = low_stress_s = med_stress_s = high_stress_s = uncat_stress_s = None
        try:
            daily_bb_stress = garth.DailyBodyBatteryStress.get(d_iso)
            if daily_bb_stress:
                stress_avg = getattr(daily_bb_stress, "avg_stress_level", None)
                stress_max = getattr(daily_bb_stress, "max_stress_level", None)
                rest_stress_s = getattr(daily_bb_stress, "rest_stress_duration_seconds", None)
                low_stress_s = getattr(daily_bb_stress, "low_stress_duration_seconds", None)
                med_stress_s = getattr(daily_bb_stress, "medium_stress_duration_seconds", None)
                high_stress_s = getattr(daily_bb_stress, "high_stress_duration_seconds", None)
                uncat_stress_s = getattr(daily_bb_stress, "uncategorized_stress_duration_seconds", None)
                bb_max = getattr(daily_bb_stress, "max_body_battery", None) # Get Max BB

                levels = [getattr(x, "level", None) for x in getattr(daily_bb_stress, "body_battery_readings", [])]
                levels = [lv for lv in levels if isinstance(lv, (int, float))]
                if levels:
                    bb_avg = round(sum(levels) / len(levels), 1)
                    bb_min = min(levels)
        except Exception as e: print(f" Error fetching BB/Stress for {d_iso}: {e}")

        inten = intensity_map.get(d_iso, {})
        intensity_total = inten.get("total")
        intensity_mod = inten.get("mod")
        intensity_vig = inten.get("vig")

        hrv = hrv_map.get(d_iso)

        weight_lb = None
        try:
            w = garth.WeightData.get(d_iso)
            if w:
                grams = getattr(w, "weight", None)
                if grams is not None: weight_lb = round((grams / 1000) * 2.20462, 2)
        except Exception as e: print(f" Error fetching Weight for {d_iso}: {e}")

        readiness = None
        try:
            r = garth.DailyTrainingReadiness.get(d_iso)
            if r: readiness = getattr(r, "training_readiness", None)
        except Exception as e: print(f" Error fetching Readiness for {d_iso}: {e}")

        training_status = None
        try:
            ts = garth.DailyTrainingStatus.get(d_iso)
            if ts: training_status = getattr(ts, "training_status", None)
        except Exception as e: print(f" Error fetching Training Status for {d_iso}: {e}")

        respiration_avg = None
        try:
            resp = garth.DailyRespiration.get(d_iso)
            if resp: respiration_avg = getattr(resp, "avg_overall_breaths_per_min", None)
        except Exception as e: print(f" Error fetching Respiration for {d_iso}: {e}")

        act = act_by_date.get(d_iso, {"count":0,"dist_mi":0.0,"dur_min":0.0,"cal":0, "names":"", "types":"", "te":"", "ae":"", "ane":"", "primary":"", "types_unique":""})
        # --- END Fetch Data ---

        # --- Populate props dictionary ---
        # CHANGED: Map all new and existing fields according to the new P and SHEET_HEADERS
        props = {
            P["Date"]: d_iso,
            P["weekday"]: calendar.day_name[d.weekday()],
            P["WeightLb"]: weight_lb,
            P["TrainingReadiness"]: readiness,
            P["TrainingStatus"]: training_status,
            P["RestingHR"]: sleep.get("resting_hr"), # RHR comes from sleep data fetch
            P["HRV"]: hrv,
            P["RespirationRateAvg"]: respiration_avg,
            P["SleepScoreOverall"]: (sleep.get("scores", {}) or {}).get("overall_score_value"), # Overall score value
            P["SleepTotalH"]: sleep.get("total_h"),
            P["SleepLightH"]: sleep.get("light_h"),
            P["SleepDeepH"]: sleep.get("deep_h"),
            P["SleepRemH"]: sleep.get("rem_h"),
            P["SleepAwakeH"]: sleep.get("awake_h"),
            P["SleepStart"]: sleep.get("start_local"),
            P["SleepEnd"]: sleep.get("end_local"),
            P["SS_overall"]: (sleep.get("scores", {}) or {}).get("overall"),
            P["SS_total_duration"]: (sleep.get("scores", {}) or {}).get("total_duration"),
            P["SS_stress"]: (sleep.get("scores", {}) or {}).get("stress"),
            P["SS_awake_count"]: (sleep.get("scores", {}) or {}).get("awake_count_fmt"),
            P["SS_rem_percentage"]: (sleep.get("scores", {}) or {}).get("rem_percentage_fmt"),
            P["SS_restlessness"]: (sleep.get("scores", {}) or {}).get("restlessness"),
            P["SS_light_percentage"]: (sleep.get("scores", {}) or {}).get("light_percentage_fmt"),
            P["SS_deep_percentage"]: (sleep.get("scores", {}) or {}).get("deep_percentage_fmt"),
            P["StressAvg"]: stress_avg,
            P["StressMax"]: stress_max,
            P["StressRestH"]: seconds_to_hours(rest_stress_s),
            P["StressLowH"]: seconds_to_hours(low_stress_s),
            P["StressMediumH"]: seconds_to_hours(med_stress_s),
            P["StressHighH"]: seconds_to_hours(high_stress_s),
            P["StressUncatH"]: seconds_to_hours(uncat_stress_s),
            P["BodyBatteryAvg"]: bb_avg,
            P["BodyBatteryMax"]: bb_max,
            P["BodyBatteryMin"]: bb_min,
            P["Steps"]: steps,
            P["StepGoal"]: step_goal,
            P["WalkDistanceMi"]: walk_mi,
            P["ActivityCount"]: act["count"],
            P["ActivityDistanceMi"]: act["dist_mi"],
            P["ActivityDurationMin"]: act["dur_min"],
            P["ActivityCalories"]: act["cal"],
            P["ActivityNames"]: act.get("names", ""),
            P["ActivityTypes"]: act.get("types", ""),
            P["PrimarySport"]: act.get("primary", ""),
            P["ActivityTypesUnique"]: act.get("types_unique", ""),
            P["ActTrainingEff"]: act.get("te", ""),
            P["ActAerobicEff"]: act.get("ae", ""),
            P["ActAnaerobicEff"]: act.get("ane", ""),
            P["IntensityMin"]: intensity_total,
            P["IntensityMod"]: intensity_mod,
            P["IntensityVig"]: intensity_vig,
        }
        # --- END Populate props ---

        # Upsert by Date (first column) (Unchanged from original)
        row_values = [props.get(h, "") for h in SHEET_HEADERS]
        if d_iso in date_index:
            row_num = date_index[d_iso]
            rng = f"A{row_num}:{gspread.utils.rowcol_to_a1(row_num, len(SHEET_HEADERS))}"
            ws.update(range_name=rng, values=[row_values], value_input_option="RAW")
            updates += 1
        else:
            ws.append_row(row_values, value_input_option="RAW")
            try: last = len(ws.col_values(1)); date_index[d_iso] = last
            except Exception: pass
            appends += 1
        time.sleep(0.05) # Rate limiting

    print(f"Done. Upserted {updates} updates; {appends} inserts into Google Sheets '{worksheet_title}'.")

if __name__ == "__main__":
    main()