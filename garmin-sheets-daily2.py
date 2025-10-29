"""
Garmin → Google Sheets Daily Rollup — v4.4.1
Change: Corrected all data extraction keys based on raw API logs.
Consolidated fetching to use g.get_stats() for Stress, Intensity, Steps, and BB Min/Max.
Fixed Body Battery Avg calculation from g.get_body_battery().
Removed Respiration Rate and Uncategorized Stress.
"""

from datetime import date, datetime, timedelta, timezone
from collections import defaultdict, Counter
import os, sys, time, calendar, re, json, base64, tarfile, io
from dotenv import load_dotenv
from garminconnect import Garmin
import garth
from zoneinfo import ZoneInfo

# Google Sheets
import gspread
from google.oauth2 import service_account

# Configure logging (removed)


# -----------------------------
# Timezone config (Unchanged)
# -----------------------------
def get_local_tz():
    tzname = os.getenv("LOCAL_TZ") or os.getenv("TIMEZONE") or os.getenv("TZ")
    if tzname:
        try: return ZoneInfo(tzname)
        except Exception: pass
    try: return datetime.now().astimezone().tzinfo
    except Exception: return timezone.utc

# -----------------------------
# Helpers (Unchanged)
# -----------------------------
def iso_date(d: date) -> str: return d.isoformat()
def today_local() -> date: tz = get_local_tz(); return datetime.now(tz).date()

def daterange(start: date, end_exclusive: date):
    d = start
    while d < end_exclusive:
        yield d
        d += timedelta(days=1)

def ms_to_local_iso(ms: int | None) -> str | None:
    if not ms: return None
    try:
        dt_utc = datetime.fromtimestamp(ms / 1000, tz=timezone.utc); tz = get_local_tz()
        dt_local = dt_utc.astimezone(tz); dt_local = dt_local.replace(second=0, microsecond=0)
        return dt_local.isoformat()
    except Exception: return None

def to_tokens(value):
    if value is None: return []
    if isinstance(value, list): return [str(v).strip() for v in value if str(v).strip()]
    s = str(value).strip();
    if not s: return []
    if any(sep in s for sep in [",", ";", "|"]): parts = re.split(r"[,\;\|]+", s)
    else: parts = s.split()
    return [p.strip() for p in parts if p.strip()]

def _first_present(dct, keys, default=None):
    for k in keys:
        if k in dct and dct.get(k) not in (None, ""): return dct.get(k)
    return default

def seconds_to_hours(seconds):
    if isinstance(seconds, (int, float)): return round(seconds / 3600.0, 2)
    return ""

def try_get(data, keys, default=""):
    if data is None: return default
    temp = data
    try:
        for key in keys:
            if temp is None: return default
            temp = temp[key]
        return temp if temp is not None else default
    except (KeyError, TypeError, IndexError): return default


# -----------------------------
# Garmin login (CI-safe) (Unchanged)
# -----------------------------
def _ensure_dir(path: str): os.makedirs(path, exist_ok=True)
def _maybe_restore_token_dir_from_tgz(token_dir: str):
    b64 = os.getenv("GARMIN_TOKEN_STORE_TGZ_B64");
    if not b64: return
    needs_restore = (not os.path.exists(token_dir)) or (os.path.isdir(token_dir) and not os.listdir(token_dir))
    if not needs_restore: return
    try:
        _ensure_dir(token_dir); data = base64.b64decode(b64)
        with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as tar: tar.extractall(token_dir)
        print(f"[garmin] Restored token-store from GARMIN_TOKEN_STORE_TGZ_B64 into {token_dir}")
    except Exception as e: print(f"[garmin] Failed to restore token-store from GARMIN_TOKEN_STORE_TGZ_B64: {e}")

def login_to_garmin():
    garmin_email = os.getenv("GARMIN_EMAIL"); garmin_password = os.getenv("GARMIN_PASSWORD")
    token_store = os.getenv("GARMIN_TOKEN_STORE", "~/.garmin_tokens"); token_store = os.path.expanduser(token_store).rstrip("/")
    mfa_code = os.getenv("GARMIN_MFA_CODE")
    if not garmin_email or not garmin_password: print("ERROR: Missing GARMIN_EMAIL or GARMIN_PASSWORD"); sys.exit(1)
    if os.path.exists(token_store) and not os.path.isdir(token_store): print(f"ERROR: GARMIN_TOKEN_STORE points to a file: {token_store}. Expected a directory."); sys.exit(1)
    _maybe_restore_token_dir_from_tgz(token_store)
    try: garth.resume(token_store); print(f"[garmin] Resumed tokens from {token_store} for garth")
    except Exception as resume_err:
        print(f"[garmin] No usable tokens for garth, will login: {resume_err}")
        if mfa_code:
            print("[garmin] Performing non-interactive MFA login for garth")
            client_state = garth.login(garmin_email, garmin_password, return_on_mfa=True)
            if client_state: garth.resume_login(client_state, mfa_code)
        else: garth.login(garmin_email, garmin_password)
        _ensure_dir(token_store); garth.save(token_store); print(f"[garmin] Saved new garth tokens to {token_store}")
    g = Garmin(garmin_email, garmin_password)
    try:
        g.login(tokenstore=token_store)
        print(f"[garmin] Garmin object login successful using tokens from {token_store}")
        return g, token_store
    except Exception as e:
        print(f"ERROR: Garmin object login error: {e}")
        try: g.login(); print(f"[garmin] Garmin object login successful on fallback."); return g, token_store
        except Exception as e2: print(f"ERROR: Full login error: {e2}"); sys.exit(1)

# -----------------------------
# Column map (Removed Respiration and Uncategorized Stress)
# -----------------------------
P = {
    "Date": "Date", "weekday": "weekday", "WeightLb": "Weight (lb)", "TrainingReadiness": "Training Readiness (0-100)",
    "TrainingStatus": "Training Status", "RestingHR": "Resting HR", "HRV": "HRV",
    # "RespirationRateAvg": "Respiration Rate Avg (BPM)", # Removed
    "SleepScoreOverall": "Sleep Score (0-100)", "SleepTotalH": "Sleep Total (h)", "SleepLightH": "Sleep Light (h)",
    "SleepDeepH": "Sleep Deep (h)", "SleepRemH": "Sleep REM (h)", "SleepAwakeH": "Sleep Awake (h)", "SleepStart": "Sleep Start (local)",
    "SleepEnd": "Sleep End (local)", "SS_overall": "Sleep Overall (q)", "SS_total_duration": "Sleep Duration (q)",
    "SS_stress": "Sleep Stress (q)", "SS_awake_count": "Sleep Awake Count (q)", "SS_rem_percentage": "Sleep REM % (q)",
    "SS_restlessness": "Sleep Restlessness (q)", "SS_light_percentage": "Sleep Light % (q)", "SS_deep_percentage": "Sleep Deep % (q)",
    "StressAvg": "Stress Avg", "StressMax": "Stress Max", "StressRestH": "Rest Stress Duration(h)", "StressLowH": "Low Stress Duration (h)",
    "StressMediumH": "Medium Stress Duration (h)", "StressHighH": "High Stress Duration (h)",
    # "StressUncatH": "Uncategorized Stress Duration (h)", # Removed
    "BodyBatteryAvg": "Body Battery Avg", "BodyBatteryMax": "Body Battery Max", "BodyBatteryMin": "Body Battery Min",
    "Steps": "Steps", "StepGoal": "Step Goal", "WalkDistanceMi": "Walk Distance (mi)", "ActivityCount": "Activities (#)",
    "ActivityDistanceMi": "Activity Distance (mi)", "ActivityDurationMin": "Activity Duration (min)", "ActivityCalories": "Activity Calories",
    "ActivityNames": "Activity Names", "ActivityTypes": "Activity Types", "PrimarySport": "primary_sport",
    "ActivityTypesUnique": "activity_types_unique", "ActTrainingEff": "Training Effect (list)", "ActAerobicEff": "Aerobic Effect (list)",
    "ActAnaerobicEff": "Anaerobic Effect (list)", "IntensityMin": "Intensity Minutes", "IntensityMod": "Intensity Moderate (min)",
    "IntensityVig": "Intensity Vigorous (min)",
}

# -----------------------------
# SHEET_HEADERS (Removed Respiration and Uncategorized Stress)
# -----------------------------
SHEET_HEADERS = [
    P["Date"], P["weekday"], P["WeightLb"], P["TrainingReadiness"], P["TrainingStatus"], P["RestingHR"], P["HRV"],
    # P["RespirationRateAvg"], # Removed
    P["SleepScoreOverall"], P["SleepTotalH"], P["SleepLightH"], P["SleepDeepH"], P["SleepRemH"],
    P["SleepAwakeH"], P["SleepStart"], P["SleepEnd"], P["SS_overall"], P["SS_total_duration"], P["SS_stress"],
    P["SS_awake_count"], P["SS_rem_percentage"], P["SS_restlessness"], P["SS_light_percentage"], P["SS_deep_percentage"],
    P["StressAvg"], P["StressMax"], P["StressRestH"], P["StressLowH"], P["StressMediumH"], P["StressHighH"],
    P["BodyBatteryAvg"], P["BodyBatteryMax"], P["BodyBatteryMin"], P["Steps"], P["StepGoal"], P["WalkDistanceMi"],
    P["ActivityCount"], P["ActivityDistanceMi"], P["ActivityDurationMin"], P["ActivityCalories"], P["ActivityNames"],
    P["ActivityTypes"], P["PrimarySport"], P["ActivityTypesUnique"], P["ActTrainingEff"], P["ActAerobicEff"], P["ActAnaerEff"],
    P["IntensityMin"], P["IntensityMod"], P["IntensityVig"]
]


# -----------------------------
# Google Sheets helpers (Unchanged)
# -----------------------------
def _gspread_client():
    file_path = os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE"); json_inline = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
    if file_path and os.path.exists(file_path): creds = service_account.Credentials.from_service_account_file(file_path, scopes=["https://www.googleapis.com/auth/spreadsheets"])
    elif json_inline:
        try: data = json.loads(json_inline)
        except Exception as e: print("ERROR: GOOGLE_SERVICE_ACCOUNT_JSON could not be parsed:", e); sys.exit(1)
        creds = service_account.Credentials.from_service_account_info(data, scopes=["https://www.googleapis.com/auth/spreadsheets"])
    else: print("ERROR: Provide GOOGLE_SERVICE_ACCOUNT_FILE or GOOGLE_SERVICE_ACCOUNT_JSON"); sys.exit(1)
    return gspread.authorize(creds)

def _open_or_create_worksheet(gc, spreadsheet_id: str, title: str):
    sh = gc.open_by_key(spreadsheet_id)
    try: ws = sh.worksheet(title)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=title, rows=2000, cols=max(50, len(SHEET_HEADERS)))
        ws.append_row(SHEET_HEADERS, value_input_option="RAW"); return ws
    try: existing = ws.row_values(1)
    except Exception: existing = []
    if existing != SHEET_HEADERS:
        current_cols = ws.col_count; target_cols = len(SHEET_HEADERS)
        if current_cols < target_cols: ws.add_cols(target_cols - current_cols); print(f"Added {target_cols - current_cols} columns to worksheet '{title}'")
        elif current_cols > target_cols:
            try: pass
            except Exception as resize_err: print(f"WARNING: Could not resize worksheet columns: {resize_err}. Leaving extra columns.")
        header_range = f"A1:{gspread.utils.rowcol_to_a1(1, target_cols)}"; ws.update(range_name=header_range, values=[SHEET_HEADERS]); print(f"Updated headers for worksheet '{title}' to match target.")
    return ws

def _read_date_index(ws):
    try: col = ws.col_values(1)
    except Exception: col = []
    idx = {}
    for i, v in enumerate(col[1:], start=2):
        if v: idx[v] = i
    return idx

# -----------------------------
# Garmin data fetchers
# -----------------------------

# Unchanged
def _format_score_value(source: dict, key_src: str):
    if not isinstance(source, dict): return None
    item = source.get(key_src) or {};
    if not isinstance(item, dict): return None
    score = item.get("score");
    if score is None: score = item.get("value", item.get("percentage"))
    qual = item.get("qualifierKey", item.get("qualifier"))
    score_str = "None" if score is None else str(score)
    return f"{score_str}({qual})" if qual is not None else score_str

# Unchanged (still gets overall_score_value)
def _sleep_scores_from(data: dict) -> dict:
    scores = {}; source = data.get("sleepScores") or data.get("dailySleepDTO", {}).get("sleepScores") or {}
    def qual(key): v = source.get(key) or {}; return v.get("qualifierKey") or v.get("qualifier") or None
    scores["overall"] = qual("overall"); scores["total_duration"] = qual("totalDuration"); scores["stress"] = qual("stress")
    scores["restlessness"] = qual("restlessness"); scores["awake_count_fmt"] = _format_score_value(source, "awakeCount")
    scores["rem_percentage_fmt"] = _format_score_value(source, "remPercentage"); scores["light_percentage_fmt"] = _format_score_value(source, "lightPercentage")
    scores["deep_percentage_fmt"] = _format_score_value(source, "deepPercentage")
    if scores["light_percentage_fmt"] is None: scores["light_percentage_fmt"] = _format_score_value(source, "light_percentage")
    if scores["deep_percentage_fmt"] is None: scores["deep_percentage_fmt"] = _format_score_value(source, "deep_percentage")
    # This correctly extracts the numeric score needed for Sleep Score (0-100)
    scores["overall_score_value"] = try_get(source, ['overall', 'score'])
    return scores

# Unchanged
def fetch_sleep_for_date(g: Garmin, d: date): # Removed first_day flag
    try:
        data = g.get_sleep_data(iso_date(d)) or {}
        daily = data.get("dailySleepDTO") or {}
        total = sum((daily.get(k) or 0) for k in ["deepSleepSeconds","lightSleepSeconds","remSleepSeconds"])
        start_ms = daily.get("sleepStartTimestampGMT") or data.get("sleepStartTimestampGMT") or daily.get("sleepStartTimestampLocal")
        end_ms = daily.get("sleepEndTimestampGMT") or data.get("sleepEndTimestampGMT") or daily.get("sleepEndTimestampLocal")
        start_local_iso = ms_to_local_iso(start_ms); end_local_iso = ms_to_local_iso(end_ms)
        scores = _sleep_scores_from(data)
        return {
            "total_h": round(total / 3600, 2), "light_h": round((daily.get("lightSleepSeconds") or 0) / 3600, 2),
            "deep_h": round((daily.get("deepSleepSeconds") or 0) / 3600, 2), "rem_h":  round((daily.get("remSleepSeconds") or 0) / 3600, 2),
            "awake_h":round((daily.get("awakeSleepSeconds") or 0) / 3600, 2), "resting_hr": data.get("restingHeartRate") or daily.get("restingHeartRate"),
            "start_local": start_local_iso, "end_local": end_local_iso, "scores": scores,
        }
    except Exception as e:
        print(f"WARNING: Could not fetch sleep data for {iso_date(d)}: {e}")
        return {}

# Unchanged
def fetch_activities_bulk(g: Garmin, start_d: date):
    try: acts = g.get_activities(0, 500) or []
    except Exception as e:
        print(f"WARNING: Could not fetch activities bulk: {e}")
        acts = []
    keep = []
    for a in acts:
        dt_str = (a.get("startTimeLocal") or a.get("startTimeGMT") or "")[:10]
        try:
            if dt_str and datetime.strptime(dt_str, "%Y-%m-%d").date() >= start_d: keep.append(a)
        except Exception: pass
    return keep

# Unchanged
def aggregate_activities_by_date(activities):
    by_date = defaultdict(lambda: {"count":0,"dist_mi":0.0,"dur_min":0.0,"cal":0.0, "names": [], "types": [], "te": [], "ae": [], "ane": []})
    for a in activities:
        dt = (a.get("startTimeLocal") or a.get("startTimeGMT") or "")[:10];
        if not dt: continue
        entry = by_date[dt]; entry["count"] += 1; entry["dist_mi"] += (a.get("distance") or 0) / 1609.34
        entry["dur_min"] += (a.get("duration") or 0) / 60.0; entry["cal"] += float(a.get("calories") or 0)
        name = _first_present(a, ["activityName","activityId"], ""); tdict = a.get("activityType") or {}
        atype = tdict.get("typeKey") if isinstance(tdict, dict) else ""
        te_label = _first_present(a, ["trainingEffectLabel","overallTrainingEffectMessage","trainingEffectMessage"])
        ae_msg = _first_present(a, ["aerobicTrainingEffectMessage","aerobicTrainingEffectLabel"])
        ane_msg = _first_present(a, ["anaerobicTrainingEffectMessage","anaerobicTrainingEffectLabel"])
        if name: entry["names"].append(str(name));
        if atype: entry["types"].append(str(atype))
        if te_label: entry["te"].append(str(te_label));
        if ae_msg: entry["ae"].append(str(ae_msg))
        if ane_msg: entry["ane"].append(str(ane_msg))
    for dt, v in by_date.items():
        v["dist_mi"] = round(v["dist_mi"], 2); v["dur_min"] = round(v["dur_min"], 2); v["cal"] = round(v["cal"], 0)
        type_counts = Counter(v["types"]); primary = type_counts.most_common(1)[0][0] if type_counts else ""
        unique_types = " ".join(sorted(set(v["types"]))); v["primary"] = primary; v["types_unique"] = unique_types
        v["names"] = " ".join(v["names"]); v["types"] = " ".join(v["types"]); v["te"] = " ".join(v["te"])
        v["ae"] = " ".join(v["ae"]); v["ane"] = " ".join(v["ane"])
    return by_date

# Unchanged
def map_hrv_last_n(n_days=50):
    out = {}
    try:
        rows = garth.DailyHRV.list(period=n_days) or []
        for r in rows:
            d = r.calendar_date.isoformat();
            out[d] = getattr(r, "last_night_avg", None) or getattr(r, "weekly_avg", None)
    except Exception as e:
        print(f"WARNING: Could not fetch HRV map: {e}")
    return out


# -----------------------------
# Main
# -----------------------------
def main():
    load_dotenv()

    tz = get_local_tz(); print(f"[tz] Using timezone: {tz}"); print(f"[tz] Today in tz: {datetime.now(tz).date()}")

    spreadsheet_id = os.getenv("GOOGLE_SHEETS_SPREADSHEET_ID")
    if not spreadsheet_id: print("ERROR: Missing GOOGLE_SHEETS_SPREADSHEET_ID"); sys.exit(1)
    worksheet_title = os.getenv("GOOGLE_SHEETS_WORKSHEET_TITLE2", "Garmin Daily Expanded")

    end_d_inclusive = today_local() if os.getenv('INCLUDE_TODAY', '1') != '0' else today_local() - timedelta(days=1)
    window_days = int(os.getenv('WINDOW_DAYS', '14'))
    start_d = end_d_inclusive - timedelta(days=window_days - 1)

    g, token_store = login_to_garmin()

    gc = _gspread_client()
    ws = _open_or_create_worksheet(gc, spreadsheet_id, worksheet_title)
    date_index = _read_date_index(ws)

    # Pre-fetch bulk data
    hrv_map = map_hrv_last_n(window_days)
    activities = fetch_activities_bulk(g, start_d)
    act_by_date = aggregate_activities_by_date(activities)

    updates = 0
    appends = 0
    
    # CHANGED: Removed first_day_processed flag
    
    for d in daterange(start_d, end_d_inclusive + timedelta(days=1)):
        d_iso = iso_date(d)
        print(f"Processing date: {d_iso}")

        # --- Fetch Data using Garmin object 'g' ---
        
        # CHANGED: Use g.get_stats() for most daily metrics
        stats = {}
        try: stats = g.get_stats(d_iso) or {}
        except Exception as e: print(f"WARNING: Could not fetch stats for {d_iso}: {e}")

        # CHANGED: Use g.get_sleep_data()
        sleep = fetch_sleep_for_date(g, d) or {} 

        # CHANGED: Use g.get_stress_data() - The logs showed this was EMPTY, but g.get_stats() had the data.
        # We will now pull stress data from the 'stats' object.
        stress = stats # Use the 'stats' object which contains stress data
        
        bb_list = []
        try: bb_list = g.get_body_battery(d_iso) or []
        except Exception as e: print(f"WARNING: Could not fetch body battery for {d_iso}: {e}")

        readiness = {}
        try: readiness = g.get_training_readiness(d_iso) or {}
        except Exception as e: print(f"WARNING: Could not fetch readiness for {d_iso}: {e}")

        training_status = {}
        try: training_status = g.get_training_status(d_iso) or {}
        except Exception as e: print(f"WARNING: Could not fetch training status for {d_iso}: {e}")

        # Removed Respiration fetch

        # Fetch using garth (still reliable for these)
        hrv = hrv_map.get(d_iso)

        weight_lb = None
        try:
            w = garth.WeightData.get(d_iso)
            if w: grams = getattr(w, "weight", None);
            if grams is not None: weight_lb = round((grams / 1000) * 2.20462, 2)
        except Exception as e: print(f"WARNING: Could not fetch Weight for {d_iso}: {e}")

        # Intensity Minutes now come from get_stats (fetched above)
        intensity_mod = try_get(stats, ['moderateIntensityMinutes'], 0)
        intensity_vig = try_get(stats, ['vigorousIntensityMinutes'], 0)
        # Calculate total: (Moderate * 1) + (Vigorous * 2)
        intensity_total = (intensity_mod or 0) + ((intensity_vig or 0) * 2)


        act = act_by_date.get(d_iso, {"count":0,"dist_mi":0.0,"dur_min":0.0,"cal":0, "names":"", "types":"", "te":"", "ae":"", "ane":"", "primary":"", "types_unique":""})
        # --- END Fetch Data ---

        # --- Calculate Body Battery Avg/Min/Max ---
        bb_avg = bb_min_calc = bb_max_calc = None
        # Use g.get_body_battery() response for calculating average
        if bb_list and isinstance(bb_list, list) and len(bb_list) > 0 and 'bodyBatteryValuesArray' in bb_list[0]:
            values = [pair[1] for pair in bb_list[0]['bodyBatteryValuesArray'] if pair and len(pair) > 1 and isinstance(pair[1], int) and pair[1] >= 0]
            if values:
                bb_avg = round(sum(values) / len(values))
        
        # Use g.get_stats() for Min/Max as it's more direct
        bb_min = try_get(stats, ['bodyBatteryLowestValue'], "")
        bb_max = try_get(stats, ['bodyBatteryHighestValue'], "")


        # --- Populate props dictionary ---
        # CHANGED: Corrected all keys based on logs and requests
        props = {
            P["Date"]: d_iso,
            P["weekday"]: calendar.day_name[d.weekday()],
            P["WeightLb"]: weight_lb,
            P["TrainingReadiness"]: try_get(readiness, [-1, 'score'], ""), # Get last score from list
            P["TrainingStatus"]: try_get(training_status, ['trainingStatusFeedbackPhrase'], ""), # Get feedback phrase
            P["RestingHR"]: sleep.get("resting_hr"),
            P["HRV"]: hrv,
            # Respiration Removed
            P["SleepScoreOverall"]: (sleep.get("scores", {}) or {}).get("overall_score_value"),
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
            P["StressAvg"]: try_get(stats, ['averageStressLevel'], ""), # From g.get_stats()
            P["StressMax"]: try_get(stats, ['maxStressLevel'], ""), # From g.get_stats()
            P["StressRestH"]: seconds_to_hours(try_get(stats, ['restStressDuration'])), # From g.get_stats()
            P["StressLowH"]: seconds_to_hours(try_get(stats, ['lowStressDuration'])), # From g.get_stats()
            P["StressMediumH"]: seconds_to_hours(try_get(stats, ['mediumStressDuration'])), # From g.get_stats()
            P["StressHighH"]: seconds_to_hours(try_get(stats, ['highStressDuration'])), # From g.get_stats()
            # Uncategorized Stress Removed
            P["BodyBatteryAvg"]: bb_avg, # Calculated from g.get_body_battery()
            P["BodyBatteryMax"]: bb_max, # From g.get_stats()
            P["BodyBatteryMin"]: bb_min, # From g.get_stats()
            P["Steps"]: try_get(stats, ['totalSteps'], ""),
            P["StepGoal"]: try_get(stats, ['dailyStepGoal'], ""), # Corrected key
            P["WalkDistanceMi"]: round((try_get(stats, ['totalDistanceMeters'], 0) or 0) / 1609.34, 2),
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
            P["IntensityMin"]: intensity_total, # Calculated
            P["IntensityMod"]: intensity_mod, # From g.get_stats()
            P["IntensityVig"]: intensity_vig, # From g.get_stats()
        }
        # --- END Populate props ---

        # Upsert by Date (first column) (Unchanged)
        row_values = [props.get(h, "") for h in SHEET_HEADERS]
        if d_iso in date_index:
            row_num = date_index[d_iso];
            rng = f"A{row_num}:{gspread.utils.rowcol_to_a1(row_num, len(SHEET_HEADERS))}"
            ws.update(range_name=rng, values=[row_values], value_input_option="RAW"); updates += 1
        else:
            ws.append_row(row_values, value_input_option="RAW")
            try: last = len(ws.col_values(1)); date_index[d_iso] = last
            except Exception: pass
            appends += 1
        
        # CHANGED: Removed first_day_processed flag
        time.sleep(0.1)

    print(f"Done. Upserted {updates} updates; {appends} inserts into Google Sheets '{worksheet_title}'.")

if __name__ == "__main__":
    main()