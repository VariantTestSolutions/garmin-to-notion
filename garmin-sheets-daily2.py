# CHANGED: This file has been updated to pull an expanded list of metrics and map them to the new header order.
# CHANGED: Re-implemented fetching logic to use garth directly for reliability, mirroring the working script.

import gspread
import os
import logging
from datetime import date, timedelta, datetime, timezone # CHANGED: Added imports
import garth
import sys
import base64
import tarfile
import io
import calendar # CHANGED: Added import
from garminconnect import (
    Garmin,
    GarminConnectConnectionError,
    GarminConnectTooManyRequestsError,
    GarminConnectAuthenticationError,
)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Configuration ---

# CHANGED: Removed unsupported fields (Sleep Respiration, Naps, Health Snapshot)
HEADER_ROW = [
    'Date', 'weekday', 'Weight (lb)', 'Training Readiness (0-100)', 'Training Status',
    'Resting HR', 'HRV', 'Respiration Rate Avg (BPM)', 
    'Sleep Score (0-100)', 'Sleep Total (h)', 'Sleep Light (h)', 'Sleep Deep (h)',
    'Sleep REM (h)', 'Sleep Awake (h)', 'Sleep Start (local)', 'Sleep End (local)',
    'Sleep Overall (q)', 'Sleep Duration (q)', 'Sleep Stress (q)', 'Sleep Awake Count (q)',
    'Sleep REM % (q)', 'Sleep Restlessness (q)', 'Sleep Light % (q)', 'Sleep Deep % (q)',
    'Stress Avg', 'Stress Max', 'Rest Stress Duration(h)',
    'Low Stress Duration (h)', 'Medium Stress Duration (h)', 'High Stress Duration (h)',
    'Uncategorized Stress Duration (h)', 'Body Battery Avg', 'Body Battery Max',
    'Body Battery Min', 'Steps', 'Step Goal', 'Walk Distance (mi)', 'Activities (#)',
    'Activity Distance (mi)', 'Activity Duration (min)', 'Activity Calories',
    'Activity Names', 'Activity Types', 'primary_sport', 'activity_types_unique',
    'Training Effect (list)', 'Aerobic Effect (list)', 'Anaerobic Effect (list)',
    'Intensity Minutes', 'Intensity Moderate (min)', 'Intensity Vigorous (min)',
]

# --- CHANGED: Updated to read variables from the new YAML file ---
# Environment variables
GARMIN_EMAIL = os.getenv('GARMIN_EMAIL')
GARMIN_PASSWORD = os.getenv('GARMIN_PASSWORD')
# This is now a FILE PATH provided by the YAML, not JSON content
GSHEET_SA_FILE = os.getenv('GOOGLE_SERVICE_ACCOUNT_FILE')
# This is the Spreadsheet ID, not the name
GSHEET_ID = os.getenv('GOOGLE_SHEETS_SPREADSHEET_ID')
# This is the Worksheet (tab) name
WORKSHEET_NAME = os.getenv('GOOGLE_SHEETS_WORKSHEET_TITLE2')
# This is the number of days to fetch
DAYS_TO_FETCH = int(os.getenv('WINDOW_DAYS', 14)) # Default to 14
# --- END OF CHANGE ---

# --- Utility Functions ---

def try_get(data, keys, default=""):
    """
    Safely traverses nested dictionaries.
    'data' is the dictionary.
    'keys' is a list of keys.
    'default' is the value to return on failure.
    """
    if data is None:
        return default
    temp = data
    try:
        for key in keys:
            if temp is None:
                return default
            temp = temp[key]
        return temp if temp is not None else default
    except (KeyError, TypeError, IndexError):
        return default

# CHANGED: Added helper to convert seconds to hours with formatting.
def seconds_to_hours(seconds):
    """Safely converts seconds (int) to hours (float, 2 decimal places)."""
    if isinstance(seconds, (int, float)):
        return round(seconds / 3600.0, 2)
    return ""

def format_list(data_list):
    """Converts a list into a comma-separated string."""
    if isinstance(data_list, list):
        return ", ".join(map(str, data_list))
    return ""

def miles_to_meters(miles):
    """Converts miles to meters."""
    return miles * 1609.34

def meters_to_miles(meters):
    """Converts meters to miles, rounding to 2 decimal places."""
    if isinstance(meters, (int, float)):
        return round(meters / 1609.34, 2)
    return ""

def seconds_to_minutes(seconds):
    """Converts seconds to minutes, rounding to 2 decimal places."""
    if isinstance(seconds, (int, float)):
        return round(seconds / 60.0, 2)
    return ""

def format_sleep_time(timestamp):
    """Formats sleep timestamp string."""
    if timestamp:
        # CHANGED: Convert to string first to handle integer timestamps
        return str(timestamp).replace('T', ' ').replace('.0', '')
    return ""

# --- Garmin API Fetch Functions ---

# CHANGED: This function is being kept
def get_sleep_data(api, date_str):
    """Fetches sleep data."""
    try:
        return api.get_sleep_data(date_str)
    except Exception as e:
        logging.warning(f"Could not fetch sleep data for {date_str}: {e}")
        return None

# CHANGED: This function is being kept
def get_activities_data(api, date_str):
    """Fetches activities for the day."""
    try:
        return api.get_activities_by_date(date_str, date_str)
    except Exception as e:
        logging.warning(f"Could not fetch activities for {date_str}: {e}")
        return []

# CHANGED: Added this function to get Steps and Step Goal
def get_daily_steps(api, date_str):
    """Fetches daily steps and goal."""
    try:
        return api.get_daily_steps(date_str)
    except Exception as e:
        logging.warning(f"Could not fetch daily steps for {date_str}: {e}")
        return None

# CHANGED: Added this function to get RHR
def get_user_summary(api, date_str):
    """Fetches user summary (RHR)."""
    try:
        return api.get_user_summary(date_str)
    except Exception as e:
        logging.warning(f"Could not fetch user summary for {date_str}: {e}")
        return None

# CHANGED: Removed get_daily_stats, get_stress_data, get_body_battery, get_hrv_data
# CHANGED: Removed get_training_readiness, get_training_status, get_daily_respiration
# CHANGED: Removed get_sleep_respiration, get_naps, get_health_snapshots (unsupported)

# --- Main Data Processing ---

# CHANGED: This function is rewritten to use direct garth calls for reliability.
def fetch_garmin_data(api, target_date):
    """
    Fetches all required Garmin data for a single day and maps it to the
    new header order.
    """
    date_str = target_date.isoformat()
    logging.info(f"Fetching data for {date_str}...")

    # --- Fetch all data points ---
    # Using 'api' object for these as they are reliable
    sleep = get_sleep_data(api, date_str)
    activities = get_activities_data(api, date_str)
    steps_data = get_daily_steps(api, date_str) # For steps and goal
    user_summary = get_user_summary(api, date_str) # For RHR
    
    # --- Using 'garth' directly for wellness stats (more reliable) ---
    daily_bb_stress = None
    try:
        daily_bb_stress = garth.DailyBodyBatteryStress.get(date_str)
    except Exception as e:
        logging.warning(f"Could not fetch BodyBattery/Stress for {date_str}: {e}")

    intensity = None
    try:
        intensity = garth.DailyIntensityMinutes.get(date_str)
    except Exception as e:
        logging.warning(f"Could not fetch IntensityMinutes for {date_str}: {e}")

    hrv_data = None
    try:
        hrv_data = garth.DailyHRV.get(date_str)
    except Exception as e:
        logging.warning(f"Could not fetch HRV for {date_str}: {e}")
        
    weight_data = None
    try:
        weight_data = garth.WeightData.get(date_str)
    except Exception as e:
        logging.warning(f"Could not fetch Weight for {date_str}: {e}")
        
    readiness = None
    try:
        readiness = garth.DailyTrainingReadiness.get(date_str)
    except Exception as e:
        logging.warning(f"Could not fetch Training Readiness for {date_str}: {e}")

    training_status = None
    try:
        training_status = garth.DailyTrainingStatus.get(date_str)
    except Exception as e:
        logging.warning(f"Could not fetch Training Status for {date_str}: {e}")
        
    daily_resp = None
    try:
        daily_resp = garth.DailyRespiration.get(date_str)
    except Exception as e:
        logging.warning(f"Could not fetch Respiration for {date_str}: {e}")

    # --- Process Fetched Data ---

    # Process Weight
    weight_lb = None
    if weight_data and hasattr(weight_data, 'weight'):
        grams = getattr(weight_data, "weight", None)
        if grams is not None:
            weight_lb = round((grams / 1000) * 2.20462, 2)

    # Process Body Battery
    bb_avg = None
    bb_min = None
    bb_max = None
    if daily_bb_stress and hasattr(daily_bb_stress, 'body_battery_readings'):
        levels = [getattr(x, "level", None) for x in getattr(daily_bb_stress, "body_battery_readings", [])]
        levels = [lv for lv in levels if isinstance(lv, (int, float))]
        if levels:
            bb_avg = round(sum(levels) / len(levels), 1)
            bb_min = min(levels)
        # Max value is on the main object
        if hasattr(daily_bb_stress, 'max_body_battery'):
             bb_max = getattr(daily_bb_stress, "max_body_battery", None)

    # Process Steps
    steps = try_get(steps_data, [0, 'totalSteps'], "")
    step_goal = try_get(steps_data, [0, 'stepGoal'], "")
    walk_dist_m = try_get(steps_data, [0, 'totalDistance'], 0)
    
    # Process Activity Data
    activity_count = len(activities)
    activity_dist_m = sum(try_get(act, ['distance'], 0) for act in activities)
    activity_dur_s = sum(try_get(act, ['duration'], 0) for act in activities)
    activity_cals = sum(try_get(act, ['calories'], 0) for act in activities)
    activity_names = format_list([try_get(act, ['activityName'], "N/A") for act in activities])
    activity_types = format_list([try_get(act, ['activityType', 'typeKey'], "N/A") for act in activities])
    primary_sport = try_get(activities, [0, 'activityType', 'typeKey'], "") if activity_count > 0 else ""
    unique_types = format_list(list(set(try_get(act, ['activityType', 'typeKey'], "N/A") for act in activities)))
    training_effect = format_list([try_get(act, ['trainingEffect'], "N/A") for act in activities])
    aerobic_effect = format_list([try_get(act, ['aerobicTrainingEffect'], "N/A") for act in activities])
    anaerobic_effect = format_list([try_get(act, ['anaerobicTrainingEffect'], "N/A") for act in activities])

    # --- Build Row Data in Order ---
    # This list maps directly to the HEADER_ROW
    row_data = [
        date_str, # Date
        target_date.strftime('%A'), # weekday
        weight_lb, # Weight (lb)
        getattr(readiness, "training_readiness", ""), # Training Readiness (0-100)
        getattr(training_status, "training_status", ""), # Training Status
        try_get(user_summary, ['restingHeartRate'], ""), # Resting HR
        getattr(hrv_data, "last_night_avg", ""), # HRV
        getattr(daily_resp, "avg_overall_breaths_per_min", ""), # Respiration Rate Avg (BPM)
        # REMOVED: Sleep Respiration
        try_get(sleep, ['sleepScores', 'overallScore'], ""), # Sleep Score (0-100)
        seconds_to_hours(try_get(sleep, ['dailySleepDTO', 'sleepTimeSeconds'], 0)), # Sleep Total (h)
        seconds_to_hours(try_get(sleep, ['dailySleepDTO', 'lightSleepSeconds'], 0)), # Sleep Light (h)
        seconds_to_hours(try_get(sleep, ['dailySleepDTO', 'deepSleepSeconds'], 0)), # Sleep Deep (h)
        seconds_to_hours(try_get(sleep, ['dailySleepDTO', 'remSleepSeconds'], 0)), # Sleep REM (h)
        seconds_to_hours(try_get(sleep, ['dailySleepDTO', 'awakeSleepSeconds'], 0)), # Sleep Awake (h)
        format_sleep_time(try_get(sleep, ['dailySleepDTO', 'sleepStartTimestampLocal'], "")), # Sleep Start (local)
        format_sleep_time(try_get(sleep, ['dailySleepDTO', 'sleepEndTimestampLocal'], "")), # Sleep End (local)
        try_get(sleep, ['sleepScores', 'overall'], ""), # Sleep Overall (q)
        try_get(sleep, ['sleepScores', 'duration'], ""), # Sleep Duration (q)
        try_get(sleep, ['sleepScores', 'stress'], ""), # Sleep Stress (q)
        try_get(sleep, ['sleepScores', 'awakeCount'], ""), # Sleep Awake Count (q)
        try_get(sleep, ['sleepScores', 'remPercentage'], ""), # Sleep REM % (q)
        try_get(sleep, ['sleepScores', 'restlessness'], ""), # Sleep Restlessness (q)
        try_get(sleep, ['sleepScores', 'lightPercentage'], ""), # Sleep Light % (q)
        try_get(sleep, ['sleepScores', 'deepPercentage'], ""), # Sleep Deep % (q)
        # REMOVED: Nap Duration
        getattr(daily_bb_stress, "avg_stress_level", ""), # Stress Avg
        getattr(daily_bb_stress, "max_stress_level", ""), # Stress Max
        seconds_to_hours(getattr(daily_bb_stress, "rest_stress_duration_seconds", 0)), # Rest Stress Duration(h)
        seconds_to_hours(getattr(daily_bb_stress, "low_stress_duration_seconds", 0)), # Low Stress Duration (h)
        seconds_to_hours(getattr(daily_bb_stress, "medium_stress_duration_seconds", 0)), # Medium Stress Duration (h)
        seconds_to_hours(getattr(daily_bb_stress, "high_stress_duration_seconds", 0)), # High Stress Duration (h)
        seconds_to_hours(getattr(daily_bb_stress, "uncategorized_stress_duration_seconds", 0)), # Uncategorized Stress Duration (h)
        bb_avg, # Body Battery Avg
        bb_max, # Body Battery Max
        bb_min, # Body Battery Min
        steps, # Steps
        step_goal, # Step Goal
        meters_to_miles(walk_dist_m), # Walk Distance (mi)
        activity_count, # Activities (#)
        meters_to_miles(activity_dist_m), # Activity Distance (mi)
        seconds_to_minutes(activity_dur_s), # Activity Duration (min)
        activity_cals, # Activity Calories
        activity_names, # Activity Names
        activity_types, # Activity Types
        primary_sport, # primary_sport
        unique_types, # activity_types_unique
        training_effect, # Training Effect (list)
        aerobic_effect, # Aerobic Effect (list)
        anaerobic_effect, # Anaerobic Effect (list)
        getattr(intensity, "total_value", ""), # Intensity Minutes
        getattr(intensity, "moderate_value", ""), # Intensity Moderate (min)
        getattr(intensity, "vigorous_value", ""), # Intensity Vigorous (min)
        # REMOVED: Health Snapshot
    ]

    return row_data


def get_gspread_client():
    """Initializes and returns the gspread client."""
    try:
        # CHANGED: Use service_account(filename=...) instead of ...from_dict(eval(...))
        gc = gspread.service_account(filename=GSHEET_SA_FILE)
        return gc
    except Exception as e:
        logging.error(f"Failed to initialize gspread client: {e}")
        raise

# -----------------------------
# Garmin login (CI-safe)
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

    # CHANGED: Initialize garth first to ensure it's configured
    try:
        garth.resume(token_store)
        print(f"[garmin] Resumed tokens from {token_store} for garth")g = Garmin(garmin_email, garmin_password) # CHANGED: Removed token_store argument
    except Exception as resume_err:
        print(f"[garmin] No usable tokens for garth, will login: {resume_err}")
        if mfa_code:
            print("[garmin] Performing non-interactive MFA login for garth")
            client_state = garth.login(garmin_email, garmin_password, return_on_mfa=True)
            if client_state:
                garth.resume_login(client_state, mfa_code)
        else:
            garth.login(garmin_email, garmin_password)
        _ensure_dir(token_store)
        garth.save(token_store)
        print(f"[garmin] Saved new garth tokens to {token_store}")
        
    # CHANGED: Initialize Garmin object *after* garth is handled
        g = Garmin(garmin_email, garmin_password) # CHANGED: Removed token_store argument

    try:
        # CHANGED: Use the existing token store for the Garmin object login
        g.login(tokenstore=token_store) # CHANGED: Added tokenstore argument
        print(f"[garmin] Garmin object login successful using tokens from {token_store}")
        return g, token_store
    except Exception as e:
        print(f"[garmin] Garmin object login error: {e}")
        # Fallback login attempt for the Garmin object
        try:
            g.login()
            print(f"[garmin] Garmin object login successful on fallback.")
            return g, token_store
        except Exception as e2:
            print(f"[garmin] Full login error: {e2}")
            sys.exit(1)

def main():
    """
    Main function to fetch data from Garmin and update Google Sheet.
    """
    # CHANGED: Updated the check to use the new variable names
    if not all([GSHEET_SA_FILE, GSHEET_ID, WORKSHEET_NAME]):
        logging.error("Missing one or more required GOOGLE environment variables (GOOGLE_SERVICE_ACCOUNT_FILE, GOOGLE_SHEETS_SPREADSHEET_ID, GOOGLE_SHEETS_WORKSHEET_TITLE).")
        return

    logging.info("Starting Garmin data sync...")

    # CHANGED: Use the robust login function
    try:
        api, token_store_path = login_to_garmin()
        logging.info(f"Garmin login successful, tokens at {token_store_path}")
    except Exception as e:
        logging.error(f"Garmin login failed: {e}")
        return

    try:
        gc = get_gspread_client()
        # CHANGED: Open the sheet by KEY (ID) instead of by NAME
        sh = gc.open_by_key(GSHEET_ID)
        # CHANGED: Open the worksheet by the variable name
        worksheet = sh.worksheet(WORKSHEET_NAME)
        logging.info(f"Opened Google Sheet (ID: {GSHEET_ID}) and worksheet '{WORKSHEET_NAME}'.")
    except Exception as e:
        logging.error(f"Failed to open Google Sheet: {e}")
        return

    today = date.today()
    all_rows_data = [HEADER_ROW]  # Start with the header

    # CHANGED: The 'range' logic now correctly uses the DAYS_TO_FETCH variable
    for i in range(DAYS_TO_FETCH - 1, -1, -1):  # Fetch last 14 days, ending with today
        target_date = today - timedelta(days=i)
        try:
            daily_data = fetch_garmin_data(api, target_date)
            all_rows_data.append(daily_data)
        except Exception as e:
            logging.error(f"Failed to fetch data for {target_date}: {e}", exc_info=True) # CHANGED: Added exc_info for better debugging
            # Append a row with just the date and weekday to show a gap
            all_rows_data.append([target_date.isoformat(), target_date.strftime('%A')] + [''] * (len(HEADER_ROW) - 2)) 

    try:
        worksheet.clear()
        # CHANGED: Use named arguments to fix DeprecationWarning
        worksheet.update(range_name='A1', values=all_rows_data, value_input_option='USER_ENTERED')
        logging.info(f"Successfully updated Google Sheet with {len(all_rows_data) - 1} days of data.")
    except Exception as e:
        logging.error(f"Failed to update Google Sheet: {e}")

    logging.info("Garmin data sync finished.")


if __name__ == "__main__":
    main()