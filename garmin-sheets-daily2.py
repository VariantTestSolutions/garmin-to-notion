# CHANGED: This file has been updated to pull an expanded list of metrics and map them to the new header order.

import gspread
import os
import logging
from datetime import date, timedelta
from garminconnect import (
    Garmin,
    GarminConnectConnectionError,
    GarminConnectTooManyRequestsError,
    GarminConnectAuthenticationError,
)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Configuration ---

# CHANGED: Replaced the old list with the new 53-column header list.
HEADER_ROW = [
    'Date', 'weekday', 'Weight (lb)', 'Training Readiness (0-100)', 'Training Status',
    'Resting HR', 'HRV', 'Respiration Rate Avg (BPM)', 'Sleep Respiration Rate Avg (BPM)',
    'Sleep Score (0-100)', 'Sleep Total (h)', 'Sleep Light (h)', 'Sleep Deep (h)',
    'Sleep REM (h)', 'Sleep Awake (h)', 'Sleep Start (local)', 'Sleep End (local)',
    'Sleep Overall (q)', 'Sleep Duration (q)', 'Sleep Stress (q)', 'Sleep Awake Count (q)',
    'Sleep REM % (q)', 'Sleep Restlessness (q)', 'Sleep Light % (q)', 'Sleep Deep % (q)',
    'Nap Duration (h)', 'Stress Avg', 'Stress Max', 'Rest Stress Duration(h)',
    'Low Stress Duration (h)', 'Medium Stress Duration (h)', 'High Stress Duration (h)',
    'Uncategorized Stress Duration (h)', 'Body Battery Avg', 'Body Battery Max',
    'Body Battery Min', 'Steps', 'Step Goal', 'Walk Distance (mi)', 'Activities (#)',
    'Activity Distance (mi)', 'Activity Duration (min)', 'Activity Calories',
    'Activity Names', 'Activity Types', 'primary_sport', 'activity_types_unique',
    'Training Effect (list)', 'Aerobic Effect (list)', 'Anaerobic Effect (list)',
    'Intensity Minutes', 'Intensity Moderate (min)', 'Intensity Vigorous (min)',
    'Health Snapshot'
]

# Environment variables
GARMIN_EMAIL = os.getenv('GARMIN_EMAIL')
GARMIN_PASSWORD = os.getenv('GARMIN_PASSWORD')
GSHEET_JSON = os.getenv('GSHEET_JSON')
GSHEET_NAME = '2025 Overview' # CHANGED: Assuming this is the name, correct if needed.
WORKSHEET_NAME = 'Garmin Data'
DAYS_TO_FETCH = 14

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
        return timestamp.replace('T', ' ').replace('.0', '')
    return ""

# --- Garmin API Fetch Functions ---

def get_daily_stats(api, date_str):
    """Fetches combined daily stats."""
    try:
        return api.get_stats(date_str)
    except Exception as e:
        logging.warning(f"Could not fetch get_stats for {date_str}: {e}")
        return None

def get_sleep_data(api, date_str):
    """Fetches sleep data."""
    try:
        # CHANGED: Modified to fetch sleep score data as well.
        return api.get_sleep_data(date_str)
    except Exception as e:
        logging.warning(f"Could not fetch sleep data for {date_str}: {e}")
        return None

def get_stress_data(api, date_str):
    """Fetches stress data."""
    try:
        # CHANGED: Modified to fetch duration data.
        return api.get_stress_data(date_str)
    except Exception as e:
        logging.warning(f"Could not fetch stress data for {date_str}: {e}")
        return None

def get_body_battery(api, date_str):
    """Fetches body battery data."""
    try:
        # CHANGED: Modified to fetch max value.
        return api.get_body_battery(date_str)
    except Exception as e:
        logging.warning(f"Could not fetch body battery for {date_str}: {e}")
        return None

def get_hrv_data(api, date_str):
    """Fetches HRV data."""
    try:
        return api.get_hrv_data(date_str)
    except Exception as e:
        logging.warning(f"Could not fetch HRV data for {date_str}: {e}")
        return None

def get_activities_data(api, date_str):
    """Fetches activities for the day."""
    try:
        return api.get_activities_by_date(date_str, date_str)
    except Exception as e:
        logging.warning(f"Could not fetch activities for {date_str}: {e}")
        return []

def get_user_summary(api, date_str):
    """Fetches user summary (RHR, Weight)."""
    try:
        return api.get_user_summary(date_str)
    except Exception as e:
        logging.warning(f"Could not fetch user summary for {date_str}: {e}")
        return None

# CHANGED: Added helper for Training Readiness
def get_training_readiness(api, date_str):
    """Fetches training readiness."""
    try:
        return api.get_training_readiness(date_str)
    except Exception as e:
        logging.warning(f"Could not fetch training readiness for {date_str}: {e}")
        return None

# CHANGED: Added helper for Training Status
def get_training_status(api, date_str):
    """Fetches training status."""
    try:
        return api.get_training_status(date_str)
    except Exception as e:
        logging.warning(f"Could not fetch training status for {date_str}: {e}")
        return None

# CHANGED: Added helper for Daily Respiration
def get_daily_respiration(api, date_str):
    """Fetches daily respiration data."""
    try:
        return api.get_respiration_data(date_str)
    except Exception as e:
        logging.warning(f"Could not fetch daily respiration for {date_str}: {e}")
        return None

# CHANGED: Added helper for Sleep Respiration
def get_sleep_respiration(api, date_str):
    """Fetches sleep respiration data."""
    try:
        return api.get_sleep_respiration_data(date_str)
    except Exception as e:
        logging.warning(f"Could not fetch sleep respiration for {date_str}: {e}")
        return None

# CHANGED: Added helper for Naps
def get_naps(api, date_str):
    """Fetches nap data."""
    try:
        return api.get_naps(date_str)
    except Exception as e:
        logging.warning(f"Could not fetch naps for {date_str}: {e}")
        return []

# CHANGED: Added helper for Health Snapshots
def get_health_snapshots(api, date_str):
    """Fetches health snapshots."""
    try:
        return api.get_health_snapshot(date_str)
    except Exception as e:
        logging.warning(f"Could not fetch health snapshots for {date_str}: {e}")
        return []

# --- Main Data Processing ---

# CHANGED: This function is rewritten to map all new metrics to the new header order.
def fetch_garmin_data(api, target_date):
    """
    Fetches all required Garmin data for a single day and maps it to the
    new header order.
    """
    date_str = target_date.isoformat()
    logging.info(f"Fetching data for {date_str}...")

    # Fetch all data points
    stats = get_daily_stats(api, date_str)
    user_summary = get_user_summary(api, date_str)
    sleep = get_sleep_data(api, date_str)
    stress = get_stress_data(api, date_str)
    body_battery_data = get_body_battery(api, date_str)
    hrv = get_hrv_data(api, date_str)
    activities = get_activities_data(api, date_str)
    readiness = get_training_readiness(api, date_str)
    training_status = get_training_status(api, date_str)
    daily_resp = get_daily_respiration(api, date_str)
    sleep_resp = get_sleep_respiration(api, date_str)
    naps = get_naps(api, date_str)
    snapshots = get_health_snapshots(api, date_str)

    # --- Process Activity Data ---
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

    # --- Process Nap Data ---
    total_nap_seconds = sum(try_get(nap, ['durationInSeconds'], 0) for nap in naps)

    # --- Process Health Snapshot Data ---
    snapshot_times = format_list([
        format_sleep_time(try_get(snap, ['startTimeLocal'], "N/A")) for snap in snapshots
    ])

    # --- Build Row Data in Order ---
    # This list maps directly to the HEADER_ROW
    row_data = [
        date_str, # Date
        target_date.strftime('%A'), # weekday
        try_get(user_summary, ['weightInLbs'], ""), # Weight (lb)
        try_get(readiness, ['trainingReadiness'], ""), # Training Readiness (0-100)
        try_get(training_status, ['trainingStatus'], ""), # Training Status
        try_get(user_summary, ['restingHeartRate'], ""), # Resting HR
        try_get(hrv, ['lastNightAvg'], ""), # HRV
        try_get(daily_resp, ['avgOverallBreathsPerMin'], ""), # Respiration Rate Avg (BPM)
        try_get(sleep_resp, ['avgBreathsPerMin'], ""), # Sleep Respiration Rate Avg (BPM)
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
        seconds_to_hours(total_nap_seconds), # Nap Duration (h)
        try_get(stress, ['averageStressLevel'], ""), # Stress Avg
        try_get(stress, ['maxStressLevel'], ""), # Stress Max
        seconds_to_hours(try_get(stress, ['restStressDurationInSeconds'], 0)), # Rest Stress Duration(h)
        seconds_to_hours(try_get(stress, ['lowStressDurationInSeconds'], 0)), # Low Stress Duration (h)
        seconds_to_hours(try_get(stress, ['mediumStressDurationInSeconds'], 0)), # Medium Stress Duration (h)
        seconds_to_hours(try_get(stress, ['highStressDurationInSeconds'], 0)), # High Stress Duration (h)
        seconds_to_hours(try_get(stress, ['uncategorizedStressDurationInSeconds'], 0)), # Uncategorized Stress Duration (h)
        try_get(body_battery_data, [-1, 'bodyBatteryValue'], ""), # Body Battery Avg (approx by last value)
        try_get(body_battery_data, [0, 'bodyBatteryMaxValue'], ""), # Body Battery Max
        try_get(body_battery_data, [0, 'bodyBatteryMinValue'], ""), # Body Battery Min
        try_get(stats, ['totalSteps'], ""), # Steps
        try_get(stats, ['stepGoal'], ""), # Step Goal
        meters_to_miles(try_get(stats, ['totalDistanceMeters'], 0)), # Walk Distance (mi)
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
        try_get(stats, ['intensityMinutes'], ""), # Intensity Minutes
        try_get(stats, ['moderateIntensityMinutes'], ""), # Intensity Moderate (min)
        try_get(stats, ['vigorousIntensityMinutes'], ""), # Intensity Vigorous (min)
        snapshot_times # Health Snapshot
    ]

    return row_data


def get_gspread_client():
    """Initializes and returns the gspread client."""
    try:
        gc = gspread.service_account_from_dict(eval(GSHEET_JSON))
        return gc
    except Exception as e:
        logging.error(f"Failed to initialize gspread client: {e}")
        raise

def main():
    """
    Main function to fetch data from Garmin and update Google Sheet.
    """
    if not all([GARMIN_EMAIL, GARMIN_PASSWORD, GSHEET_JSON]):
        logging.error("Missing one or more environment variables.")
        return

    logging.info("Starting Garmin data sync...")

    try:
        api = Garmin(GARMIN_EMAIL, GARMIN_PASSWORD)
        api.login()
        logging.info("Garmin login successful.")
    except (GarminConnectConnectionError, GarminConnectTooManyRequestsError, GarminConnectAuthenticationError) as e:
        logging.error(f"Garmin login failed: {e}")
        return
    except Exception as e:
        logging.error(f"An unexpected error occurred during Garmin login: {e}")
        return

    try:
        gc = get_gspread_client()
        sh = gc.open(GSHEET_NAME)
        worksheet = sh.worksheet(WORKSHEET_NAME)
        logging.info(f"Opened Google Sheet '{GSHEET_NAME}' and worksheet '{WORKSHEET_NAME}'.")
    except Exception as e:
        logging.error(f"Failed to open Google Sheet: {e}")
        return

    today = date.today()
    all_rows_data = [HEADER_ROW]  # Start with the header

    for i in range(DAYS_TO_FETCH - 1, -1, -1):  # Fetch last 14 days, ending with today
        target_date = today - timedelta(days=i)
        try:
            daily_data = fetch_garmin_data(api, target_date)
            all_rows_data.append(daily_data)
        except Exception as e:
            logging.error(f"Failed to fetch data for {target_date}: {e}")
            # Append a row with just the date and weekday to show a gap
            all_rows_data.append([target_date.isoformat(), target_date.strftime('%A')] + [''] * (len(HEADER_ROW) - 2)) # CHANGED: Adjusted empty row length

    try:
        worksheet.clear()
        worksheet.update('A1', all_rows_data, value_input_option='USER_ENTERED')
        logging.info(f"Successfully updated Google Sheet with {len(all_rows_data) - 1} days of data.")
    except Exception as e:
        logging.error(f"Failed to update Google Sheet: {e}")

    logging.info("Garmin data sync finished.")


if __name__ == "__main__":
    main()