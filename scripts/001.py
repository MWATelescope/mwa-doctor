################################################################################
# 001
# Fetch all past and future observations, combine into .csv files, filter to valid calibration solutions and fetch their .jsons
################################################################################

import json, requests, itertools, csv, time
from datetime import datetime
from pathlib import Path

################################################################################
# Paths and Constants
################################################################################

# Steps to run #
REFRESH_PAST = False             # Fetch the latest pages of past observations
REFRESH_FUTURE = False           # Fetch ALL pages of future observations

REFRESH_CSV = False              # Update future.csv and past.csv based on the pages found above
REFRESH_CALIBS = False           # Populate calibrations.csv with the calibration observations in past.csv

FETCH_FITS = True             # Attempt to fetch the .json for each observation id in calibrations.csv
FORCE_RETRY_FITS = False      # By default, fits that are known to not exist (from past requests) are skipped automatically. TRUE to override this behaviour.

################

LOGS = Path("logging/001/")
OUTPUT = Path("output/001/")
ALL_PAST_OBSERVATIONS = OUTPUT / "past/"
ALL_FUTURE_OBSERVATIONS = OUTPUT / "future/"
ALL_CALIBRATIONS = OUTPUT / "calibrations/"

LOGS.mkdir(parents=True, exist_ok=True)
OUTPUT.mkdir(parents=True, exist_ok=True)
ALL_PAST_OBSERVATIONS.mkdir(parents=True, exist_ok=True)
ALL_FUTURE_OBSERVATIONS.mkdir(parents=True, exist_ok=True)
ALL_CALIBRATIONS.mkdir(parents=True, exist_ok=True)

PAST_COMBINED = OUTPUT / "past.csv"
FUTURE_COMBINED = OUTPUT / "future.csv"
CALIBRATIONS = OUTPUT / "calibrations.csv"
MISSING = OUTPUT / "missing.csv"

# Create log file
LOG_START = datetime.now()
LOG_FILE = LOGS / f"log_{LOG_START.strftime("%Y-%m-%dT%H-%M-%S")}.txt"
with open(LOG_FILE, "a") as f:
    f.write(f"({LOG_START.strftime("%Y-%m-%d %H-%M-%S")}) Starting...\n")
    print(f"({LOG_START.strftime("%Y-%m-%d %H-%M-%S")}) Starting...")

################################################################################
# Helper functions
################################################################################

def log(message):
    time_tag = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    with open(LOG_FILE, "a") as f:
        f.write(f"({time_tag}) {message}\n")
        print(f"({time_tag}) {message}")

################################################################################
# Web service functions (this is only used to fetch all past and future observations)
################################################################################

def update(start_page = 1, future = 0):
    # this sometimes gives a random error... maybe from reading too fast? just restart from the last successful page and it should work
    if future: output_folder = ALL_FUTURE_OBSERVATIONS
    else: output_folder = ALL_PAST_OBSERVATIONS

    for i in itertools.count(start_page, 1):
        log(f"Fetching page {i}...")
        payload = {"page": i, "future": future}
        r = requests.get('https://ws.mwatelescope.org/metadata/find', params=payload)
        observations = r.json()

        # End on empty response
        if not observations:
            log(f"Failed to fetch page {i}!")
            break

        output_path = Path(output_folder / f"results{i:04d}.json")
        with open(output_path, 'w') as f:
            json.dump(observations, f, indent=4)
        
################################################################################
# Fetch required .json files for all past and future observations
################################################################################

# For past observations, get the latest page of results and fetch from that point
if REFRESH_PAST:
    latest_past = sorted(set(ALL_PAST_OBSERVATIONS.glob("*.json")))
    try: page = int(str(latest_past[-1]).split("past/results")[1].split(".json")[0])   # idk
    except: page = 1

    log(f"Updating past observations (from page {page})...")
    update(page, 0)
    log(f"Done!")

# For future observations, fetch everything as they change from page 1
if REFRESH_FUTURE:
    log(f"Updating future observations...")
    update(1, 1)
    log(f"Done!")

################################################################################
# Combine each set of observations into a single past/future .csv file
################################################################################

if REFRESH_PAST or REFRESH_CSV:
    log(f"Combining past observations into past.csv...")
    with PAST_COMBINED.open("w", newline="", encoding="utf-8") as csv_output:
        writer = csv.writer(csv_output)
        for page in sorted(ALL_PAST_OBSERVATIONS.glob("*.json"), reverse=True):
            with page.open("r", encoding="utf-8") as f:
                data = json.load(f)
                for row in data: writer.writerow(row)
        log(f"Done!")

if REFRESH_FUTURE or REFRESH_CSV:
    log(f"Combining future observations into future.csv...")
    with FUTURE_COMBINED.open("w", newline="", encoding="utf-8") as csv_output:
        writer = csv.writer(csv_output)
        for page in sorted(ALL_FUTURE_OBSERVATIONS.glob("*.json"), reverse=True):
            with page.open("r", encoding="utf-8") as f:
                data = json.load(f)
                for row in data: writer.writerow(row)
        log(f"Done!")

################################################################################
# Filter past.csv to calibrations.csv, which contains only observations with project code D0006
################################################################################

if REFRESH_CALIBS:
    log(f"Finding calibration observations in past.csv...")
    with PAST_COMBINED.open("r", newline="", encoding="utf-8") as observations:
        with CALIBRATIONS.open("w", newline="", encoding="utf-8") as calibrations:
            reader = csv.reader(observations)
            writer = csv.writer(calibrations)

            for row in reader:
                isCalibration = (row[3].strip() == "D0006")
                if isCalibration: writer.writerow(row)
    log(f"Done!")

################################################################################
# Fetch .json for all calibration fits that exist among each calibration observation
################################################################################

if FETCH_FITS:
    skipped_exist = 0       # we already have these 
    skipped_nonexist = 0    # these do not exist

    log(f"Checking existing calibrations...")
    existing_calibs = set()
    for calib in ALL_CALIBRATIONS.glob("*.json"):
        calib_number = calib.name.split("fit_")[1].split(".")[0]
        existing_calibs.add(calib_number)

    log(f"Checking known missing fits...")
    missing_fits = set()
    try:
        with open(MISSING, 'r') as f:
            for line in f:
                missing_fit_id = line.strip()
                missing_fits.add(missing_fit_id)
    except:
        log("Missing fits file does not exist. Skipping...")
        pass

    log(f"Fetching data from all calibrations...")
    with CALIBRATIONS.open("r", newline="", encoding="utf-8") as calibrations:
        reader = csv.reader(calibrations)
        for row in reader:
            id = row[0].strip()
            
            # Do not fetch fits we already have
            if id in existing_calibs:
                # log(f"Skipping {id} as record already exists...")
                skipped_exist += 1
                continue

            # Do not fetch fits we know do not exist
            if (id in missing_fits) and (not FORCE_RETRY_FITS):
                # log(f"Skipping {id} as record is known to not exist...")
                skipped_nonexist += 1
                continue

            # Fetch the fit associated with the given obs_id
            payload = {"obs_id": id}
            r = requests.get("https://ws.mwatelescope.org/calib/get_cal_json", params=payload)

            match r.status_code:
                case 400: log(f"Faulty request for {id}! Skipping...")
                case 500: log(f"Internal server error for {id}! Skipping...")
                case 404:
                    with open(MISSING, 'a') as f: f.write(f"{id}\n")
                    log(f"No fit found for {id}! Skipping...")
                case _:
                    fit = r.json()
                    log(f"Fetched fit for {id}...")
                    with open(Path(ALL_CALIBRATIONS / f"fit_{id}.json"), 'w') as f:
                        json.dump(fit, f, indent=4)

    log(f"Skipped {skipped_exist} fits as records already exist...")
    log(f"Skipped {skipped_nonexist} fits as records are known to not exist...")
    log(f"Done!")