################################################################################
# 002
# Fetch observation .json files for each valid calibration and sort them by calibrator
################################################################################

import json, requests, itertools, csv, shutil
from datetime import datetime
from pathlib import Path

################################################################################
# Paths and Constants
################################################################################

THRESHOLD = 1000  # Minimum number of observations for a calibrator to be added to the list.

################

LOGS = Path("logging/002/")
OUTPUT = Path("output/002/")
FITS = OUTPUT / "fits/"
OBSERVATIONS = OUTPUT / "observations/"
CALIBRATORS = OUTPUT / "calibrators/"

LOGS.mkdir(parents=True, exist_ok=True)
OUTPUT.mkdir(parents=True, exist_ok=True)
FITS.mkdir(parents=True, exist_ok=True)
OBSERVATIONS.mkdir(parents=True, exist_ok=True)
CALIBRATORS.mkdir(parents=True, exist_ok=True)

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
# Copy over the calibration solutions from 001 where we have valid metadata
################################################################################

SOLUTIONS = Path("output/001/") / "calibrations"

for calibration in SOLUTIONS.glob("*.json"):
    obs_id = calibration.name.removeprefix("fit_").removesuffix(".json")

    # Skip obs_ids which have been previously processed.
    if Path(FITS / f"fit_{obs_id}.json").exists():
        log(f"Observation metadata for obs_id {obs_id} already exists. Skipping...")
        continue

    # I would make this more robust if I had more time.
    try:
        payload = {"obs_id": obs_id}
        log(f"Got observation metadata for obs_id {obs_id}.")
        r = requests.get('https://ws.mwatelescope.org/metadata/obs', params=payload)
        metadata = r.json()
    except:
        log(f"Failed to fetch observation metadata for obs_id {obs_id}. Skipping...")
        continue

    # Save the relevant files.
    with open(OBSERVATIONS / f"obs_{obs_id}.json", "w") as f:
        json.dump(metadata, f, indent=4)

    shutil.copy2(calibration, FITS / f"fit_{obs_id}.json")

################################################################################
# Get a list of all calibrators and create folders for each one
################################################################################
calibrators = {}

for observation in OBSERVATIONS.glob("*.json"):
    
    with observation.open("r", encoding="utf-8") as f:
        data = json.load(f)
    metadata = data["metadata"]
    calibrator = metadata["calibrators"]
    
    if calibrator in calibrators.keys():
        calibrators[calibrator] += 1
    else:
        calibrators[calibrator] = 1

for calibrator in calibrators:
    if calibrators[calibrator] > THRESHOLD:
        log(f"Creating folder for calibrator: {calibrator} [{calibrators[calibrator]}]")
        (CALIBRATORS / calibrator / "observations").mkdir(parents=True, exist_ok=True)
        (CALIBRATORS / calibrator / "fits").mkdir(parents=True, exist_ok=True)

################################################################################
# Populate calibrator folders with observation and fit files
################################################################################
for observation in OBSERVATIONS.glob("*.json"):
    
    with observation.open("r", encoding="utf-8") as f:
        data = json.load(f)
    metadata = data["metadata"]
    calibrator = metadata["calibrators"]
    obs_id = metadata["observation_number"]

    if Path(CALIBRATORS / calibrator).exists():
        if Path(CALIBRATORS / calibrator / f"observations/obs_{obs_id}.json").exists():
            log(f"Observation and fit files for {obs_id} already exist in {calibrator} folder. Skipping...")
            continue
        shutil.copy2(observation, CALIBRATORS / calibrator / f"observations/obs_{obs_id}.json")
        shutil.copy2(FITS / f"fit_{obs_id}.json", CALIBRATORS / calibrator / f"fits/fit_{obs_id}.json")
        log(f"Copied observation and fit files for {obs_id} to {calibrator} folder.")