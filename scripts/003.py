################################################################################
# 003
# Plot data for a calibrator over a given date range.
################################################################################

import json, requests, itertools, csv, shutil, matplotlib.pyplot as plt, imageio
from datetime import datetime, timedelta
from pathlib import Path
from astropy.time import Time

################################################################################
# Paths and Constants
################################################################################

REFRESH_DATA = True    # If True, plots individual observations and saves data for GIF creation.
                        # Else, only creates combined plots and GIFs for each tile from existing data.

START_DATE = datetime(year=2025, month=12, day=1)    # Starts at 0:00 on this date
END_DATE   = datetime(year=2026, month=1, day=1)    # Ends at 0:00 on this date

CALIBRATOR = "HydA"

Y_LIM = (0, 2)

GIF_FPS = 1    # Frames per second for the GIFs

################

LOGS = Path("logging/003/")
OUTPUT = Path("output/003/")

SEARCH = OUTPUT / f"{CALIBRATOR}_{START_DATE.strftime("%Y-%m-%d")}_{END_DATE.strftime("%Y-%m-%d")}/"
DATES = SEARCH / "dates/"
PLOTS = SEARCH / "plots/"
GIFS = SEARCH / "gifs/"

DATA = Path("output/002/") / "calibrators"

LOGS.mkdir(parents=True, exist_ok=True)
OUTPUT.mkdir(parents=True, exist_ok=True)
PLOTS.mkdir(parents=True, exist_ok=True)
GIFS.mkdir(parents=True, exist_ok=True)
SEARCH.mkdir(parents=True, exist_ok=True)

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
# Plotting individual observations
################################################################################

if REFRESH_DATA:
    log(f"Creating tile gifs for calibrator {CALIBRATOR} from {START_DATE.strftime('%Y-%m-%d')} to {END_DATE.strftime('%Y-%m-%d')}.")

    fits = []
    observations = []
    for fit in sorted(DATA.glob(f"{CALIBRATOR}/fits/*.json"), reverse=True):
        fits.append(fit)
    for observation in sorted(DATA.glob(f"{CALIBRATOR}/observations/*.json"), reverse=True):
        observations.append(observation)

    # Go through each fit/observation pair
    for fit_file, obs_file in zip(fits, observations):
        with open(fit_file, "r") as f:
            fit_data = json.load(f)
            fit_meta = fit_data["metadata"]

        with open(obs_file, "r") as f:
            obs_data = json.load(f)
            obs_meta = obs_data["metadata"]
            obs_id = obs_meta["observation_number"]
            obs_time = Time(obs_data["starttime"], format="gps").to_datetime()
            channels = obs_data["rfstreams"]["0"]["frequencies"]
            tiles = obs_data["rfstreams"]["0"]["tileset"]

        # I really should do this earlier to avoid wasting time, but this is fine for now
        if obs_time < START_DATE or obs_time >= END_DATE:
            log(f"Observation {obs_id} is outside date range. Skipping...")
            continue

        log(f"Processing observation {obs_id}...")

        # Make a folder for the date of the observation if it does not exist.
        date_folder = DATES / f"{obs_time.strftime('%Y-%m-%d')}"
        date_folder.mkdir(parents=True, exist_ok=True)

        date_channel_folders = [
            date_folder / "Solar",
            date_folder / "Ch57-80",
            date_folder / "Ch81-104",
            date_folder / "Ch109-132",
            date_folder / "Ch133-156",
            date_folder / "Ch157-180",
        ]

        for folder in date_channel_folders:
            folder.mkdir(parents=True, exist_ok=True)

        # Plot each tile for this observation
        for tile_x, tile_y in zip(tiles["xlist"], tiles["ylist"]):
            if tile_x != tile_y: exit(1)

            log(f"Plotting tile {tile_x}...")
            try:
                x_gains = fit_data[str(tile_x)]["X"]["gains"]
                x_sigma = fit_data[str(tile_x)]["X"]["phase_sigma_resid"]
                x_chi2 = fit_data[str(tile_x)]["X"]["phase_chi2dof"]
                x_quality = fit_data[str(tile_x)]["X"]["phase_fit_quality"]

                y_gains = fit_data[str(tile_x)]["Y"]["gains"]
                y_sigma = fit_data[str(tile_x)]["Y"]["phase_sigma_resid"]
                y_chi2 = fit_data[str(tile_x)]["Y"]["phase_chi2dof"]
                y_quality = fit_data[str(tile_x)]["Y"]["phase_fit_quality"]
            except KeyError as e:
                log(f"Tile {e} does not exist in the fit data. Skipping...")
                continue

            plt.figure(figsize=(10, 6))
            plt.plot(channels, x_gains, label="X Gain", color="blue")
            plt.plot(channels, y_gains, label="Y Gain", color="red")
            plt.title(f"Tile: {tile_x}, Calibrator: {CALIBRATOR}, Date: {obs_time}")
            plt.xlabel("Channel")
            plt.ylabel("Gain")
            plt.legend()

            img_folder = PLOTS / f"{CALIBRATOR}_{obs_id}"
            img_folder.mkdir(parents=True, exist_ok=True)

            plot_path = img_folder / f"tile{tile_x}.png"
            plt.savefig(plot_path)
            plt.close()

            # Save this tile's x_gains and y_gains for later GIF creation
            match channels:
                case [58, 61, 65, 69, 73, 77, 81, 86, 91, 96, 101, 107, 113, 120, 127, 134, 142, 150, 158, 167, 177, 187, 210, 226]:
                    channels_name = "Solar"
                case [57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80]:
                    channels_name = "Ch57-80"
                case [81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102, 103, 104]:
                    channels_name = "Ch81-104"
                case [109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127, 128, 129, 130, 131, 132]:
                    channels_name = "Ch109-132"
                case [133, 134, 135, 136, 137, 138, 139, 140, 141, 142, 143, 144, 145, 146, 147, 148, 149, 150, 151, 152, 153, 154, 155, 156]:
                    channels_name = "Ch133-156"
                case [157, 158, 159, 160, 161, 162, 163, 164, 165, 166, 167, 168, 169, 170, 171, 172, 173, 174, 175, 176, 177, 178, 179, 180]:
                    channels_name = "Ch157-180"
                case _:
                    log(f"{channels} does not match any known band. Skipping saving gains for tile {tile_x}...")
                    continue

            with open(date_folder / f"{channels_name}" / f"{tile_x}.json", "w") as f:
                data = {
                    "x": {
                        "x_gains": x_gains,
                        "x_sigma": x_sigma,
                        "x_chi2": x_chi2,
                        "x_quality": x_quality
                    },
                    "y": {
                        "y_gains": y_gains,
                        "y_sigma": y_sigma,
                        "y_chi2": y_chi2,   
                        "y_quality": y_quality
                    }
                }
                json.dump(data, f, indent=4)

################################################################################
# Create GIF frames by combining band plots into one image for each tile on each date
################################################################################

for date_folder in sorted(DATES.glob("*")):
    # Build a combined plot of all bands for each tile on this date
    for data_solar in (date_folder / "Solar").glob("*.json"):
        log(f"Creating combined plot for tile {data_solar.name.removesuffix('.json')} on {date_folder.name}...")
        tile_number = data_solar.name.removesuffix(".json")

        data = {
            "Solar": data_solar,
            "Ch57-80": Path(date_folder / "Ch57-80" / f"{tile_number}.json"),
            "Ch81-104": Path(date_folder / "Ch81-104" / f"{tile_number}.json"),
            "Ch109-132": Path(date_folder / "Ch109-132" / f"{tile_number}.json"),
            "Ch133-156": Path(date_folder / "Ch133-156" / f"{tile_number}.json"),
            "Ch157-180": Path(date_folder / "Ch157-180" / f"{tile_number}.json"),
        }

        fig, axs = plt.subplots(2, 3, figsize=(15, 10))

        axs[0, 0].set_title("Solar")
        with open(data["Solar"], "r") as f:
            solar_data = json.load(f)
            channels = [58, 61, 65, 69, 73, 77, 81, 86, 91, 96, 101, 107, 113, 120, 127, 134, 142, 150, 158, 167, 177, 187, 210, 226]
            axs[0, 0].plot(channels, solar_data["x"]["x_gains"], label="X Gain", color="blue")
            axs[0, 0].plot(channels, solar_data["y"]["y_gains"], label="Y Gain", color="red")
            axs[0, 0].set_ylim(Y_LIM)
            axs[0, 0].legend()

        axs[0, 1].set_title("Ch57-80")
        with open(data["Ch57-80"], "r") as f:
            Ch57_80_data = json.load(f)
            channels = list(range(57, 81))
            axs[0, 1].plot(channels, Ch57_80_data["x"]["x_gains"], label="X Gain", color="blue")
            axs[0, 1].plot(channels, Ch57_80_data["y"]["y_gains"], label="Y Gain", color="red")
            axs[0, 1].set_ylim(Y_LIM)
            axs[0, 1].legend()

        axs[0, 2].set_title("Ch81-104")
        with open(data["Ch81-104"], "r") as f:
            Ch81_104_data = json.load(f)
            channels = list(range(81, 105))
            axs[0, 2].plot(channels, Ch81_104_data["x"]["x_gains"], label="X Gain", color="blue")
            axs[0, 2].plot(channels, Ch81_104_data["y"]["y_gains"], label="Y Gain", color="red")
            axs[0, 2].set_ylim(Y_LIM)
            axs[0, 2].legend()

        axs[1, 0].set_title("Ch109-132")
        with open(data["Ch109-132"], "r") as f:
            Ch109_132_data = json.load(f)
            channels = list(range(109, 133))
            axs[1, 0].plot(channels, Ch109_132_data["x"]["x_gains"], label="X Gain", color="blue")
            axs[1, 0].plot(channels, Ch109_132_data["y"]["y_gains"], label="Y Gain", color="red")
            axs[1, 0].set_ylim(Y_LIM)
            axs[1, 0].legend()

        axs[1, 1].set_title("Ch133-156")
        with open(data["Ch133-156"], "r") as f:
            Ch133_156_data = json.load(f)
            channels = list(range(133, 157))
            axs[1, 1].plot(channels, Ch133_156_data["x"]["x_gains"], label="X Gain", color="blue")
            axs[1, 1].plot(channels, Ch133_156_data["y"]["y_gains"], label="Y Gain", color="red")
            axs[1, 1].set_ylim(Y_LIM)
            axs[1, 1].legend()

        axs[1, 2].set_title("Ch157-180")
        with open(data["Ch157-180"], "r") as f:
            Ch157_180_data = json.load(f)
            channels = list(range(157, 181))
            axs[1, 2].plot(channels, Ch157_180_data["x"]["x_gains"], label="X Gain", color="blue")
            axs[1, 2].plot(channels, Ch157_180_data["y"]["y_gains"], label="Y Gain", color="red")
            axs[1, 2].set_ylim(Y_LIM)
            axs[1, 2].legend()

        fig.suptitle(f"Tile {tile_number} - {date_folder.name}")
        plot_path = PLOTS / f"{date_folder.name}"
        plot_path.mkdir(parents=True, exist_ok=True)
        fig_path = plot_path / f"tile{tile_number}.png"
        plt.savefig(fig_path)
        plt.close()

################################################################################
# Create GIFs by stitching together combined plots for each tile over the date range
################################################################################

date_folder = sorted(DATES.glob("*"))[0]
for data_solar in (date_folder / "Solar").glob("*.json"):
        tile_number = data_solar.name.removesuffix(".json")
        log(f"Creating GIF for tile {tile_number}...")

        img_files = []
        for date_folder in sorted(DATES.glob("*")):
            plot_path = PLOTS / f"{date_folder.name}" / f"tile{tile_number}.png"
            img_files.append(plot_path)

        gif_path = GIFS / f"tile{tile_number}.gif"
        frames = [imageio.v2.imread(p) for p in img_files]
        imageio.mimsave(gif_path, frames, duration=1 / GIF_FPS, loop=0)
