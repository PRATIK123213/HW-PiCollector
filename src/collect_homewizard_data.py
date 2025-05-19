"""
In this module, we define a script that will be used to collect data from the HomeWizard sensors
and save them in CSV files on the Raspberry Pi.
"""
__title__: str = "collect_homewizard_data"
__version__: str = "1.0.0"
__author__: str = "Brice Petit"
__license__: str = "MIT"

# ----------------------------------------------------------------------------------------------- #
# ------------------------------------------- IMPORTS ------------------------------------------- #
# ----------------------------------------------------------------------------------------------- #
# Imports standard libraries.
from concurrent.futures import ThreadPoolExecutor
import logging
import logging.handlers
import os
import pwd
import socket
import time

# Imports third party libraries.
from datetime import datetime, timedelta
import pandas as pd
import pytz
import requests

# Imports from src.

# ----------------------------------------------------------------------------------------------- #
# -------------------------------------- GLOBAL VARIABLES --------------------------------------- #
# ----------------------------------------------------------------------------------------------- #

# Log file.
LOG_FILE = "/var/log/coomep/collect_homewizard_data.log"
# Set up the logger.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.handlers.TimedRotatingFileHandler(
            "/var/log/coomep/collect_homewizard_data.log", when='midnight', backupCount=7
        )
    ]
)
# Get the username of the user running the script.
USERNAME = [usr.pw_name for usr in pwd.getpwall() if usr.pw_name[:3] in ['CDB', 'ECH', 'vde']][0]
# Load the configuration file.
CONFIG = pd.read_excel('/opt/coomep/users_config.xlsx')
# Path to the directory where the data will be saved.
DATA_DIR = "/opt/coomep/data"
# Check if the user has a photovoltaic installation.
HAS_PV = CONFIG[CONFIG['user_id'] == USERNAME]['has_pv'][0]
# Get base URL.
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
s.connect(("8.8.8.8", 80))
BASE_URL = '.'.join(s.getsockname()[0].split('.')[:3])
s.close()
# URL for the HomeWizard P1 sensor.
P1_URL = f"http://{BASE_URL}.59/api/v1/data"
# URL for the HomeWizard PV sensor.
PV_URL = f"http://{BASE_URL}.101/api/v1/data" if HAS_PV else None
# Columns to keep in the data.
COLUMNS_TO_KEEP = [
    'timestamp', 'date', 'total_power_import_kwh', 'total_power_export_kwh', 'active_power_w',
    'active_power_l1_w', 'active_power_l2_w', 'active_power_l3_w', 'active_voltage_v',
    'active_voltage_l1_v', 'active_voltage_l2_v', 'active_voltage_l3_v', 'active_current_a',
    'active_current_l1_a', 'active_current_l2_a', 'active_current_l3_a',
]
# Create a thread pool executor.
executor = ThreadPoolExecutor(max_workers=1)

# ----------------------------------------------------------------------------------------------- #
# ------------------------------------------ FUNCTIONS ------------------------------------------ #
# ----------------------------------------------------------------------------------------------- #


def get_sensor_data(next_sample_time):
    """
    Get the data from the HomeWizard sensors.

    :param next_sample_time:    The next sampling time.
    """
    p1_data, pv_data = None, None
    # Get the data from the HomeWizard P1 sensor.
    try:
        p1_response = requests.get(P1_URL, timeout=0.1)
        p1_response.raise_for_status()
        p1_data = p1_response.json()
        p1_data['timestamp'] = next_sample_time
        p1_data = pd.DataFrame([p1_data.values()], columns=p1_data.keys())
    except requests.exceptions.HTTPError as http_err:
        logging.error("%s - HTTP error occurred: %s \n", next_sample_time, http_err)
    except requests.exceptions.ConnectionError as conn_err:
        logging.error("%s - Error connecting to %s: %s \n", next_sample_time, P1_URL, conn_err)
    except requests.exceptions.RequestException as req_err:
        logging.error("%s - An error occurred: %s \n", next_sample_time, req_err)
    # Get the data from the HomeWizard PV sensor.
    try:
        pv_data = requests.get(PV_URL, timeout=0.1) if HAS_PV else None
        if pv_data:
            pv_data.raise_for_status()
            pv_data = pv_data.json()
            pv_data['timestamp'] = next_sample_time
            pv_data = pd.DataFrame([pv_data.values()], columns=pv_data.keys())
    except requests.exceptions.HTTPError as http_err:
        logging.error("%s - HTTP error occurred: %s \n", next_sample_time, http_err)
    except requests.exceptions.ConnectionError as conn_err:
        logging.error("%s - Error connecting to %s: %s \n", next_sample_time, PV_URL, conn_err)
    except requests.exceptions.RequestException as req_err:
        logging.error("%s - An error occurred: %s \n", next_sample_time, req_err)
    return p1_data, pv_data


def wait_until(target_time):
    """
    Wait until the target time is reached.

    :param target_time:    The target time to reach.
    """
    while datetime.now(pytz.timezone('Europe/Brussels')) < target_time:
        time.sleep(0.001)


def create_dirs(directory):
    """
    Create the directories if they do not exist.

    :param directory:    The directory to create.
    """
    if not os.path.exists(directory):
        os.makedirs(directory)


def save_to_csv(data, file_path, data_type):
    """
    Save the data to a CSV file. In this function, the data are resampled to have a sample every
    second and we add a column date to the data.

    :param data:        The data to save.
    :param file_path:   The path to the file where the data will be saved.
    :param data_type:   The type of data to save. (P1 or PV)
    """
    # Keep only the columns we want to save.
    data_to_save = data.filter(COLUMNS_TO_KEEP)
    # Set the index to the timestamp.
    data_to_save.set_index('timestamp', inplace=True)
    # Get the full index
    full_index = pd.date_range(
        start=data_to_save.index.min(), end=data_to_save.index.max(), freq='500ms'
    )
    # Reindex the data.
    data_to_save = data_to_save.reindex(full_index)
    # Resample the data.
    data_resampled = data_to_save.resample('s').mean()
    # Add a column date to the data.
    data_resampled['date'] = data_resampled.index.date
    # Add a column home_id to the data.
    data_resampled['home_id'] = USERNAME
    # Define the file name.
    filename = f"{file_path}_{data_type}.csv"
    # Check if the file exists.
    need_header = not os.path.exists(filename)
    with open(filename, mode='a', newline='', encoding="utf-8") as file:
        data_resampled.to_csv(file, header=need_header)


def save_data(p1_samples, pv_samples):
    """
    Save the data in CSV files.

    :param p1_samples:  The samples from the P1 sensor.
    :param pv_samples:  The samples from the PV sensor.
    """
    # Check if there are samples to save.
    if not p1_samples.empty:
        logging.info("Saving p1 data \n")
        # For each date, save p1 data in a CSV file.
        for date, group in p1_samples.groupby(p1_samples['timestamp'].dt.date):
            # Define the path to the folder to save the data.
            path_to_save = f"{DATA_DIR}/{date.year}/{date.month:02}"
            # Create the directory to save the data if it does not exist.
            create_dirs(path_to_save)
            # Save p1 data.
            save_to_csv(
                group,
                f"{path_to_save}/{date.year}_{date.month:02}_{date.day:02}_{USERNAME}", 'p1'
            )
    # Check if there are samples from the PV sensor.
    if HAS_PV and not pv_samples.empty:
        logging.info("Saving pv data \n")
        # For each date, save pv data in a CSV file.
        for date, group in pv_samples.groupby(pv_samples['timestamp'].dt.date):
            # Define the path to the folder to save the data.
            path_to_save = f"{DATA_DIR}/{date.year}/{date.month:02}"
            # Create the directory to save the data if it does not exist.
            create_dirs(path_to_save)
            # Save pv data.
            save_to_csv(
                group,
                f"{path_to_save}/{date.year}_{date.month:02}_{date.day:02}_{USERNAME}", 'pv'
            )


def main():
    """
    Main function to collect data from the HomeWizard sensors.
    """
    # DataFrames to store the samples (p1 and pv).
    p1_samples = pd.DataFrame()
    pv_samples = pd.DataFrame()
    # Get the current time.
    now = datetime.now(pytz.timezone('Europe/Brussels'))
    # Define the next sampling time.
    next_sample_time = now.replace(microsecond=0, second=now.second + 1)
    # Define the first sample to save.
    first_sample_to_save = next_sample_time + timedelta(minutes=1)
    # Wait until the next sampling time.
    wait_until(next_sample_time)
    # Start the sampling loop to collect data from the sensors every 0.5 seconds.
    try:
        while True:
            # Get the data from the sensors.
            p1_data, pv_data = get_sensor_data(next_sample_time)
            # Add the data to the samples.
            p1_samples = pd.concat([p1_samples, p1_data])
            if pv_data:
                pv_samples = pd.concat([pv_samples, pv_data])
            logging.info(
                "Data collected at %s - actually %s \n",
                next_sample_time, datetime.now(pytz.timezone('Europe/Brussels'))
            )
            # Check if it is time to save the data.
            if next_sample_time >= first_sample_to_save:
                # Save the data in a thread.
                executor.submit(save_data, p1_samples.copy(), pv_samples.copy())
                # Reset the samples to save.
                p1_samples = pd.DataFrame()
                pv_samples = pd.DataFrame()
                # Update the first sample to save
                first_sample_to_save += timedelta(minutes=1)
            # Update the next sampling time
            next_sample_time += timedelta(seconds=0.5)
            # Wait until the next sampling time
            wait_until(next_sample_time)
    except KeyboardInterrupt:
        logging.info("Keyboard interrupt received, stopping the sampling \n")
    except Exception as e:
        logging.error("An error occurred: %s \n", e, exc_info=True)
    finally:
        # Wait for the save thread to finish
        executor.shutdown(wait=True)
        # Save the remaining data
        save_data(p1_samples, pv_samples)
        logging.info("Sampling finished and data saved in csv files \n")


if __name__ == "__main__":
    main()
