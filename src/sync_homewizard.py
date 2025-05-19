"""
This module is the sync module of the project. The idea is to synchronize the data from the
HomeWizard sensors to a Cassandra database.
"""

__title__: str = "sync_homewizard"
__version__: str = "1.0.0"
__author__: str = "Brice Petit"
__license__: str = "MIT"

# ----------------------------------------------------------------------------------------------- #
# ------------------------------------------- IMPORTS ------------------------------------------- #
# ----------------------------------------------------------------------------------------------- #

# ------------------------------------- STANDARD LIBRARIES -------------------------------------- #

import argparse
import multiprocessing as mp
import os

# ------------------------------------ THIRD PARTY LIBRARIES ------------------------------------ #

import bcrypt
import pandas as pd

# ------------------------------------------- SOURCE -------------------------------------------- #

from config import (
    CASSANDRA_KEYSPACE,
    DATA_DIR,
    FROM_FIRST_TS,
    OLDEST_TS,
    TBL_ACCESS,
    TBL_POWER,
    TBL_RAW_P1,
    TBL_RAW_PV,
    TBL_LAST_TS,
    TMP_DIR,
    USERS_CONFIG,
)
import py_to_cassandra as ptc

from utils import get_pwd, logging, parse_routing_table, ssh_sftp_connection

# ----------------------------------------------------------------------------------------------- #
# -------------------------------------- GLOBAL VARIABLES --------------------------------------- #
# ----------------------------------------------------------------------------------------------- #

# -------------------------------------------- PATHS -------------------------------------------- #


# ------------------------------------- Execution Variables ------------------------------------- #


# ----------------------------------------------------------------------------------------------- #
# ------------------------------------------ FUNCTIONS ------------------------------------------ #
# ----------------------------------------------------------------------------------------------- #


def create_tables():
    """
    Function to create the tables in the Cassandra database. The tables are the following:
    - access: the table to store the access data (login info, pv info and installations info where
    the installation info is about which site is accessible from the current user).
    - raw_p1: the table to store the raw data from the P1 sensor.
    - raw_pv: the table to store the raw data from the PV sensor.
    - last_ts: the table to store the last timestamp of the data for each user.
    - power: the table to store the calculated power data.
    """
    # Create the access table.
    ptc.create_table(
        CASSANDRA_KEYSPACE,
        TBL_ACCESS,
        ["home_id TEXT", "pwd TEXT", "has_pv BOOLEAN", "installations LIST<TEXT>"],
        ["home_id"],
        [],
        {}
    )
    # Create the table for the raw p1 data.
    ptc.create_table(
        CASSANDRA_KEYSPACE,
        TBL_RAW_P1,
        [
           "home_id TEXT", "day TEXT", "timestamp TIMESTAMP", "total_power_import_kwh FLOAT",
           "total_power_export_kwh FLOAT", "active_power_w FLOAT", "active_power_l1_w FLOAT",
           "active_power_l2_w FLOAT", "active_power_l3_w FLOAT", "active_voltage_v FLOAT",
           "active_voltage_l1_v FLOAT", "active_voltage_l2_v FLOAT", "active_voltage_l3_v FLOAT",
           "active_current_a FLOAT", "active_current_l1_a FLOAT", "active_current_l2_a FLOAT",
           "active_current_l3_a FLOAT"
        ],
        ["home_id"],
        ["day", "timestamp"],
        {"day": "ASC", "timestamp": "ASC"}
    )
    # Create the table for the raw pv data.
    ptc.create_table(
        CASSANDRA_KEYSPACE,
        TBL_RAW_PV,
        [
           "home_id TEXT", "day TEXT", "timestamp TIMESTAMP", "total_power_import_kwh FLOAT",
           "total_power_export_kwh FLOAT", "active_power_w FLOAT", "active_power_l1_w FLOAT",
           "active_power_l2_w FLOAT", "active_power_l3_w FLOAT", "active_voltage_l1_v FLOAT",
           "active_voltage_l2_v FLOAT", "active_voltage_l3_v FLOAT", "active_current_l1_a FLOAT",
           "active_current_l2_a FLOAT","active_current_l3_a FLOAT"
        ],
        ["home_id"],
        ["day", "timestamp"],
        {"day": "ASC", "timestamp": "ASC"}
    )
    # Create the table for the last timestamp.
    ptc.create_table(
        CASSANDRA_KEYSPACE,
        TBL_LAST_TS,
        ["home_id TEXT", "timestamp TIMESTAMP"],
        ["home_id"],
        [],
        {}
    )
    # Create the table for power.
    ptc.create_table(
        CASSANDRA_KEYSPACE,
        TBL_POWER,
        [
            "home_id TEXT", "day TEXT", "timestamp TIMESTAMP", "p_cons FLOAT", "p_prod FLOAT",
            "p_tot FLOAT"
        ],
        ["home_id"],
        ["day", "timestamp"],
        {"day": "ASC", "timestamp": "ASC"}
    )


def check_access_table():
    """
    Check the access table. If the table is empty, insert the users from the configuration file.
    """
    # If the access table is empty, insert the users.
    if ptc.is_table_empty(CASSANDRA_KEYSPACE, TBL_ACCESS):
        # Read the configuration file.
        xlsx = pd.read_excel(USERS_CONFIG)
        # For each user in the configuration file.
        for i in range(len(xlsx)):
            # Get the secured password.
            pwd = bcrypt.hashpw(
                xlsx.iloc[i][0].encode('utf-8'), bcrypt.gensalt()
            ).decode('utf-8')
            # Insert the user in the access table.
            ptc.insert_query(
                CASSANDRA_KEYSPACE,
                TBL_ACCESS,
                ["login", "pwd", "has_pv", "installations"],
                [
                    xlsx.iloc[i]['user_id'], pwd, xlsx.iloc[i]['has_pv'],
                    xlsx.iloc[i]['configurations'].strip('][').split(', ')
                ]
            )


def check_last_ts(home_id: str,  new_ts: pd.Timestamp, last_ts: pd.Timestamp=None):
    """
    Check the last timestamp for a user and update it if the new timestamp is greater.

    :param home_id: The home_id of the user.
    :param new_ts:  The new timestamp to check.
    :param last_ts: The last timestamp for the user.
    """
    # Check if the last timestamp is not empty.
    if last_ts:
        # Check if the new timestamp is greater than the last timestamp. If it is, update it.
        if new_ts > last_ts:
            ptc.update_query(
                CASSANDRA_KEYSPACE, TBL_LAST_TS, ["timestamp"], [new_ts.floor('s')],
                f"home_id = '{home_id}'"
            )
    # Otherwise, insert the new timestamp.
    else:
        ptc.insert_query(
            CASSANDRA_KEYSPACE, TBL_LAST_TS, ["home_id", "timestamp"], [home_id, new_ts.floor('s')]
        )


def sync_files(sftp, home_id, date, data_type, table, classic_sync = False):
    """
    Function to sync the files from the RPi to the Cassandra database.

    :param sftp:            The SFTP connection.
    :param home_id:         The home_id of the user.
    :param date:            The date to sync.
    :param data_type:       The type of data to sync.
    :param table:           The table to insert the data.
    :param classic_sync:    Boolean to indicate if the sync is the classic sync.
    """
    # Get the year, month and day.
    year, month, day = date.year, date.month, date.day
    # Get the filename.
    filename = f"{year}_{month:02}_{day:02}_{home_id}_{data_type}.csv"
    try:
        # Check if the file exists. If not, it raises an exception.
        sftp.stat(f"{DATA_DIR}/{year}/{month}/{filename}")
        # Get the file from the RPi.
        sftp.get(f"{DATA_DIR}/{year}/{month}/{filename}", f"{TMP_DIR}/{filename}")
        # If the file exists, read the data.
        df = pd.read_csv(f"{TMP_DIR}/{filename}")
        # Add the home_id and the day to the DataFrame.
        df['home_id'] = home_id
        df['day'] = f"{year}-{month}-{day}"
        if classic_sync:
            df = df[df['timestamp'] >= date]
        # Write the df in the Cassandra database.
        ptc.batch_insert_query(CASSANDRA_KEYSPACE, table, df)
        # Remove the file from the tmp folder.
        os.remove(f"{TMP_DIR}/{filename}")
    except FileNotFoundError:
        logging.warning(
            f"Exception occurred in 'sync_files', the file {filename} "
            f"does not exist for the home_id {home_id}: ",
            exc_info=True
        )
    return df


def get_available_dates(sftp):
    """
    Get the available dates in the data folder.

    :param sftp:    The SFTP connection.

    :return:        The list of available dates.
    """
    available_dates = []
    # For each year folder.
    for year in sorted(sftp.listdir(DATA_DIR)):
        # For each month folder.
        for month in sorted(sftp.listdir(f"{DATA_DIR}/{year}")):
            # For each file in the month folder.
            for file in sorted(sftp.listdir(f"{DATA_DIR}/{year}/{month}")):
                # Check if the file is a csv file.
                if file.endswith('.csv'):
                    # Get the day from the filename.
                    day = file.split('_')[2]
                    # Get the current timestamp.
                    current_ts = pd.Timestamp(f"{year}-{month}-{day}")
                    available_dates.append(current_ts)
    return available_dates

def sync_all_days(sftp, home_id, has_pv, last_ts):
    """
    Sync all days of the data for the user.

    :param sftp:        The SFTP connection.
    :param home_id:     The home_id of the user.
    :param has_pv:      Boolean to indicate if the user has a pv.
    :param last_ts:     The last timestamp for the user.
    """
    # Get the available dates.
    available_dates = get_available_dates(sftp)
    # Check if there are available dates.
    if not available_dates:
        logging.warning(f"No available dates found for home_id {home_id}.")
        return
    # Get the start and end date.
    start_date = min(available_dates)
    end_date = max(available_dates)
    logging.info(f"Syncing all data from {start_date} to {end_date} for home_id {home_id}.")
    # Sync the data for the custom dates.
    sync_dates(sftp, home_id, has_pv, last_ts, start_date, end_date)


def sync_dates(sftp, home_id, has_pv, last_ts, start_date, end_date, classic_sync = False):
    """
    Sync the data for custom dates.

    :param sftp:            The SFTP connection.
    :param home_id:         The home_id of the user.
    :param has_pv:          Boolean to indicate if the user has a pv.
    :param last_ts:         The last timestamp for the user.
    :param start_date:      The start date to sync.
    :param end_date:        The end date to sync.
    :param classic_sync:    Boolean to indicate if the sync is the classic sync.
    """
    # For each day in the range.
    for date in pd.date_range(start_date, end_date):
        logging.info(f"Collecting data for the day {date} for the home_id {home_id}.")
        # Sync the data for the day for the p1 meter.
        p1 = sync_files(sftp, home_id, date, "p1", TBL_RAW_P1, classic_sync)
        # Compute the power data.
        final_df = pd.DataFrame({
            "home_id": home_id,
            "day": p1["day"],
            "timestamp": p1["timestamp"],
            "p_cons": p1["active_power_w"],
            "p_prod": 0,
            "p_tot": p1["active_power_w"]
        })
        # Sync the data for the day for the pv meter if the user has a pv.
        if has_pv:
            pv = sync_files(sftp, home_id, date, "pv", TBL_RAW_PV, classic_sync)
            # Compute the power data.
            final_df["p_prod"] = pv["active_power_w"] * -1
            final_df["p_tot"] = final_df["p_cons"] + final_df["p_prod"]
        logging.info(f"Inserting the computed data for the day {date} for the home_id {home_id}.")
        # Write the df in the Cassandra database.
        ptc.batch_insert_query(CASSANDRA_KEYSPACE, TBL_POWER, final_df)
    # Check the last timestamp is the newest.
    check_last_ts(home_id, end_date, last_ts)


def process_home(args):
    """
    Process the home to sync the data.

    :param args:    The arguments containing the home_id, has_pv, hostname, pwd, all_days,
                    start_date, end_date, last_ts, now.
    """
    # Get the arguments from the tuple during the multiprocessing.
    home_id, has_pv, hostname, pwd, all_days, start_date, end_date, last_ts, now = args
    if not hostname:
        logging.warning(
            f"Exception occured in 'sync_data', the home_id {home_id} is not in the "
            "list of hosts to collect data from: ",
            exc_info=True
        )
        return
    logging.info(f"Start the sync of the data for the home_id {home_id}.")
    # Connect to the RPi.
    with ssh_sftp_connection(hostname, home_id, pwd) as sftp:
        # Check if we need to sync all days.
        if all_days:
            sync_all_days(sftp, home_id, has_pv, last_ts)
        else:
            # Check if we need to sync custom dates.
            if start_date or end_date:
                start_date = start_date or (end_date - pd.Timedelta(days=FROM_FIRST_TS))
                end_date = end_date or now
                classic_sync = False
            # Otherwise, do the classic sync.
            else:
                start_date = (
                    last_ts
                    if (now - last_ts) < pd.Timedelta(days=FROM_FIRST_TS)
                    else now - pd.Timedelta(days=FROM_FIRST_TS)
                )
                end_date = now
                classic_sync = True
            sync_dates(
                sftp, home_id, has_pv, last_ts, start_date, end_date, classic_sync
            )


def sync_data(
    bids: pd.DataFrame, hostnames: dict, last_ts: pd.DataFrame, all_days=False, start_date=None,
    end_date=None
):
    """
    Sync the data from the HomeWizard sensors to the Cassandra database.

    :param bids:        The DataFrame with the bids and has_pv info.
    :param hostnames:   The dictionary with hostnames of each RPi.
    :param last_ts:     The DataFrame with the last timestamp for each bid.
    :param all_days:    Sync all days.
    :param start_date:  The start date to sync.
    :param end_date:    The end date to sync.
    """
    # Create a dictionary with the last timestamp for each home_id where the key is the home_id.
    last_ts_dic = {row[0]: row[1] for idx, row in last_ts.iterrows()}
    # Get the password to connect to the RPi.
    pwd = get_pwd()
    # Get the now timestamp.
    now = pd.Timestamp.now().floor('s')
    # Create the arguments for the multiprocessing. The arguments are home_id, has_pv, hostname,
    # pwd, all_days, start_date, end_date, last_ts, now.
    args = [
        (
            row['home_id'], row['has_pv'], hostnames.get(row['home_id']), pwd, all_days,
            start_date, end_date, last_ts_dic.get(row['home_id'], pd.Timestamp(OLDEST_TS)), now
        )
        for _, row in bids.iterrows()
    ]
    # Use multiprocessing Pool to process homes in parallel with a limit of
    # 4 processes or CPU count.
    with mp.Pool(processes=min(mp.cpu_count(), 4)) as pool:
        pool.map(process_home, args)


def sync_homewizard(all_days=False, custom_start_date=None, custom_end_date=None):
    """
    Sync the data from the HomeWizard sensors to the Cassandra database.

    :param all_days:           Sync all days.
    :param custom_start_date:  The start date to sync.
    :param custom_end_date:    The end date to sync.
    """
    # Create a folder to store the data in the tmp folder.
    if not os.path.exists("/tmp/coomep"):
        os.mkdir("/tmp/coomep")
    # Get the bids, hostnames and last timestamp.
    bids = ptc.select_query(CASSANDRA_KEYSPACE, TBL_ACCESS, ['home_id', 'has_pv'])
    hostnames = parse_routing_table()
    last_ts = ptc.select_query(CASSANDRA_KEYSPACE, TBL_LAST_TS, '*')
    # Sync the data.
    sync_data(bids, hostnames, last_ts, all_days, custom_start_date, custom_end_date)


def get_arguments():
    """
    Get the arguments of the script.

    :return:    The arguments.
    """
    # Create the argument parser.
    arg_parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    # Create a mutually exclusive group.
    group = arg_parser.add_mutually_exclusive_group(required=True)
    # Add the arguments.
    group.add_argument(
        "--all",
        action="store_true",
        help="Sync all days."
    )
    group.add_argument(
        "--start",
        type=str,
        default=str(pd.Timestamp.now()),
        help="Start date to sync. Format : YYYY-mm-dd"
    )
    group.add_argument(
        "--end",
        type=str,
        default=str(pd.Timestamp.now()),
        help="End date to sync. Format : YYYY-mm-dd"
    )
    # Parse the arguments.
    args = arg_parser.parse_args()
    # Check if the start and end arguments are used together.
    if (args.start and not args.end) or (args.end and not args.start):
        arg_parser.error("--start and --end must be used together")
    return args


def main():
    """
    Main function of the script.
    """
    # Get the arguments.
    args = get_arguments()
    # Create the tables.
    create_tables()
    # Check the access table.
    check_access_table()
    # Sync the data.
    if args.all:
        sync_homewizard(all_days=True)
    else:
        sync_homewizard(
            custom_start_date=pd.Timestamp(args.start), custom_end_date=pd.Timestamp(args.end)
        )


if __name__ == '__main__':
    main()
