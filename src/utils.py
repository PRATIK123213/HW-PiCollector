"""
This module is the utils module of the project.
"""

__title__: str = "utils"
__version__: str = "1.0.0"
__author__: str = "Brice Petit"
__license__: str = "MIT"

# ----------------------------------------------------------------------------------------------- #
# ------------------------------------------- IMPORTS ------------------------------------------- #
# ----------------------------------------------------------------------------------------------- #

# Imports standard libraries.
from contextlib import contextmanager
import json
import logging
import logging.handlers
import sys

# Imports third party libraries.

import paramiko

# Imports from src.

from config import (
    CREDENTIALS_FILE,
    LOG_FILE,
    LOG_HANDLER,
    LOG_LEVEL,
    VPN_LOG
)


def setup_log_level():
    """
    Function to set logging level based on a constant levels :
    - CRITICAL
    - ERROR
    - WARNING
    - INFO
    - DEBUG

    :return:    The logging level.
    """
    if LOG_LEVEL == "CRITICAL":
        return logging.CRITICAL
    elif LOG_LEVEL == "ERROR":
        return logging.ERROR
    elif LOG_LEVEL == "WARNING":
        return logging.WARNING
    elif LOG_LEVEL == "INFO":
        return logging.INFO
    elif LOG_LEVEL == "DEBUG":
        return logging.DEBUG


def get_log_handler():
    """
    Function to setup the output of the logging.

    :return:    The logging handler.
    """
    if LOG_HANDLER == "logfile":
        handler = logging.handlers.TimedRotatingFileHandler(
            LOG_FILE,
            when='midnight',
            backupCount=7,
        )
    else:  # stdout
        handler = logging.StreamHandler(stream=sys.stdout)
    return handler


# Create and configure logger.
logging.getLogger("paramiko").setLevel(logging.ERROR)
logging.basicConfig(
    level=setup_log_level(),
    format="{asctime} {levelname:<8} {filename:<16} {message}",
    style='{',
    handlers=[get_log_handler()]
)


def parse_routing_table():
    """
    Parse the ROUTING TABLE section and extract 'Virtual Address' and 'Common Name'.

    :param file_path:   The path to the file to parse.

    :return:            A dictionary with the 'Common Name' as key and 'Virtual Address' as value.
    """
    with open(VPN_LOG, 'r', encoding='utf-8') as file:
        lines = file.readlines()
    # Initialize variables
    routing_table_data = {}
    in_routing_table = False
    # Parse lines
    for line in lines:
        # Remove surrounding whitespace.
        line = line.strip()
        # Check if the line is the start of the routing table to start capturing data.
        if line == "ROUTING TABLE":
            in_routing_table = True
        # Check if the line is the end of the routing table to stop capturing data.
        elif line == "GLOBAL STATS":
            in_routing_table = False
            break
        # Capture data if we are in the routing table and the line is not empty.
        elif in_routing_table and line:  # Skip empty lines
            # Split the line into fields
            fields = line.split(',')
            # Check if the line has at least 2 fields.
            if len(fields) >= 2:
                # Get the virtual address and common name.
                virtual_address = fields[0].strip()
                common_name = fields[1].strip()
                # Add the data to the routing table data.
                routing_table_data[common_name] = virtual_address
    return routing_table_data


def str2bool(v: str) -> bool:
    """
    Convert a string to a boolean.

    :param v:   The string to convert.

    :return:    The boolean value.
    """
    return v.lower() in ("yes", "oui", "true", "t", "1")


def get_pwd():
    """
    Get the password from the user.

    :return:    The password.
    """
    with open(CREDENTIALS_FILE, 'r', encoding='utf-8') as cred:
        pwd = json.load(cred)['pwd']
    return pwd


def ssh_connection(host, username, password):
    """
    Connect to the Raspberry Pi with the SSH protocol.

    :param host:        The host of the Raspberry Pi.
    :param username:    The username to connect.
    :param password:    The password to connect.

    :return:            The SSH connection.
    """
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(host, username=username, password=password)
    return ssh


@contextmanager
def ssh_sftp_connection(hostname, home_id, pwd):
    """
    Connect to the Raspberry Pi with the SSH protocol and open a SFTP connection.

    :param hostname:    The hostname of the Raspberry Pi.
    :param home_id:     The username to connect.
    :param pwd:         The password to connect.

    :return:            The SFTP connection.
    """
    ssh = ssh_connection(hostname, home_id, pwd)
    sftp = ssh.open_sftp()
    try:
        yield sftp
    finally:
        sftp.close()
        ssh.close()
