"""
In this module, we define the global variables that will be used in the entire project.
"""

__title__: str = "config"
__version__: str = "1.0.0"
__author__: str = "Brice Petit"
__license__: str = "MIT"

# ----------------------------------------------------------------------------------------------- #
# ------------------------------------------- IMPORTS ------------------------------------------- #
# ----------------------------------------------------------------------------------------------- #

# Imports standard libraries

# Imports third party libraries

# Imports from src

# ----------------------------------------------------------------------------------------------- #
# -------------------------------------- GLOBAL VARIABLES --------------------------------------- #
# ----------------------------------------------------------------------------------------------- #

# ------------------------------------- Execution Variables ------------------------------------- #
# The path to the users configuration file.
USERS_CONFIG = "/opt/coomep/users_config.xlsx"
# The path to the data directory.
DATA_DIR = "/opt/coomep/data"
# The path to the temporary directory.
TMP_DIR = "/tmp/coomep"
# The limitation of the number of days to collect raw data.
FROM_FIRST_TS = 4
# The oldest timestamp to consider for the raw data that does not exist.
OLDEST_TS = "1970-01-01"
# Number of lines to insert per batch insert when inserting in Cassandra table.
INSERTS_PER_BATCH = 43200
# Log of the OpenVPN status to get the virtual addresses.
VPN_LOG = "/var/log/openvpn/openvpn-status.log"
# The path to the credentials file.
CREDENTIALS_FILE = "/opt/coomep/credentials.json"

# ------------------------------------- Cassandra Variables ------------------------------------- #

# Set to True for production.
PROD = False
# Log files.
LOG_FILE = "/var/log/coomep/prod.log" if PROD else "/var/log/coomep/test.log"
LOG_LEVEL = "INFO" if PROD else "DEBUG"
LOG_HANDLER = "logfile" if PROD else "stdout"
# Credentials file.
CASSANDRA_CREDENTIALS_FILE = '/opt/vde/cassandra_serv_credentials.json'
# Cassandra keyspaces.
CASSANDRA_KEYSPACE = "homewizard" if PROD else "test"
# Use NetworkTopologyStrategy if more than one datacenter.
CASSANDRA_REPLICATION_STRATEGY = 'SimpleStrategy'
# The replication factor must not exceed the number of nodes in the cluster.
CASSANDRA_REPLICATION_FACTOR = 1
# Cassandra tables names.
TBL_ACCESS = "access"
TBL_POWER = "power"
TBL_RAW_P1 = "raw_p1"
TBL_RAW_PV = "raw_pv"
TBL_LAST_TS = "last_ts"

# -------------------------------------- Server Addresses --------------------------------------- #

# Address of the frontend server.
SERVER_FRONTEND_IP = 'iridia-cde-frontend.hpda.ulb.ac.be'
# Address of the backend server.
SERVER_BACKEND_IP = 'iridia-cde-db.hpda.ulb.ac.be'
# Port of the backend server.
SERVER_BACKEND_PORT = 9042
