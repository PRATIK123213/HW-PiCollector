"""
In this module, we define functions to interact with a Cassandra database.
"""

__title__: str = "py_to_cassandra"
__version__: str = "1.0.0"
__author__: str = "Brice Petit"
__license__: str = "MIT"

# ----------------------------------------------------------------------------------------------- #
# ------------------------------------------- IMPORTS ------------------------------------------- #
# ----------------------------------------------------------------------------------------------- #

# Imports standard libraries
import json
import logging
import os.path
import sys

# Imports third party libraries
import cassandra
import cassandra.auth
import cassandra.cluster
import cassandra.policies
import pandas as pd

# Imports from src
from config import (
    CASSANDRA_CREDENTIALS_FILE,
    CASSANDRA_KEYSPACE,
    CASSANDRA_REPLICATION_FACTOR,
    CASSANDRA_REPLICATION_STRATEGY,
    INSERTS_PER_BATCH,
    SERVER_BACKEND_IP,
    SERVER_BACKEND_PORT
)

# ----------------------------------------------------------------------------------------------- #
# -------------------------------------- GLOBAL VARIABLES --------------------------------------- #
# ----------------------------------------------------------------------------------------------- #


# ----------------------------------------------------------------------------------------------- #
# ------------------------------------------ FUNCTIONS ------------------------------------------ #
# ----------------------------------------------------------------------------------------------- #

def load_credentials(file: str) -> dict:
    """
    Function to load the credentials from a JSON file.

    :param file:    str, the path to the JSON file.

    :return:        dict, the credentials.
    """
    credentials = {}
    # Check if the file exists.
    if os.path.exists(file):
        # Load the credentials.
        with open(file, 'r', encoding='utf-8') as f:
            credentials = json.load(f)
    return credentials


def create_keyspace(session, keyspace_name):
    """
    Function to create a new keyspace in the Cassandra database.

    :param session:         cassandra.cluster.Session, the session to the Cassandra database.
    :param keyspace_name:   str, the name of the keyspace to create.

    command : CREATE KEYSPACE <keyspace> WITH REPLICATION =
                {'class': <replication_class>, 'replication_factor': <replication_factor>}
    """
    # Query to create a new keyspace.
    keyspace_query = (
        f"CREATE KEYSPACE {keyspace_name} WITH REPLICATION = {{'class' : "
        f"'{CASSANDRA_REPLICATION_STRATEGY}', 'replication_factor': "
        f"{CASSANDRA_REPLICATION_FACTOR}}};"
    )
    # Log the query.
    logging.debug(keyspace_query)
    # Execute the query.
    session.execute(keyspace_query)


def connect_to_cluster(keyspace):
    """
    Function to connect to the Cassandra Cluster.

    - either locally : simple, ip = 127.0.0.1:9042, by default
    - or with username and password using AuthProvider, if the credentials file exits.

    :param keyspace:    str, the name of the keyspace to connect to.

    :return:            cassandra.cluster.Session, the session to the Cassandra database.
    """
    lbp = cassandra.policies.DCAwareRoundRobinPolicy(local_dc='datacenter1')
    auth_provider = None
    try:
        # Load the credentials.
        cred = load_credentials(CASSANDRA_CREDENTIALS_FILE)
        if len(cred):
            auth_provider = cassandra.auth.PlainTextAuthProvider(
                username=cred["username"],
                password=cred["password"]
            )
        # Connect to the Cassandra Cluster.
        cluster = cassandra.cluster.Cluster(
            contact_points=[SERVER_BACKEND_IP, '127.0.0.1'],
            port=SERVER_BACKEND_PORT,
            load_balancing_policy=lbp,
            protocol_version=4,
            auth_provider=auth_provider,
        )
        # Connect to the keyspace.
        session = cluster.connect()
        session.set_keyspace(keyspace)
    except cassandra.InvalidRequest:
        # Create the keyspace if it does not exist.
        create_keyspace(session, keyspace)
        session.set_keyspace(keyspace)
    except Exception:
        logging.critical("Exception occured in 'connect_to_cluster' cassandra: ", exc_info=True)
        sys.exit(1)
    return session


def get_right_format(values):
    """
    Function to get the right string format given a list of values for a cassandra query.

    :param values:  list, the list of values to format.

    :return:        list, the list of formatted values.
    """
    res = []
    for val in values:
        if isinstance(val, str):
            res.append("'" + val + "'")
        elif isinstance(val, list):
            # => ['v1', 'v2', 'v3', ... ]
            _list = "["
            for v in val:
                _list += "'" + v + "',"
            res.append(_list[:-1] + "]")
        elif "isoformat" in dir(val):
            res.append("'" + v.isoformat() + "'")
        else:
            res.append(str(v))
    return res


def get_ordering(ordering):
    """
    Function to get the ordering of the table.

    The ordering format is {"column_name": "ASC", "column_name2": "DESC"}.

    :param ordering:    dict, the ordering of the table.

    :return:            str, the ordering of the table.
    """
    res = ""
    if len(ordering) > 0:
        res += "WITH CLUSTERING ORDER BY ("
        for col_name, ordering_type in ordering.items():
            res += col_name + " " + ordering_type + ","
        # Remove the last "," and add ")".
        res = res[:-1] + ")"
    return res


# Create a session to the Cassandra database.
SESSION = connect_to_cluster(CASSANDRA_KEYSPACE)


def get_insert_query(keyspace, table, columns, values):
    """
    Get the prepared statement for an insert query.

    command : INSERT INTO <keyspace>.<table> (<columns>) VALUES (<values>);

    :param keyspace:    str, the name of the keyspace.
    :param table:       str, the name of the table.
    :param columns:     list, the columns of the table.
    :param values:      list, the values to insert.

    :return:            str, the query.
    """
    return (
        f"INSERT INTO {keyspace}.{table} ({','.join(columns)}) "
        f"VALUES ({','.join(get_right_format(values))});"
    )

def insert_query(keyspace, table, columns, values):
    """
    Insert a new row in the table

    :param keyspace:    str, the name of the keyspace.
    :param table:       str, the name of the table.
    :param columns:     list, the columns of the table.
    :param values:      list, the values to insert.

    command : INSERT INTO <keyspace>.<table> (<columns>) VALUES (<values>);
    """
    # Create the query.
    query = get_insert_query(keyspace, table, columns, values)
    # Log the query.
    logging.debug("===> insert query : %s", query)
    # Execute the query.
    SESSION.execute(query)


def batch_insert_query(keyspace, table, df):
    """
    Insert batch of rows in a table in the database based on a DataFrame.

    :param keyspace:    str, the name of the keyspace.
    :param table:       str, the name of the table.
    :param df:          pandas.DataFrame, the data to insert.

    command : BEGIN BATCH <insert_query> APPLY BATCH;
    """
    # List of queries.
    queries = []
    # For each row in the DataFrame.
    for i in range(len(df)):
        # Get the insert query for the current row and append it to the list of queries.
        queries.append(get_insert_query(keyspace, table, list(df.columns), df.iloc[i].values))
        # If the number of queries is equal to the number of inserts per batch, execute the batch.
        if (i + 1) % INSERTS_PER_BATCH == 0:
            batch_query = "BEGIN BATCH " + " ".join(queries) + " APPLY BATCH;"
            logging.debug("===> batch insert query : %s", batch_query)
            SESSION.execute(batch_query)
            queries = []
    # If there are remaining queries, execute the batch.
    if queries:
        batch_query = "BEGIN BATCH " + " ".join(queries) + " APPLY BATCH;"
        logging.debug("===> batch insert query : %s", batch_query)
        SESSION.execute(batch_query)


def create_table(keyspace, table_name, columns, primary_keys, clustering_keys, ordering):
    """
    Function to create a new table in the database.

    :param keyspace:        str, the name of the keyspace.
    :param table_name:      str, the name of the table to create.
    :param columns:         list, the columns of the table.
    :param primary_keys:    list, the primary keys of the table.
    :param clustering_keys: list, the clustering keys of the table.
    :param ordering:        dict, the ordering of the table.

    command : CREATE TABLE IF NOT EXISTS <keyspace>.<table_name>
                (<columns>, PRIMARY KEY (<primary keys><clustering keys>)) <ordering>;
    """
    # Create the query.
    query = (
        f"CREATE TABLE IF NOT EXISTS {keyspace}.{table_name} "
        f"({','.join(columns)}, "
        f"PRIMARY KEY (({','.join(primary_keys)}"
        f"{',' + ','.join(clustering_keys) if len(clustering_keys) else ''}))"
        f"{get_ordering(ordering)});"
    )
    # Log the query.
    logging.debug("===> create table query : %s", query)
    # Execute the query.
    SESSION.execute(query)


def is_table_empty(keyspace, table_name):
    """
    Function to check if a table is empty.

    :param keyspace:    str, the name of the keyspace.
    :param table_name:  str, the name of the table.

    :return:            bool, True if the table is empty, False otherwise.
    """
    # Create the query.
    query = f"SELECT * FROM {keyspace}.{table_name} LIMIT 1;"
    # Log the query.
    logging.debug("===> is table empty query : %s", query)
    # Execute the query.
    rows = SESSION.execute(query)
    return len(rows.current_rows) == 0


def select_query(
        keyspace,
        table_name,
        columns,
        where_clause='',
        limit=None,
        allow_filtering=True,
        distinct=False,
        tz='CET'
):
    """
    Function to apply a select query on the Cassandra database. The command is the followuing:
    SELECT <distinct> <columns>
    FROM <keyspace>.<table_name>
    WHERE <where_clause> <LIMIT> <ALLOW FILTERING>;

    :param keyspace:        str, the name of the keyspace.
    :param table_name:      str, the name of the table.
    :param columns:         list, the columns to select.
    :param where_clause:    str, the where clause.
    :param limit:           int, the limit of the query.
    :param allow_filtering: bool, True if the query allows filtering, False otherwise.
    :param distinct:        bool, True if the query is distinct, False otherwise.
    :param tz:              str, the timezone to convert the dates.
    
    
    return:                 pandas.DataFrame, the result of the query.
    """
    query = (
        f"SELECT {distinct} {','.join(columns)} "
        f"FROM {keyspace}.{table_name} "
        f"{f'WHERE {where_clause}' if len(where_clause) > 0 else '' } "
        f"{f'LIMIT {limit}' if limit is not None else ''} "
        f"{'ALLOW FILTERING' if allow_filtering else ''};"
    )
    logging.debug("===> select query : %s", query)
    df = pd.DataFrame(list(SESSION.execute(query)))
    if 'ts' in df.columns:
        df['ts'] = df['ts'].dt.tz_localize("UTC").dt.tz_convert(tz)
    return df


def update_query(keyspace, table_name, columns, values, where_clause):
    """
    Function to apply an update query on the Cassandra database. The command is the following:
    UPDATE <keyspace>.<table>
    SET <set_clause>
    WHERE <where_clause>;

    :param keyspace:        str, the name of the keyspace.
    :param table_name:      str, the name of the table.
    :param columns:         list, the columns to update.
    :param values:          list, the values to update.
    :param where_clause:    str, the where clause.
    """
    query = (
        f"UPDATE {keyspace}.{table_name} "
        f"SET {','.join([f'{col}={val}' for col, val in zip(columns, values)])} "
        f"WHERE {where_clause};"
    )
    logging.debug("===> update query : %s", query)
    SESSION.execute(query)
