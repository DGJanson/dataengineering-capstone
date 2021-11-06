"""
Some methods for connecting to the database
"""

import psycopg2
import logging

logger = logging.getLogger("sparkifier")

def createConnection(config):
    """
    Simply create a connection to a postgres database, based on a config
    THE CALLING METHOD SHOULD CLOSE IT.

    Args:
        config (dict): the config as read in by configreader

    Raises:
        ValueError: in case of not being able the connect

    Returns:
        connection: a connection to a database. Can be used for queries, etc.
    """
    try:
        connection = psycopg2.connect(host = config["database"]["host"],
                                      port = config["database"]["port"],
                                      database = config["database"]["database"],
                                      user = config["database"]["username"],
                                      password = config["database"]["password"])
        return(connection)
    except Exception as err:
        logger.error("Could not connect to database. Message:")
        logger.error(err)
        raise ValueError("Generic exception while trying to connect to database")

def performQueryNoResult(conn, query):
    """
    Simply performs the query on the connection. Does not return anything.
    This method will create a cursor and close it.

    Args:
        query (string): A query to perform using the passed connection

    Raise:
        ValueError: in case anything goes wrong

    Returns:
        nothing
    """
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        cursor.close()
    except Exception as err:
        logger.error("Error while executing query: {}. Message:".format(query))
        logger.error(err)
        raise ValueError("Error performing query")
