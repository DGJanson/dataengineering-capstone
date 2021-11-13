"""
Some methods for connecting to the database
"""

import psycopg2
import logging
import os

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
        conn: connection to use for query
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

def performQueryWithOneResult(conn, query):
    """
    Simply performs the query on the connection.
    This method will create a cursor and close it.

    Args:
        conn: connection to use for query
        query (string): A query to perform using the passed connection

    Raise:
        ValueError: in case anything goes wrong

    Returns:
        The single result
    """
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        result = cursor.fetchone()
        cursor.close()
        return result
    except Exception as err:
        logger.error("Error while executing query: {}. Message:".format(query))
        logger.error(err)
        raise ValueError("Error performing query")

def performQueryWriteToCsv(conn, query, fileLocation):
    """
    Performs a query and writes it to a csv.
    Does not do batches or anything, so writes the entire output in one go.
    (So do not make queries too big :))

    Args:
        conn: connection to use for query
        query (string): the export query to perform. Pass a SELECT query, this method will output to csv
        fileLocation: full path relative path (start with ./) to file to export. In case of relative path, will write to os.getcwd

    Raises:
        ValueError: in case anything goes wrong

    Returns:
        nothing
    """
    if fileLocation.startswith("./"):
        fileLocation = os.path.join(os.getcwd(), fileLocation)

    try:
        cursor = conn.cursor()
        outputQuery = "COPY ({}) TO STDOUT WITH CSV HEADER".format(query)
        with open (fileLocation, "w") as outputFile:
            cursor.copy_expert(outputQuery, outputFile)
        cursor.close()
    except Exception as err:
        logger.error("Error while executing output query: {}. Message:".format(outputQuery))
        logger.error(err)
        raise ValueError("Error performing query")


def getSparkDBProps(config):
    """
    Create a url and properties dict that spark can use to connect to the (postgres) database

    Args:
        config (dict): config as read by config reader

    Returns:
        tuple: 1st item is url to connect spark to database 2nd is property dict that spark can use
    """
    url = "jdbc:postgresql://{}:{}/{}".format(config["database"]["host"], config["database"]["port"], config["database"]["database"])
    dbProps = {}
    dbProps["user"] = config["database"]["username"]
    dbProps["password"] = config["database"]["password"]
    dbProps["driver"] = config["database"]["driver"]

    return (url, dbProps)
