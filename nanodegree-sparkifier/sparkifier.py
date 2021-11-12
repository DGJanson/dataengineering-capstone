"""
This script orchestrates all the work. It calls the required functions in the right
order, hopefully.
"""

import logging
import sys

from .database.connection import createConnection, performQueryNoResult, performQueryWithOneResult
from .database.queries import getTableNames, createDropQueries, createCreateQueries,createDataQualityQueries

from .data.converter import startConversion

from .spark.session import createSparkSession

logger = logging.getLogger("sparkifier")

def setupDatabase(config):
    """
    Setup database. Make connection and create / drop tables if necessary.
    Args:
        config (dict): the config with the settings
    """
    try:
        connection = createConnection(config)
        logger.info("Database connection setup with: {}".format(connection.get_dsn_parameters()))
    except ValueError as err:
        logger.error("Could not connect to database. Exiting")
        sys.exit(0)

    listOfTableNames = getTableNames(config)

    if config["database"]["dropTablesFirst"] == "true":
        logger.info("Dropping existing tables if they exist")
        try:
            for query in createDropQueries(listOfTableNames):
                performQueryNoResult(connection, query)
            connection.commit()
            logger.info("Succesfully dropped tables")
        except ValueError as ve:
            logger.error("Problem while dropping tables. See logging for more details.")

    if config["database"]["createTablesFirst"] == "true":
        logger.info("Creating tables if they do not exist")
        try:
            for query in createCreateQueries(listOfTableNames):
                performQueryNoResult(connection, query)
            connection.commit()
            logger.info("Succesfully created tables")
        except ValueError as ve:
            logger.error("Problem while creating tables. See logging for more details.")

    # always close the connection :)
    connection.close()

def doQualityControlQueries(config):
    """
    Perform queries to check if data is read into the database.
    Use the database connection as defined in config, not with spark.

    Args:
        config (dict): the config with the settings

    Returns:
        nothing, but will leave some strongly worded log messages if quality is not as expected
    """
    try:
        connection = createConnection(config)
        logger.info("Database connection setup with: {}".format(connection.get_dsn_parameters()))
    except ValueError as err:
        logger.error("Could not connect to database for quality control.")
        return

    listOfTableNames = getTableNames(config)

    for (query, expectedResult) in createDataQualityQueries(listOfTableNames):
        try:
            actualResult = performQueryWithOneResult(connection, query)
            if expectedResult > actualResult[0]: # if expected is bigger, it means there is not as much as we expected. So error it is
                logger.warning("Query {} did not return expected result {}".format(query, expectedResult))
        except ValueError as ve:
            logger.error("Problem while performing quality control queries.")

def initConversion(config):
    """
    Bootstraps (if that is the right word) the conversion.
    Does not do anything by itself, but calls the appropriate methods in the right order

    Args:
        config (dict): dict with all config settings. See configreader

    Returns:
        nada: does not return anything
    """

    # database stuff
    setupDatabase(config)

    # spark stuff
    try:
        spark = createSparkSession(config)
        logger.info("Initialized Spark Session")
    except Exception as err:
        logger.error("Error while creating Spark session. Exiting program. Error:")
        logger.error(err)
        sys.exit(0)

    # start doing conversions
    startConversion(config, spark)
    logger.info("Finished all imports.")

    # clean up spark stuff
    spark.stop()

    # do some quality checking. Create the queries check results
    # do not use spark, since we only want to do simple counts
    logger.info("Starting quality control queries")
    doQualityControlQueries(config)

    logger.info("Program finished, exiting. Thank you for using this script :)")

    sys.exit(0)
