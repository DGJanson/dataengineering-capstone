"""
    This class outputs some queries to show some data
    It can be called seperately from the main program, so it can be used on existing database
"""

import logging
import logging.config
import os
import sys

from .configReader import readConfig
from .database.queries import getTableNames
from .database.connection import createConnection, performQueryWriteToCsv
from .database.outputQueries import createOverviewWeather, createOverviewMortality, createDutchExampleQuery


logger = logging.getLogger("sparkifier")

def outputData(config):
    """
    Output some data on the database

    Args:
        config (dict): the config file as read by the configreader. Can use the same as for the conversion
    """
    logger.info("Starting output.")

    try:
        listOfTableNames = getTableNames(config)
        conn = createConnection(config)

        outputFolder = config["data"]["outputfolder"]

        performQueryWriteToCsv(conn, createOverviewWeather(listOfTableNames), os.path.join(outputFolder, "overviewWeather.csv"))
        logger.info("Outputted overviewWeather")

        performQueryWriteToCsv(conn, createOverviewMortality(listOfTableNames), os.path.join(outputFolder, "overviewMortality.csv"))
        logger.info("Outputted overviewMortality")

        performQueryWriteToCsv(conn, createDutchExampleQuery(listOfTableNames), os.path.join(outputFolder, "dutchExample.csv"))
        logger.info("Outputted dutch example query")

        logger.info("Finished all output queries.")
    except ValueError as ve:
        logger.error("Error while performing an output query. Exiting")
        sys.exit(0)
    finally:
        conn.close()

def setupLogging(config):
    if config["logging"]["type"] == "console":
        logging.basicConfig(level = logging.INFO)
    elif config["logging"]["type"] == "file":
        logging.basicConfig(filename = config["logging"]["logfile"], filemode = "a", level = logging.INFO)
    else:
        raise ValueError("Unknown logging type in config")

if __name__ == '__main__':
    try:
        # look for env args. A config file should be passed. If not look for file in current wd
        if len(sys.argv) == 2:
            configPath = sys.argv[1]
        else:
            configPath = os.path.join(os.getcwd(), "conf", "sparkifier.ini")

        # read config file
        config = readConfig(configPath)
    except FileNotFoundError as fnfe:
         logger.error("Exception while looking for config file. File not found. Error message:")
         logger.error(fnfe)
         sys.exit(0)
    except KeyError as err:
        logger.error("Exception while parsing config. Please check file. Error message:")
        logger.error(err)
        sys.exit(0)
    except Exception as err:
        logger.error("Generic exception while doing something with config. Error message:")
        logger.error(err)
        sys.exit(0)

    # use config to config logging and start etl job
    try:
        setupLogging(config)
        logger.info("Initialized, starting output")
    except ValueError as ve:
        logger.error("Error while configuring ETL run. Message:")
        logger.error(ve)
        sys.exit(0)

    try:
        outputData(config)
    except Exception as err:
        logger.error("Unknown error during conversion. This should not happen actually. Good luck debugging. Message:")
        logger.error(err)
        sys.exit(0)
