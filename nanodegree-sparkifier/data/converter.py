"""
Entry point for conversion stuff
Manages the different conversion steps by calling the appropriate methods
"""

import logging
import os

from ..database.queries import getTableNames
from ..database.connection import getSparkDBProps

from .nutsdata import importNuts



logger = logging.getLogger("sparkifier")

def findFiles(config):
    """
    Looks for the data file directories

    Args:
        config (dict): config file with all settings needed for this conversion

    Returns:
        tuple: of the directories, if found, in this order: mortality, weather, nuts
    """
    fileLocation = config["data"]["folder"]
    if fileLocation.startswith(r"./"):
        fileLocation = os.path.join(os.getcwd(), fileLocation)

    if not os.path.isdir(fileLocation):
        raise FileNotFoundError("Could not find data directory")

    tupleToReturn = (os.path.join(fileLocation, "mortality"), \
                     os.path.join(fileLocation, "weather"), \
                     os.path.join(fileLocation, "nuts"))

    for fileDir in tupleToReturn:
        if not os.path.isdir(fileDir):
            raise FileNotFoundError("Could not find data file directory {}".format(fileDir))
    return(tupleToReturn)



def startConversion(config, spark):
    """
    Starts the conversion.

    Args:
        config (dict): the configdict with all settings needed for this conversions
        spark (SparkSession): the spark session to use in the conversion

    Returns:
        nada
    """
    try:
        fileDirs = findFiles(config)
    except FileNotFoundError as fnfe:
        logger.error("Could not find input data files. Exiting.")
        return

    # get info for database
    databasePropsForSpark = getSparkDBProps(config)
    tableNames = getTableNames(config)

    # mortality data

    # weather data

    # nuts data
    importNuts(fileDirs[2], databasePropsForSpark, tableNames[2], spark)
