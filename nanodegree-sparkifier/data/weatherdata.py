"""
Script that reads the weather data into the database
"""

import logging
import os

logger = logging.getLogger("sparkifier")

def findWeatherFiles(weatherFolder):
    """
    Looks for csv files in the weather folder`

    Args:
        weatherFolder (string): path of the folder the weather files are stored

    Returns:
        list: list of weather files to import
    """
    files = os.scandir(weatherFolder)
    listToReturn = []

    for entry in files:
        if entry.is_file():
            if entry.name.endswith("csv"):
                listToReturn.append(entry.path)
    files.close()

    return(listToReturn)


def importWeather(weatherFolder, dbProps, tableName, spark):
    """
    Read in weather data. We assume data in csv file(s) in the folder
    Data is read into the database from this csv file

    Args:
        weatherFolder (string): where to find weather data
        dbProps (tuple): database connection settings as provided by the getSparkDBProps method
        tableName (string): name of the nuts table in the database
        spark (SparkSession): spark session to use

    Returns:
        nothing
    """
    logger.info("Starting weather import")

    weatherFiles = findWeatherFiles(weatherFolder)
    for weatherFile in weatherFiles:
        weatherDf = spark.read.option("header", True).csv(weatherFile)

        # do not need to do much, just select the correct columns in right order
        weatherDf = weatherDf.select(weatherDf["YEAR"], \
                                     weatherDf["MONTH"], \
                                     weatherDf["NUTS"], \
                                     weatherDf["Mean_MAX_T"], \
                                     weatherDf["Mean_MIN_T"], \
                                     weatherDf["Mean_AVG"], \
                                     weatherDf["Mean_Pre"], \
                                     weatherDf["Mean_snow"])

        # import into the database
        weatherDf.write.jdbc(dbProps[0], tableName, properties = dbProps[1], mode = "overwrite")

    logger.info("Succesfully read in weather data")
