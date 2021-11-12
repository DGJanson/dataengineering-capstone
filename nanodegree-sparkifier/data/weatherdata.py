"""
Script that reads the weather data into the database
"""

import logging
import os

from pyspark.sql.types import IntegerType, FloatType

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
        tableName (string): name of the weather table in the database
        spark (SparkSession): spark session to use

    Returns:
        nothing
    """
    logger.info("Starting weather import")

    weatherFiles = findWeatherFiles(weatherFolder)
    for weatherFile in weatherFiles:
        weatherDf = spark.read.option("header", True).csv(weatherFile)

        # do not need to do much, just select the correct columns in right order
        weatherDf = weatherDf.select(weatherDf["YEAR"].alias("year").cast(IntegerType()), \
                                     weatherDf["MONTH"].alias("month").cast(IntegerType()), \
                                     weatherDf["NUTS"].alias("nuts"), \
                                     weatherDf["Mean_MAX_T"].alias("mean_maxT").cast(FloatType()), \
                                     weatherDf["Mean_MIN_T"].alias("mean_minT").cast(FloatType()), \
                                     weatherDf["Mean_AVG"].alias("mean_avgT").cast(FloatType()), \
                                     weatherDf["Mean_Pre"].alias("precipitation").cast(FloatType()), \
                                     weatherDf["Mean_snow"].alias("snow").cast(FloatType()))

        weatherDf = weatherDf.filter(weatherDf["year"].isNotNull())

        # import into the database
        weatherDf.write.jdbc(dbProps[0], tableName, properties = dbProps[1], mode = "append")

    logger.info("Succesfully read in weather data")
