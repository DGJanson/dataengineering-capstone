"""
Script that reads nuts data into the database
"""

import logging
import os

from pyspark.sql.types import StringType
from pyspark.sql.functions import udf

logger = logging.getLogger("sparkifier")

def findNutsFiles(nutsFolder):
    files = os.scandir(nutsFolder)
    listToReturn = []

    for entry in files:
        if entry.is_file():
            if entry.name.endswith("csv"):
                listToReturn.append(entry.path)
    files.close()

    return(listToReturn)

def importNuts(nutsFolder, dbProps, tableName, spark):
    """
    Read in nuts data. We assume data in csv file(s) in the folder
    Data is read into the database from this csv file

    Args:
        nutsFolder (string): where to find nuts data
        dbProps (tuple): database connection settings as provided by the getSparkDBProps method
        tableName (string): name of the nuts table in the database
        spark (SparkSession): spark session to use

    Returns:
        nothing
    """

    logger.info("Starting nuts import")

    nutsFiles = findNutsFiles(nutsFolder)
    for nutsFile in nutsFiles:
        nutsDf = spark.read.option("header", True).csv(nutsFile)

        # We need to reshuffle this data a bit to fit it into the table
        # the country and regional data (higher level) are on seperate columns now,
        # but we want to condense this data.
        # One row per 3 level code, with country, level1 and level2 code for EACH row

        # First seperate the data for each level and then put them together again
        countriesDf = nutsDf.filter(nutsDf["Country"].isNotNull()) \
            .select(nutsDf["Code 2021"], nutsDf["Country"]) \
            .withColumnRenamed("Code 2021", "CountryCode")

        level1Df = nutsDf.filter(nutsDf["Nuts level 1"].isNotNull()) \
            .select(nutsDf["Code 2021"], nutsDf["Nuts Level 1"]) \
            .withColumnRenamed("Code 2021", "LevelCode")

        level2Df = nutsDf.filter(nutsDf["Nuts level 2"].isNotNull()) \
            .select(nutsDf["Code 2021"], nutsDf["Nuts Level 2"]) \
            .withColumnRenamed("Code 2021", "LevelCode")

        nutsDf = nutsDf.select(nutsDf["Code 2021"], nutsDf["Nuts Level 3"])
        nutsDf = nutsDf.filter(nutsDf["Nuts level 3"].isNotNull())

        # we want to match on part of the key. According to SO we need a lambda to substring the Code
        # https://stackoverflow.com/questions/43583623/pyspark-joining-with-part-of-key
        # I suspect there may be a neater solution for this... but I cannot get the substring to work in the join function itself

        substringCountryUdf = udf(lambda code : code[0:2], StringType())
        nutsDf = nutsDf.withColumn("country_code", substringCountryUdf(nutsDf["Code 2021"]))
        nutsDf = nutsDf.join(countriesDf, nutsDf["country_code"] == countriesDf["CountryCode"]) \
            .select(nutsDf["Code 2021"], countriesDf["Country"], nutsDf["Nuts level 3"])

        substringLevelOneUdf = udf(lambda code : code[0:3], StringType())
        nutsDf = nutsDf.withColumn("level_code", substringLevelOneUdf(nutsDf["Code 2021"]))
        nutsDf = nutsDf.join(level1Df, nutsDf["level_code"] == level1Df["LevelCode"]) \
            .select(nutsDf["Code 2021"], nutsDf["Country"], level1Df["Nuts Level 1"], nutsDf["Nuts level 3"])

        substringLevelTwoUdf = udf(lambda code : code[0:4], StringType())
        nutsDf = nutsDf.withColumn("level_code", substringLevelTwoUdf(nutsDf["Code 2021"]))
        nutsDf = nutsDf.join(level2Df, nutsDf["level_code"] == level2Df["LevelCode"]) \
            .select(nutsDf["Code 2021"].alias("nuts_code"), \
                    nutsDf["Country"].alias("country_name"), \
                    nutsDf["Nuts Level 1"].alias("region_name"), \
                    level2Df["Nuts Level 2"].alias("province_name"), \
                    nutsDf["Nuts level 3"].alias("area_name"))

        logger.info(nutsDf.show(10))

        nutsDf.write.jdbc(dbProps[0], tableName, properties = dbProps[1], mode = "append")

    logger.info("Imported nuts data")
