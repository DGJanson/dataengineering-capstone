"""
Script that reads the mortality data into the database
"""

import logging
import os

from pyspark.sql.types import IntegerType, StringType
from pyspark.sql.functions import arrays_zip, col, explode, length, udf

logger = logging.getLogger("sparkifier")

def findLeftSide(input):
    """
    Find the left integer of the expression, if present
    """
    dashIndex = input.find("-")
    if dashIndex == -1:
        return(None)
    elif input.startswith("Y"): # ages start with character
        return input[1:dashIndex]
    else:
        return input[0:dashIndex]

def findRightSide(input):
    """
    Find the right integer of the expression if present
    """
    dashIndex = input.find("-")
    if dashIndex == -1:
        return(None)
    else:
        return input[(dashIndex + 1):]

def findMortalityFiles(mortalityFolder):
    """
    Looks for csv files in the mortality folder

    Args:
        mortalityFolder (string): path of the folder the mortality files are stored

    Returns:
        list: list of mortality files to import
    """
    files = os.scandir(mortalityFolder)
    listToReturn = []

    for entry in files:
        if entry.is_file():
            if entry.name.endswith("xml"):
                listToReturn.append(entry.path)
    files.close()

    return(listToReturn)

def importMortality(mortalityFolder, dbProps, tableName, spark):
    """
    Read in mortality data. We assume data in xml file(s) in the folder
    Data is read into the database from this csv file

    Args:
        mortalityFolder (string): where to find weather data
        dbProps (tuple): database connection settings as provided by the getSparkDBProps method
        tableName (string): name of the mortality table in the database
        spark (SparkSession): spark session to use

    Returns:
        nothing
    """
    logger.info("Starting mortality import")

    mortalityFiles = findMortalityFiles(mortalityFolder)
    for mortalityFile in mortalityFiles:
        mortDf = spark.read \
                      .format("com.databricks.spark.xml") \
                      .option("rootTag", "data:DataSet") \
                      .option("rowTag", "data:Series") \
                      .load(mortalityFile)

        # this gives us a nested data structure, use explode function (see import) to make a row for each observation
        # but first let us select what we actually need

        # select on level 3 nuts. that means a 5 char nuts
        # use the spark sql function length
        mortDf = mortDf.filter(length(mortDf["_geo"]) == 5)
        mortDf = mortDf.filter(col("_age").rlike("\d\d?-\d\d") | col("_age").rlike("\d\d\+"))

        # now explode some stuff
        mortDf = mortDf.withColumn("zip", arrays_zip(mortDf["data:Obs"]))
        mortDf = mortDf.select(explode(mortDf["zip"]), mortDf["_age"], mortDf["_geo"], mortDf["_sex"])

        # not completely sure what is happening, but the exploded data is now 3 levels deep
        # we get a free "col" column which is a struct containing the Data:Obs struct
        # it does work though... we can reach the mortality data
        mortDf = mortDf.select(mortDf["col"]["data:Obs"]["_TIME_PERIOD"].alias("period"), \
                               mortDf["_geo"], \
                               mortDf["_sex"], \
                               mortDf["_age"], \
                               mortDf["col"]["data:Obs"]["_OBS_VALUE"].alias("mortNr"))

        # split the age and period data using udfs
        getLeftInt = udf(lambda i: findLeftSide(i), StringType())
        getRightInt = udf(lambda i: findRightSide(i), StringType())
        mortDf = mortDf.withColumn("Year", getLeftInt(mortDf["period"])) \
                       .withColumn("Week", getRightInt(mortDf["period"])) \
                       .withColumn("minAge", getLeftInt(mortDf["_age"])) \
                       .withColumn("maxAge", getRightInt(mortDf["_age"]))

        # put in right order one final time and write to database
        mortDf = mortDf.select(mortDf["Year"], \
                               mortDf["Week"], \
                               mortDf["_geo"], \
                               mortDf["_sex"], \
                               mortDf["minAge"], \
                               mortDf["maxAge"], \
                               mortDf["mortNr"])

        mortDf.write.jdbc(dbProps[0], tableName, properties = dbProps[1], mode = "overwrite")

    logger.info("Finished importing mortality data")
