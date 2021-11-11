"""
Script that generates dates data
No import files needed, just generate the dates
"""

from datetime import date, timedelta

import logging

logger = logging.getLogger("sparkifier")

def importDates(dbProps, tableName, spark):
    """
    Read in ndates data

    Args:
        dbProps (tuple): database connection settings as provided by the getSparkDBProps method
        tableName (string): name of the nuts table in the database
        spark (SparkSession): spark session to use

    Returns:
        nothing
    """
    logger.info("Start generating dates")

    # this is a bit sloppy but just use python to generate a list of date tuples
    # and read into database
    # in grand scheme of things this is a very simple operation
    base = date(2000, 1, 1)
    date_list = [base + timedelta(days = x) for x in range(11000)] # ~ 3 years

    listOfTuples = []
    for datum in date_list:
        dateTuple = (datum, datum.month, datum.isocalendar()[1], datum.year)
        listOfTuples.append(dateTuple)

    dateDf = spark.createDataFrame(data = listOfTuples)

    dateDf.write.jdbc(dbProps[0], tableName, properties = dbProps[1], mode = "overwrite")

    logger.info("Finished importing dates")
