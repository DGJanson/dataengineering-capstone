"""
File that handles creating the sesion and handling data
"""

from pyspark.conf import SparkConf
from pyspark.sql import SparkSession

def createSparkSession(config):
    """
    Creates the sparksession based on some config settings

    Args:
        config (dict): a dictionary with some config settings

    Returns:
        SparkSession: the created spark session
    """

    # TODO add additional config settings as required
    # add the conf method of the builder for

    spark = SparkSession.builder \
        .master(config["spark"]["url"]) \
        .appName(config["spark"]["appname"]) \
        .getOrCreate()

    return(spark)
