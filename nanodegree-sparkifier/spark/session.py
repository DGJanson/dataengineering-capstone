"""
File that handles creating the sesion and handling data
"""

import os

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

    postgresJarPath = config["spark"]["postgres-jar"]
    if postgresJarPath.startswith(r"./"):
        postgresJarPath = os.path.join(os.getcwd(), postgresJarPath)

    databricksXmlJarPath = config["spark"]["databricks-xml-jar"]
    if databricksXmlJarPath.startswith(r"./"):
        databricksXmlJarPath = os.path.join(os.getcwd(), databricksXmlJarPath)

    jars = postgresJarPath + "," + databricksXmlJarPath

    spark = SparkSession.builder \
        .master(config["spark"]["url"]) \
        .appName(config["spark"]["appname"]) \
        .config("spark.jars", jars) \
        .getOrCreate()

    return(spark)
