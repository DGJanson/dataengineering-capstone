import configparser
import os

"""
    Some helper functions for reading config
    Throws keyerror or FileNotFoundError when something is wrong
"""

def findConfig(path):
    """
        Simply check if file is present
        Return the path so that it can be used
        I thought this function would be different, but it is basically useless now :(
    """
    if not os.path.isfile(path):
        raise FileNotFoundError("Config file does not exist")
    return(path)

def readConfOrDefault(config, dict, category, key, failIfAbsent = False, default = ""):
    """
        Method for helping read config and fill dict
        Pass it:
        config: the configparser used
        dict: the settings dict to fill
        category: the name of the section in the ini File
        key: the name of the item in the config File
        failIfAbsent: throw an error when value not filled
        default: optional default value if value not found
    """
    configValue = default
    if category in config:
        if key in config[category]:
            configValue = config[category][key]

    if configValue == "" and failIfAbsent == True:
        raise KeyError("Config option for {} - {} not set.".format(category, key))

    if category in dict:
        dict[category][key] = configValue
    else:
        dict[category] = {}
        dict[category][key] = configValue

def readSparkConfig(config, dict):
    """
        Reads and sets the various spark settings.
        Defaults to local mode.
        todo: add settings for setting up spark cluster
    """
    readConfOrDefault(config, dict, "spark", "mode", default="local")

def readDataConfig(config, dict):
    """
        Read the location of the data files
        We assume a single dir with the following folders:
        - mortality
        - nuts
        - weather
    """
    readConfOrDefault(config, dict, "data", "folder", default="./test-data")

def readDatabaseConfig(config, dict):
    """
        Read database (connection) settings
        By default we assume a local postgres install with ident authentication
    """
    readConfOrDefault(config, dict, "database", "url", default="jdbc:postgresql://localhost:5432/capstone")
    readConfOrDefault(config, dict, "database", "authentication", default="ident")

    # password settings. Validate only if password authentication is used
    if dict["database"]["authentication"] == "password":
        readConfOrDefault(config, dict, "database", "username", failIfAbsent = True)
        readConfOrDefault(config, dict, "database", "password", failIfAbsent = True)

    readConfOrDefault(config, dict, "database", "table_prefix")
    readConfOrDefault(config, dict, "database", "table_mortality", default="mortality")
    readConfOrDefault(config, dict, "database", "table_weather", default="weather")
    readConfOrDefault(config, dict, "database", "table_nuts", default="nuts")
    readConfOrDefault(config, dict, "database", "table_date", default="dates")

def readLoggingConfig(config, dict):
    """
        Read some logging options.
        todo if desired, add a debug switch option here. probably not necessary
    """
    readConfOrDefault(config, dict, "logging", "type", default = "console") # other option is file
    if dict["logging"]["type"] == "file": # we need a file when logging
        readConfOrDefault(config, dict, "logging", "logfile", failIfAbsent = True)

def readConfig(path):
    """
        Read the config file, given a certain path.
        Return a dictionary with the settings
    """
    findConfig(path)
    config = configparser.ConfigParser()
    config.read(path)

    configDict = {}

    # setup the various parts of the script by calling the appropriate methods
    readSparkConfig(config, configDict)
    readDataConfig(config, configDict)
    readDatabaseConfig(config, configDict)
    readLoggingConfig(config, configDict)

    return(configDict)