import logging
import logging.config
import os
import sys

from .configReader import readConfig

"""
    Main entry point of module
    If the module is called directly, check for config file and start process
"""

logger = logging.getLogger("sparkifier")

def setupLogging(config):
    if config["logging"]["type"] == "console":
        logging.basicConfig(level = logging.INFO)
    elif config["logging"]["type"] == "file":
        logging.basicConfig(filename = config["logging"]["logfile"], filemode = "a", level = logging.INFO)
    else:
        raise ValueError("Unknown logging type in config")

if __name__ == "__main__":
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
        logger.info("Initialized, start setting up ETL job.")
    except ValueError as ve:
        logger.error("Error while configuring ETL run. Message:")
        logger.error(ve)
        sys.exit(0)
