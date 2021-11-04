"""
This script orchestrates all the work. It calls the required functions in the right
order, hopefully.
"""

import logging

from .database.connection import createConnection
from .database.queries import getTableNames, createDropQueries, createCreateQueries

logger = logging.getLogger("sparkifier")

def setupDatabase(config):
    """
    Setup database. Make connection and create / drop tables if necessary.
    Args:
        config (dict): the config with the settings
    """
    connection = createConnection(config)
    logger.info(connection.get_dsn_parameters())

    connection.close()
    listOfTableNames = getTableNames(config)
    print(createDropQueries(listOfTableNames))
    print(createCreateQueries(listOfTableNames))


def initConversion(config):
    """
    Bootstraps (if that is the right word) the conversion.
    Does not do anything by itself, but calls the appropriate methods in the right order

    Args:
        config (dict): dict with all config settings. See configreader

    Returns:
        nada: does not return anything
    """

    # database stuff
    setupDatabase(config)
