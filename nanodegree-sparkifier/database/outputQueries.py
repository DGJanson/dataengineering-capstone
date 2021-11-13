"""
Collection of output queries so we can investigate the data a bit
"""

def createOverviewWeather(listOfTableNames):
    query = """
        SELECT nu.country_name, we.year, COUNT(*) AS number_of_measurements
        FROM {} AS we LEFT JOIN {} AS nu ON we.nuts = nu.nuts_code
        GROUP BY nu.country_name, we.year
    """.format(listOfTableNames[1], listOfTableNames[2])

    return(query)

def createOverviewMortality(listOfTableNames):
    query = """
        SELECT nu.country_name, MIN(mo.year) AS earliest_year, MAX(mo.year) AS last_year, COUNT(*) AS number_of_measurements
        FROM {} AS mo LEFT JOIN {} AS nu on mo.nuts = nu.nuts_code
        GROUP BY nu.country_name
    """.format(listOfTableNames[0], listOfTableNames[2])

    return(query)

def createDutchExampleQuery(listOfTableNames):
    query = """
        WITH weekmonths AS( -- Select a single week for each month
          SELECT week, month, year
          FROM (
            SELECT week, month, year, ROW_NUMBER() OVER (PARTITION BY year, week ORDER BY month) AS rij
            FROM {}
          ) AS sq
          WHERE sq.rij = 1
        ), mortalityFull AS (
          SELECT mortality.*, weekmonths.month
          FROM {} AS mortality JOIN weekmonths ON mortality.year = weekmonths.year AND mortality.week = weekmonths.week AND mortality.year >= 2015 AND mortality.year < 2019
          JOIN nuts ON mortality.nuts = nuts.nuts_code AND nuts.country_name = 'Nederland'
        ), mortalityAgg AS (
          SELECT year, month, week, nuts, sum(number) AS totalMort
          FROM mortalityFull
          GROUP BY year, month, week, nuts
        )
        SELECT mortalityAgg.*, weather.mean_maxT, weather.mean_minT, weather.mean_avgT, weather.precipitation,  weather.snow
        FROM mortalityAgg JOIN {} AS weather ON mortalityAgg.year = weather.year AND mortalityAgg.month = weather.month AND mortalityAgg.nuts = weather.nuts
    """.format(listOfTableNames[3], listOfTableNames[0], listOfTableNames[1])

    return(query)
