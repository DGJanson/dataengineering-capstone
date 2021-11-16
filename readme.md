## Sparkifier

This is the final project for the data engineering nanodegree of Udacity: the capstone project. If this code is good enough, it will allow me to become a certified nano data engineer.

The project uses Spark to read in two big datasets and some helper data. The big datasets are on mortality and the weather. Imaginary data analysts are interested in whether the weather can be used to predict mortality, since it may be very likely the two are correlated. An interesting topic with the global pandemic going on, in my humble opinion.

#### The data

The mortality data are from the [European Data Portal](https://data.europa.eu/en), a site that collects all kinds of open data from the EU. The data are in xml format and describe the number of deaths per region, age group and gender. [Link to dataset](https://data.europa.eu/data/datasets/q0jvahp4pgzxufeucuvqw?locale=en).

The weather data are from a study which published their data: [Angelova, Denitsa; Blanco, Norman (2020), “Meteorological indicator dataset for selected European NUTS 3 regions ”, Mendeley Data, V2, doi: 10.17632/sf9x4h5jfk.2](https://data.mendeley.com/datasets/sf9x4h5jfk/2). These come in the form of several csvs, one per country.

All location data in the above sets is based on the NUTS system. A [European standard for dividing the continent into regions](https://ec.europa.eu/eurostat/web/nuts/background). We read in this standard as supporting data. The original data was in xlsx format, but I manually exported some of it to csv to make it easier to read with spark.

Finally, we have create some data on dates. Specifically, we want information on weeks, since the mortality data is on a weekly basis; the weather data is on a monthly basis. We can generate these in code; no import needed.

## The application

The application tries to utilize the powerful possibilities of [Apache Spark](https://spark.apache.org/). More specifically, pyspark, in which Spark is run via python. The main chunk of this application is the python script that imports the necessary data into a database.

This repository contains all code required to perform a test run with test data. Though a database (Postgres preferably) is required. The code in this repo can also be used to read in the complete datasets, but it still only runs a local Spark session and imports into a postgres database. At the end of this readme some recommendations are given as to how to use this script to run on a multitude of data, as required by the rubric of the capstone project. The script was made to be easily extendable to utilize a spark cluster. It requires some extra config options (see config below) to point the scipt to a spark cluster (or vice versa, probably).

The process consists of a few steps. The preparation will read the config and (re-)create the database tables if required. The biggest part of the program is a "converter" which will read in all available data in the input directories. This leverages the Spark dataframes to do the heavy lifting. Finally, some cleaning up is done and a small quality check is performed; basically if we have weather- and mortality-data in the database.

#### Python and configuration

We assume a python3 install with pip as our package manager. It is advisable to create a virtual environment in python to run this project. The repository includes a requirements file that can be used to install the necessary python packages.

The script needs a config file to run. By default the script looks for a config file in the work directory:

```python
os.getcwd() + "/conf/sparkifier.ini"
```
For anything but the most simple run of this script, you probably want to generate a custom config file. This can then be added as an argument to the python call to run the program. Speaking of, to run the application, call the module nanodegree-sparkifier. For example:

```
python3 -m nanodegree-sparkifier /path/to/custom/config.ini
```

#### The database

The project is based on a postgres database. By default we assume a postgres database on localhost using a username password combination. These options can be changed in the config file, see below.

I chose postgres, since this is the closest to RedShift (a distributed database based off a Postgres fork). I have tried to use only those Postgres properties that are also used by Redshift. However, this will probably work 100%. Therefore, at the end of this readme I have included some recommendations on how to convert this project's database to Redshift.

#### The data model

The main datamodel consists of 4 tables. These are defined in the **queries.py** script in this repo. The model is quite straight forward; all table definitions are quite close to the data in the import files. Since the datasets all use very similar data-formats, it is quite easy to join them together without elaborate tricks and data processing.

##### Mortality

The mortality table holds a year, week and region (nuts code). In addition a gender (m/f) and age group column are defined. For each combination of these columns there is a number of deaths. The original xml file contains aggregated data per age and gender, but these numbers are **not** imported. Aggregations can be made again using the database. The resulting table contains about 15,000,000 rows.

|Column     |Type      |
|-----------|----------|
|year       |integer   |
|week       |integer   |
|nuts       |varchar(5)|
|sex        |varchar(1)|
|age        |varchar(6)|
|number     |integer   |

*INDEX on (year, week, nuts)*

An overview of the mortality data has been included in the output directory. It shows for each country how which years are available and how many rows there are.

##### Weather

The weather table contains for each year, month and region (nuts) code: the mean maximum temperature, the mean minimum temperature, the mean average temperature, the mean precipitation and the mean snow (all decimal numbers). This table contains about 400,000 rows.

|Column        |Type      |
|--------------|----------|
|year          |integer   |
|month         |integer   |
|nuts          |varchar(5)|
|mean_maxT     |float     |
|mean_minT     |float     |
|mean_avgT     |float     |
|precipitation |float     |
|snow          |float     |

*PRIMARY KEY on (year, month, nuts)*

An overview of the weather data has been included in the output directory. It show for each country and year the number of measurements for that year.

##### Nuts data

Nuts, or geographical data, is based on nuts codes. Since all our data is at the lowest level, we are only interested in the lowest level nuts codes: level 3. As such, the table is a list of 5 letter (level 3) nuts codes. For each code it holds the corresponding country, level 1 (region), level 2 (province) and level 3 (area) names.

|Column        |Type      |
|--------------|----------|
|nuts_code     |char(5)   |
|country_name  |text      |
|region_name   |text      |
|province_name |text      |
|area_name     |text      |

*PRIMARY KEY on (nuts_code)*

##### Dates data

The dates data is simple: a is of dates from 2000-01-01 (that is when the mortality data starts) and for each date we have a week, month and year. We can use this table to match the weekly data of the mortality numbers to the monthly data of the weather data.

|Column        |Type      |
|--------------|----------|
|date          |date      |
|month         |integer   |
|week          |integer   |
|year          |integer   |

*PRIMARY KEY on (date)*

##### Joining the tables

The data can be connected using the nuts codes and the year, month and week columns. To connect the mortality to the weather data you need to use the dates table to convert the week and year column of the mortality table to the month and year column of the weather data. Please find an example of a query for how to do this further below.

#### The config file

Most settings for this project can be set in the config file. See above for how to pass a config file to the script. The **configreader.py** reads in the config file and sets the options for the script. Most options (except the username and password for the database) contain a default setting. If passing a file / folder location to the settings, you can use a relative path by using a dot: *./test-data*. The ./ will be replaced by the *os.getcwd* call. The config file uses the standard python ini format. See included **sparkifier.ini** file for an example.

The config file contains the following settings:

**Spark**

- url: where to connect to a sparkContext. Default: local
- appname: name of the spark app. Default: sparkifier
- postgres-jar: where to find the postgres-jar that spark needs. Included in the repo. Default: ./jars/postgresql-42.3.1.jar
- databricks-xml-jar: where to find the databricks-xml-jar that spark needs. Included in the repo. Default: ./jars/spark-xml_2.12-0.14.0.jar

**Data**

- folder: where to look for the data files to import. The folder should contain 3 subfolders: mortality, weather and nuts. Default: ./test-data
- outputfolder: where to output the overview and example data of the outputter. Default: ./output

**Database**

- host: where to look for database. Default: localhost
- port: which port does database use. Default: 5432
- database: name of database to use. Default: capstone
- username: username to use in connection with database. No default
- password: password to use in connection with database. No default
- driver: driver (for spark) to use to interact with database. Default: org.postgresql.Driver.
- dropTablesFirst: whether to clear the database before importing. Best not do this after reading in the large mortality dataset. Default: true
- createTablesFirst: whether to create the database before importing. Default: true
- table_prefix: whether to prepend something to all tablenames. You can use this to for example create a set of test tables and regular tables in a single database. Default: ""
- table_mortality: tablename for mortality table. Default: mortality
- table_weather: tablename for weather table. Default: weather
- table_nuts: tablename for nuts table. Default: nuts
- table_dates: tablename for dates table. Default: dates

**Logging**

- type: what kind of logging to use. Options are console and file. Default: console
- logfile: if file logging type selected, we need a file to write log to. Will log to single file in append mode. Pass a path to file. No default.

## Quality checks

There are some checks in the data for quality control. First, all tables except the mortality data contain a primary key to make sure data is only read once. Furthermore, after importing the data, two check queries are performed. The first simply checks if some weather data has been imported into the database. The other counts the number of age categories in the mortality data; we did not import the aggregated data, so we only expect 10 different age categories. Not more and not less. Finally, the outputter creates some overviews that can be checked manually. For example, the weather data contains the same number of measurements per year per country. And all these numbers are multiples of 12, implying all months were imported.

## How to use for analysis

To use this set for analysis the mortality and weather tables need to be joined. We can only do this with the dates table, since mortality is on weekly basis and weather is on monthly basis. If geographical selection is required, a join with the nuts table is also required. A sample query will follow below.

The tables are now mostly set up with the time component as indeces. This implies that analyses also will follow the time dimension. If the geographical component is more important, then it may be worthwhile to add / replace indeces on nuts columns as well.

Say we want to investigate the effect of the weather in rainy Holland on mortality in the years 2015 - 2018 (there is not a lot of dutch data in the set). We are not interested in gender or age, only general trends. Use the following query to get the appropriate data:

```sql
WITH weekmonths AS( -- Select a single week for each month
  SELECT week, month, year
  FROM (
    SELECT week, month, year, ROW_NUMBER() OVER (PARTITION BY year, week ORDER BY month) AS rij
    FROM dates
  ) AS sq
  WHERE sq.rij = 1
), mortalityFull AS (
  SELECT mortality.*, weekmonths.month
  FROM mortality JOIN weekmonths ON mortality.year = weekmonths.year AND mortality.week = weekmonths.week AND mortality.year >= 2015 AND mortality.year < 2019
  JOIN nuts ON mortality.nuts = nuts.nuts_code AND nuts.country_name = 'Nederland'
), mortalityAgg AS (
  SELECT year, month, week, nuts, sum(number) AS totalMort
  FROM mortalityFull
  GROUP BY year, month, week, nuts
)
SELECT mortalityAgg.*, weather.mean_maxT, weather.mean_minT, weather.mean_avgT, weather.precipitation,  weather.snow
FROM mortalityAgg JOIN weather ON mortalityAgg.year = weather.year AND mortalityAgg.month = weather.month AND mortalityAgg.nuts = weather.nuts;
```

The output of this query is included in the output directory of this repo.

## Scaling up

Say we have a lot more of data to add to the database, 100x more according to the rubric. This would mean that we need to migrate to a cloud environment. First off, Spark would need to run on a strong cluster, since a single laptop takes quite a while to even import the regular dataset apparently. The script should be adapted to run on a clustered instance, but hopefully that is quite easy to achieve.

The harder part will be to change the database. I think a distributed, columnar storage database is required, like Redshift. The data lends itself quite well to this format. The queries in this repo can be used to setup Redshift as well. An important decision however is whether it is desired to partition the data by time or by region (nuts-code). This depends on the type of analyses the data are used for. If the data are split by region in the analyses, so for example analysing all France data over the years, then the nuts data should be the most significant part of the partition key. If the analyses are more temporal, for example analysing all 2012 data in all regions, then the time data should be the most significant part of the partition key.

If the above is clear the tables can be created in Redshift. The nuts and dates tables are small and should be stored on every shard of the database. The weather and mortality data can be partitioned as described above, though care should be taken to include gender and age in the partition key for mortality.

To ensure availability in case of hundreds of users on this database I would advise to create multiple clusters. Depending on the type of analysis or application for the data different specs can be chosen for each cluster. Database-wise, Amazon has several options for creating replicas of the database, to ensure a database will be available for all users. In case of users from outside of Europe are interested in this data, different availability zones can be created. This will create a standby database in a different availability zones, which in turn can have replicas as well. The different clusters can also be deployed in different availability zones.

To keep the ETL job able to run quickly, several improvements can be made:

- The job can be cut in several parts. The imports of the different sources do not depend on each other, so they can be performed separately on different clusters.
- The data can be cut in parts as well. It is not necessary to read in data twice, so a mechanism could be created to keep track of what data was imported already. Redshift does not support upserting directly, but several methods can be used. For example, you can keep track of the last date on which data was imported, and ignore all data before that date. Or you can keep track of which files were imported, for example by saving a list of imported files in S3, and only import files that were not previously imported.

To make sure the pipelines are run on a daily basis an orchestration tool is needed. This can be Apache Airflow or a combination of Amazon options (assuming we are using Redshift by this time): Cloudwatch, Step functions, AWS Lambda and AWS batch. The main function of the orchestration tool is to schedule the import scripts and monitor their progress; so that they can be run daily before 7 am for example. If more efficiency is required, the above suggestions on how to cut the data into parts need to be combined with the orchestration tool. That would allow for more precise scoping of (and thus reducing) the data needed in the run and parallelize the execution of the different parts of the script. This is something that both tools can do.

## Known issues

The quality checks are very basic. It is basically a quick check to see if data is present at all. A more dynamic process is preferable. Perhaps we can generate quality check queries based on the dataframe data from Spark?

The dates table is not ideal. The goal is to match a given week in a year to a month, but with the current data model this requires a few extra steps. See the above weekmonths part of the query. I should replace the current table with the weekmonths table above.

There is no central fact table or star model now. I think that due to the number of questions that can be asked of the data, a single large table may not be required? A setup with several datamarts based on the 4 tables we have now seems like a good option.
