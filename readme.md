## Sparkifier

This is the final project for the data engineering nanodegree of Udacity: the capstone project. If this code is good enough, it will allow me to become a certified nano data engineer.

The project uses Spark to read in two big datasets and some helper data. The big datasets are on mortality and the weather. Imaginary data analysts are interested in whether the weather can be used to predict mortality, since it may be very likely the two are correlated. An interesting topic with the global pandemic going on, in my humble opinion.

#### The data

The mortality data are from the [European Data Portal]{https://data.europa.eu/en}, a site that collects all kinds of open data from the EU. The data are in xml format and describe the number of deaths per region, age group and gender.

The weather data are from a study which published their data: [Angelova, Denitsa; Blanco, Norman (2020), “Meteorological indicator dataset for selected European NUTS 3 regions ”, Mendeley Data, V2, doi: 10.17632/sf9x4h5jfk.2]{https://data.mendeley.com/datasets/sf9x4h5jfk/2}. These come in the form of several csvs, one per country.

All location data in the above sets is based on the NUTS system. A [European standard for dividing the continent into regions]{https://ec.europa.eu/eurostat/web/nuts/background}. We read in this standard as supporting data.

Finally, we have create some data on dates. Specifically, we want information on weeks, since the mortality data is on a weekly basis; the weather data is on a monthly basis.

## The application

The application tries to utilize the powerful possibilities of [Apache Spark]{https://spark.apache.org/}. More specifically, pyspark, in which Spark is run via python. The main chunk of this application is the python script that imports the necessary data into a database.

This repository contains all code required to perform a test run with test data. Though a database (Postgres preferably) is required. The code in this repo can also be used to read in the complete datasets, but it still only runs a local Spark session and imports into a postgres database. At the end of this readme some recommendations are given as to how to use this script to run on a multitude of data, as required by the rubric of the capstone project. In theory the script is able to run fully on a clustered Spark session and a distributed database like RedShift from Amazon. Though this does require some configuration by the user of this project.

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

The project is based on a postgres database. By default we assume a postgres database on localhost using ident authentication (the default install basically). These options can be changed in the config file, see below.

I chose postgres, since this is the closest to RedShift (a distributed database based off a Postgres fork). I have tried to only those Postgres properties that are also used by Redshift. However, this will never work 100%. Therefore, at the end of this readme I have included some recommendations on how to convert this project's database to Redshift.

#### The data model

TODO: describe the data model

#### The config file

TODO: explain the different config options

## Scaling up

TODO: recommendations on how to use this project for datasets with 1000x more data -> distributed database and clusted Spark instances.
