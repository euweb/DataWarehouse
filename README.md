# DataWarehouse

This project allows Sparkify employees to get analytics from the usage of their app. Since the data is stored in JSON files there is no easy way to query their data.
For this purpose a ETL pipeline should be developed to extract the data from AWS S3 and load it in Redshift data warehouse.

## AWS Architecture

![ER Diagram](https://github.com/euweb/DataWarehouse/blob/master/DataWarehouseAWS.png?raw=true)

## Database schema and ER Diagram

We use a star schema which is optimized for queries on the song play analysis. For the data consistency we define constraints such as foreign keys or not null values. We use python programming language to implement the automated AWS Redshift Cluster and database creation and the running of ETL process / pipeline. There are two types of JSON files containing song data (artist and song data) and song play data (song plays and user information). We use jupyter lab for querying our data and plot diagrams.

**Staging tables:**

![ER Diagram Staging Tables](https://github.com/euweb/DataWarehouse/blob/master/DataWarehouseERStaging.png?raw=true)

**Fact and dimension tables:**

![ER Diagram](https://github.com/euweb/DataWarehouse/blob/master/DataWarehouseER.png?raw=true)

## Running

1. clone this repository
2. copy dwh.cfg.example to dwh.cfg and set all values
3. create virtual environment running `python -m venv .venv` and active it `source .venv/bin/activate`
4. install python dependencies `pip install -f requirements.txt`
5. create redshift cluster `python redshift_util.py -c`
6. create tables `python create_tables.py` 
7. import data `python etl.py` 
8. use Dashboard.ipynb for querying the database
9. delete redshift cluster `python redshift_util.py -d`

## File list

| Name                      	| Description                                                    	|
|---------------------------	|----------------------------------------------------------------	|
| create_tables.py          	| (re)creates tables in the database                             	|
| Dashboard.ipynb           	| contains simple example for querying the database               	|
| dwh.cfg.example              	| example configuration file                                      	|
| etl.py                      	| etl pipeline                                                   	|
| README.md                 	| this documentation                                              	|
| redshift_util.py 	            | python script for creating and deleting redshift cluster         	|
| requirements.txt 	            | list with necessary python modules                              	|
| sql_queries.py            	| python file with sql queries for create tables and etl scripts 	|
