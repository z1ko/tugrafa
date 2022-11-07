# Tugrafa

A simple project aimed at analyzing the VeronaCard dataset to extract common tourists' itineraries.

## Single machine setup

To deploy the application on a single machine use this command:
```
docker compose up -d
```
This will pull all the necessary docker containers and create the tugrafa network.
Note that in older docker installations the correct command is ```docker-compose```.

## Run the notebooks

To run the notebooks execute the ```notebook.sh``` bash script after the initial setup, this will attach a PySpark enviroment and map the entire ```notebook/``` folder, allowing you to easily access the dataset and issue jobs to the Spark cluster. To open the notebook just click on the link in the output.

## Procedure

The process is subdiveded in two main phases, the offline loading and the online queries.
In the first phase we do the following:
- Remove unnecessary data from the CSV, we keep only the card id, the timestamp(date and time) and the POI id.
- Load the CSV data into a NoSql database (Cassandra)

In the second phase:
- Generate a base RDD from the NoSql data, usind a Cassandra connector
- Count and aggregate the number of paths between distinct POIs in a count RDD [(poi_1, poi_2) | num_paths]
- Map the count RDD into a GraphX graph rappresentation with vertices and edges RDDs
- Query the graph