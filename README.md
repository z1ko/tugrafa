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