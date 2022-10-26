#!/bin/bash
# This file attaches a jupyter pyspark notebook to the tugrafa network, mapping the notebook/ folder
# and thus allowing the user to launch pyspark applications.

# Checks if the tugrafa network is alive and running 
# TODO

docker run --rm \
    -p 8888:8888 \
    --network=tugrafa_net \
    -v "${PWD}"/notebook:/home/jovyan/notebook \
    jupyter/pyspark-notebook