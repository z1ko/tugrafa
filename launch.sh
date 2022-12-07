#!/bin/bash

export PYSPARK_DRIVER_PYTHON=python3
export PYSPARK_PYTHON=/usr/bin/python3
spark-submit \
    --packages com.datastax.spark:spark-cassandra-connector_2.12:3.2.0 \
    --py-files path.py,pyspark_kmodes.py,Kmodes.py,graph.py,deviation.py \
    main.py
    
#--archives environment.tar.gz#environment \