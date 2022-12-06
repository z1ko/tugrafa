#!/bin/bash

export PYSPARK_DRIVER_PYTHON=python
export PYSPARK_PYTHON=$HOME/anaconda3/envs/tugrafa/bin/python
spark-submit \
    --packages com.datastax.spark:spark-cassandra-connector_2.12:3.2.0 \
    --archives environment.tar.gz#environment \
    --py-files path.py,pyspark_kmodes.py,Kmodes.py,graph.py \
    main.py