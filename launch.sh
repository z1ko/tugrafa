#!/bin/bash
spark-submit --packages                                         \
    com.datastax.spark:spark-cassandra-connector_2.12:3.2.0,graphframes:graphframes:0.8.2-spark3.2-s_2.12               \
    main.py