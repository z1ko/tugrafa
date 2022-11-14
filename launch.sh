#!/bin/bash
spark-submit --packages                                         \
    com.datastax.spark:spark-cassandra-connector_2.12:3.2.0     \
    main.py