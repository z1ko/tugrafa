#!/bin/bash
spark-submit --packages                                         \
    com.datastax.spark:spark-cassandra-connector_2.11:2.5.1     \
    main.py