#!/bin/bash

# ========================================================================================
# Install python and java dependencies

#sudo apt-get install python3.8 default-jdk -y
#pip install pip

# ========================================================================================
# Install Cassandra

wget -q -O - https://www.apache.org/dist/cassandra/KEYS | sudo apt-key add -

CASSANDRA_SOURCE_VAL="deb http://www.apache.org/dist/cassandra/debian 40x main"
CASSANDRA_SOURCE_APT="/etc/apt/sources.list.d/cassandra.sources.list"

if [ ! -f $CASSANDRA_SOURCE_APT ]; then
    echo "CASSANDRA NOT APT FILE FOUND!"
    sudo touch $CASSANDRA_SOURCE_APT
fi

if ! grep -q $CASSANDRA_SOURCE_VAL $CASSANDRA_SOURCE_APT; then
    echo "CASSANDRA NOT PRESENT IN APT SOURCES!"
    echo $CASSANDRA_SOURCE_VAL | sudo tee -a $CASSANDRA_SOURCE_APT $CASSANDRA_SOURCE_VAL
fi

sudo apt-get update
sudo apt-get install cassandra -y

# ========================================================================================
# Create Cassandra service

sudo cp ./config/cassandra/cassandra.yml /etc/cassandra/cassandra.yml

sudo systemctl enable cassandra
sudo systemctl start  cassandra

# ========================================================================================
# Setup keyspace and tables and load CSV data

if cqlsh -f ./config/cassandra/setup.cql; then
    echo "CASSANDRA SETUP COMPLETE!"
else
    echo "CASSANDRA SETUP FAILED!"
    exit 1
fi

#cqlsh -f ./config/cassandra/load.cql

# ========================================================================================
# Install Spark

SPARK_DOWNLOAD_URL="https://dlcdn.apache.org/spark/spark-3.3.1/spark-3.3.1-bin-hadoop3.tgz"
wget SPARK_DOWNLOAD_URL

tar xvf spark-3.3.1-bin-hadoop3.tgz
sudo mv spark-3.3.1-bin-hadoop3 /opt/spark
rm spark-3.3.1-bin-hadoop3.tgz

echo "export SPARK_HOME=/opt/spark" >> ~/.profile
echo "export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin" >> ~/.profile
echo "export PYSPARK_PYTHON=/usr/bin/python3" >> ~/.profile

source ~/.profile