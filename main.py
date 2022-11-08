
from pprint import pprint
from dotenv import dotenv_values

from pyspark.sql import SparkSession


if __name__ == '__main__':

    config = dotenv_values("config.env")
    print("Loaded configuration:")
    pprint(config)

    session = SparkSession.builder                      \
        .master(config['TUGRAFA_SPARK_HOST'])           \
        .appName(config['TUGRAFA_SPARK_APP_NAME'])      \
        .getOrCreate()

    pprint(session)


