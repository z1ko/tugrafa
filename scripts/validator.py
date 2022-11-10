#
# Load all CSV files and validates the content patching it if necessary
#

import pandas as pd
from pprint import pprint
import datetime

# Header of the CSV files
CSV_HEADER = ["date", "time", "poi", "u1", "card_id", "activation", "u2", "u3", "duration"]


# Convert a (date, time) pair to a valid iso8610 datetime
def convert_to_iso8610(date: str, time: str) -> str:
    dt = datetime.datetime.strptime(date, "%d-%m-%y")
    return dt.strftime("%Y-%m-%d") + " " + time


# ============================================================================================
# SCRIPT

df = pd.read_csv("./data/dati_2014.csv", names=CSV_HEADER)
df["datetime"] = df.apply(lambda x: convert_to_iso8610(x["date"], x["time"]), axis=1)
print(df)

df.to_csv("./data/df_dati_2014.csv", header=True, index=False, columns=["card_id", "datetime", "poi"])