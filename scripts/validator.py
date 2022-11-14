#
# Load all CSV files and validates the content patching it if necessary
#

import pandas as pd
from pprint import pprint
import datetime
import os

# Data folder where to search for the CSVs
DATA_FOLDER = "./data"

# Header of the CSV files
CSV_HEADER_INITIAL = ["date", "time", "poi", "u1", "card_id", "activation", "u2", "u3", "duration"]
CSV_HEADER_FINAL   = ["card_id", "datetime", "poi"]

# Convert a (date, time) pair to a valid iso8610 datetime
def convert_to_iso8610(date: str, time: str) -> str:
    dt = datetime.datetime.strptime(date, "%d-%m-%y")
    return dt.strftime("%Y-%m-%d") + " " + time


# ============================================================================================
# SCRIPT

for filename in os.listdir(DATA_FOLDER):
    if filename.startswith("processed"):
        continue

    f = os.path.join(DATA_FOLDER, filename)
    print(f"Processing file: {f}")

    df = pd.read_csv(f, names=CSV_HEADER_INITIAL)
    df["datetime"] = df.apply(lambda x: convert_to_iso8610(x["date"], x["time"]), axis=1)
    print(df)

    output_filename = "processed_" + filename
    df.to_csv(f"./data/{output_filename}", header=True, index=False, columns=CSV_HEADER_FINAL)