from prefect import flow, task
import pandas as pd
import requests
import sqlite3

def extract():
    url = "https://raw.githubusercontent.com/mwaskom/seaborn-data/master/tips.csv"
    data = pd.read_csv(url)
    return data

def transform(data):
    data["total_bill"] = data["total_bill"] * 1.1  # Apply 10% tax
    return data

def load(data):
    conn = sqlite3.connect("etl_data.db")
    data.to_sql("tips", conn, if_exists="replace", index=False)
    conn.close()

def etl_pipeline():
    data = extract()
    transformed_data = transform(data)
    load(transformed_data)

if __name__ == "__main__":
    etl_pipeline()
