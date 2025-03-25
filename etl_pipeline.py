import pandas as pd
import requests
from prefect import task, Flow
from prefect.schedules import Interval
from datetime import timedelta

@task
def extract_data():
    url = 'https://jsonplaceholder.typicode.com/posts'
    response = requests.get(url)
    data = response.json()
    return data

@task
def transform_data(data):
    df = pd.DataFrame(data)
    df['title'] = df['title'].str.upper()
    return df


@task
def load_data(df):
    df.to_csv('transformed_data.csv', index=False)
    print("Data loaded successfully!")

schedulecustom = Interval(timedelta(hours=1))

@Flow
def myflow():
    raw_data = extract_data()
    transformed_data = transform_data(raw_data)
    load_data(transformed_data)

def create_deployment():

    try:
        print("Building deployment...")
        myflow.deploy(
            name="etl_pipeline",
            schedule=schedulecustom,
            work_pool_name = "my_work_pool")
        print("Applying deployment...")
        deployment.apply()
        print("Deployment successfully applied!")
    except:
        print(f"Error during deployment process")



if __name__ == "__main__":
    create_deployment()
    #myflow.serve (name="ETL Pipeline", schedules=[schedulecustom])

