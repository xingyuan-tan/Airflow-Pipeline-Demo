# Built-in libraries
from datetime import datetime, timedelta
from dateutil import tz
import configparser
import logging

# Third-party libraries
import pandas as pd

# Airflow packages
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator


# IMPORT CONFIGURATION
config = configparser.ConfigParser()
config.read('pipeline_transformation.cfg')

retrieve_location = config['MAIN']['retrieve_location']
extract_file1 = config['MAIN']['extract_file1']
extract_file2 = config['MAIN']['extract_file2']
destination_file = config['MAIN']['destination_file']


def transform_file(file):
    df = pd.read_csv(retrieve_location + file, sep=',', header=0)

    # dropping null value columns to avoid errors
    df.dropna(inplace=True)

    # new data frame with split value columns
    new_name = df["name"].str.split(" ", n=1, expand=True)

    # convert price to float to eliminate extra zeroes
    new_price = df['price'].astype(float)

    # check if price > 100
    above_100 = new_price > 100

    # create new dataframe
    new_df = pd.DataFrame()

    # add the desired columns to new dataframe
    new_df["first_name"] = new_name[0]
    new_df["last_name"] = new_name[1]
    new_df["price"] = new_price
    new_df["above_100"] = above_100

    # Load the file to destination
    new_df.to_csv(destination_file + 'new_' + file, sep=',', header=True, index=False, encoding='utf-8-sig')


def merge_csv():
    try:
        # For both files, combine as csv using pandas
        all_filenames = [destination_file + 'new_' + extract_file1,
                         destination_file + 'new_' + extract_file2]
        combined_csv = pd.concat([pd.read_csv(f) for f in all_filenames])
    except:
        # Failing means only one file is present. If no file is present, this function will not trigger at all
        logging.info("Only one file present")
        return

    # export to csv
    combined_csv.to_csv(destination_file + "new_dataset.csv", sep=',', header=True, index=False, mode='a',
                        encoding='utf-8-sig')
    logging.info('Merged and loaded')


dag = DAG('pipeline_transformation', description='Hello World DAG',
          schedule_interval=timedelta(days=1),
          start_date=datetime(2021, 8, 22, 1, 15, tzinfo=tz.gettz('Asia / Singapore')),
          catchup=False,
          )

transform_operator1 = PythonOperator(task_id='transform_task1',
                                     op_kwargs={'file': extract_file1},
                                     python_callable=transform_file,
                                     dag=dag)

transform_operator2 = PythonOperator(task_id='transform_task2',
                                     op_kwargs={'file': extract_file2},
                                     python_callable=transform_file,
                                     dag=dag)

merge_operator = PythonOperator(task_id='merge_task',
                                python_callable=merge_csv,
                                trigger_rule='one_success',  # Will process even if only 1 file present
                                dag=dag)

# Removing all the intermediary files and the initial files to prepare for the next landing.
teardown_operator1 = BashOperator(task_id="teardown_task1",
                                  bash_command="rm " + destination_file + 'new_' + extract_file1, retries=0)

teardown_operator2 = BashOperator(task_id="teardown_task2",
                                  bash_command="rm " + destination_file + 'new_' + extract_file2, retries=0)

teardown_operator3 = BashOperator(task_id="teardown_task3",
                                  bash_command="rm " + retrieve_location + extract_file1, retries=0)

teardown_operator4 = BashOperator(task_id="teardown_task4",
                                  bash_command="rm " + retrieve_location + extract_file2, retries=0)

transform_operator1 >> merge_operator
transform_operator2 >> merge_operator
merge_operator >> teardown_operator1
merge_operator >> teardown_operator2
merge_operator >> teardown_operator3
merge_operator >> teardown_operator4
