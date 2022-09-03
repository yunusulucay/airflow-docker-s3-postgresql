import os, json, boto3, pathlib, psycopg2, requests
import pandas as pd
from airflow import DAG
from pathlib import Path
import airflow.utils.dates
from airflow.sensors.python import PythonSensor
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

dag1 = DAG(
    dag_id="get_api_send_psql",
    start_date=airflow.utils.dates.days_ago(1),
    schedule_interval=None)

dag2 = DAG(
    dag_id="get_psql_send_s3",
    start_date=airflow.utils.dates.days_ago(1),
    schedule_interval=None)

def _fetch_and_save():
    response_API = requests.get('https://api.covid19india.org/state_district_wise.json')
    data = response_API.text

    data_json = json.loads(data)

    names = [i for i in data_json]

    active_numbers = []
    for name in names:
        active_no = sum([data_json[name]["districtData"][i]["active"] for i in data_json[name]["districtData"] if i!="Unknown"])
        active_numbers.append(active_no)

    # Check if path is exists or create
    main_path = "/tmp/json_file"
    pathlib.Path(main_path).mkdir(parents=True, exist_ok=True)

    json_object = json.dumps(data_json)

    with open(main_path+"/covid_19.json", "w") as outfile:
        outfile.write(json_object)

def _read_json_file(**context):
    main_path = "/tmp/json_file"
    with open(main_path+"/covid_19.json", "r") as openfile:
        json_object = json.load(openfile)

    names = list(json_object.keys())

    active_numbers = []
    for name in names:
        active_no = sum([json_object[name]["districtData"][i]["active"] for i in json_object[name]["districtData"] if i!="Unknown"])
        active_numbers.append(active_no)

    context["task_instance"].xcom_push(key="names",value=names)
    context["task_instance"].xcom_push(key="active_numbers",value=active_numbers)

def _send_to_postgresql(**context):
    names = context["task_instance"].xcom_pull(task_ids="read_json_file", key="names")
    active_numbers = context["task_instance"].xcom_pull(task_ids="read_json_file", key="active_numbers")

    conn = psycopg2.connect(
        database="airflow", 
        user="airflow", 
        password="airflow", 
        host="end_to_end_project_postgres_1", port="5432")

    cursor = conn.cursor()

    cursor.execute("""CREATE TABLE IF NOT EXISTS covid_table(id SERIAL PRIMARY KEY NOT NULL, district_name TEXT, active_number INT)""")

    for name, number in zip(names, active_numbers):
        name = f"""'{name}'"""
        cursor.execute(f"""INSERT INTO covid_table(district_name, active_number) VALUES({name}, {number})""")

    conn.commit()

    cursor.close()

    conn.close()

    os.remove("/tmp/json_file/covid_19.json")

def _wait_for_json(filepath):
    return Path(filepath).exists()

def _wait_for_csv(filepath):
    return Path(filepath).exists()

def _send_csv_s3():
    with open("/tmp/conf_file/configurations.json","r") as output:
        configurations = json.load(output)

    s3_client = boto3.client(
        service_name=configurations["service_name"],
        region_name=configurations["region_name"],
        aws_access_key_id=configurations["aws_access_key_id"],
        aws_secret_access_key=configurations["aws_secret_access_key"])

    s3_client.upload_file(
        "/tmp/csv/covid_19.csv",
        configurations["bucket_name"],
        configurations["file_name"])

    os.remove("/tmp/csv/covid_19.csv")

def _fetch_psql_save_csv(file_path, csv_path):
    conn = psycopg2.connect(
        database="airflow",
        user="airflow",
        password="airflow",
        host="end_to_end_project_postgres_1",
        port="5432")

    cursor = conn.cursor()

    cursor.execute("""SELECT * FROM covid_table""")
    all_data = cursor.fetchall()

    dataframe = pd.DataFrame({}, columns=["district_name", "active_number"])
    for data in all_data:
        dataframe = dataframe.append({"district_name":data[1], "active_number":data[2]}, ignore_index=True)

    pathlib.Path(file_path).mkdir(parents=True, exist_ok=True)

    dataframe.to_csv(csv_path, index=False)
    print("Dataframe saved!")

    cursor.close()
    conn.close()

fetch_and_save = PythonOperator(
    task_id="fetch_and_save",
    python_callable=_fetch_and_save,
    dag=dag1)

# Poke interval is for every 30 seconds check
# timeout is to prevent sensor deadlock
# mode is reschedule to make free of the sensor's slot if it is not poking.
wait_for_json = PythonSensor(
    task_id="wait_for_json",
    python_callable=_wait_for_json,
    poke_interval=30,
    timeout=24*60*60,
    mode="reschedule",
    op_kwargs={
        "filepath":"/tmp/json_file/covid_19.json"},
    dag=dag1)

read_json_file = PythonOperator(
    task_id="read_json_file",
    python_callable=_read_json_file,
    dag=dag1)

send_to_postgresql = PythonOperator(
    task_id="send_to_postgresql",
    python_callable=_send_to_postgresql,
    dag=dag1)

trigger_dag2 = TriggerDagRunOperator(
    task_id="trigger_dag2", 
    trigger_dag_id="get_psql_send_s3",
    dag=dag1)

fetch_psql_save_csv = PythonOperator(
    task_id="fetch_psql_save_csv",
    python_callable=_fetch_psql_save_csv,
    op_kwargs={"file_path":"/tmp/csv",
               "csv_path":"/tmp/csv/covid_19.csv"},
    dag=dag2)

wait_for_csv = PythonSensor(
    task_id="wait_for_csv",
    python_callable=_wait_for_csv,
    poke_interval=30,
    timeout=24*60*60,
    mode="reschedule",
    op_kwargs={"filepath":"/tmp/csv/covid_19.csv"},
    dag=dag2)

send_csv_s3 = PythonOperator(
    task_id="send_csv_s3",
    python_callable=_send_csv_s3,
    dag=dag2)

fetch_and_save >> wait_for_json >> read_json_file >> send_to_postgresql >> trigger_dag2

fetch_psql_save_csv >> wait_for_csv >> send_csv_s3





