import time
from datetime import datetime, timedelta

from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.providers.databricks.hooks.databricks import DatabricksHook
from airflow.operators.python_operator import PythonOperator



def databricks_run(cluster_id, notebook_name, repo_name):
    notebook_task_params = {
        'run_name': f'{notebook_name}',
        'existing_cluster_id': f'{cluster_id}',
        'notebook_task': {
                'notebook_path': f'/Repos/ramon@go.social/{repo_name}/{notebook_name}',
                        }
    }

    return notebook_task_params



def start_databricks_cluster(cluster_id):
    try:
        json_cluster = {"cluster_id":f"{cluster_id}"}
        db_hook = DatabricksHook(databricks_conn_id="databricks_default")
        print(f"Starting cluster: {cluster_id}")
        db_hook.start_cluster(json_cluster)
        print("Success!")
    except Exception as e:
        print("Cluster could not be started. Please review the following error:")
        print(e)


def stop_databricks_cluster(cluster_id):
    try:
        json_cluster = {"cluster_id":f"{cluster_id}"}
        db_hook = DatabricksHook(databricks_conn_id="databricks_default")
        print(f"Terminating cluster: {cluster_id}")
        db_hook.terminate_cluster(json_cluster)
        print("Success!")
    except Exception as e:
        print("Cluster could not be terminated. Please review the following error:")
        print(e)



feed_recommendations = databricks_run("0712-191737-exist272", repo_name="Feed_recommendations", notebook_name="test")


#default args for DAG
default_args = {
    'owner': 'Airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    "email": ["ramon@go.social"],
    "email_on_failure": True,
    "email_on_retry": True
}


#workflow

with DAG(dag_id="feed_recommendations",
         start_date=datetime(2021,8,31, hour=9, minute=0, second=0),
         schedule_interval=timedelta(minutes=30),
         default_args=default_args) as dag:

    start_cluster_operator = PythonOperator(task_id="start_databricks_cluster", python_callable=start_databricks_cluster,
                                            op_kwargs={"cluster_id":"0712-191737-exist272"})

    delay_job_start = PythonOperator(task_id="delay_job_start", python_callable=lambda: time.sleep(300))

    feed_recommendation_pipeline = DatabricksSubmitRunOperator(task_id="feed_recommendations", json=feed_recommendations)

    stop_cluster_operator = PythonOperator(task_id="stop_databricks_cluster",
                                            python_callable=stop_databricks_cluster,
                                            op_kwargs={"cluster_id": "0712-191737-exist272"})


    #dependencies
    start_cluster_operator >> delay_job_start >> feed_recommendation_pipeline >> stop_cluster_operator





