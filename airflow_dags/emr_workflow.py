
from datetime import datetime, timedelta

#airflow operators

from airflow import DAG
from airflow.providers.amazon.aws.operators.emr_add_steps import EmrAddStepsOperator
from airflow.providers.amazon.aws.operators.emr_create_job_flow import EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.operators.emr_terminate_job_flow import EmrTerminateJobFlowOperator
from airflow.providers.amazon.aws.sensors.emr_step import EmrStepSensor
from airflow.operators.python import PythonOperator


#utils airflow
from utils import utils_airflow

#cluster configuration (passed to EmrCreateJobFlowOperator)
cluster_conf = utils_airflow.cluster_config("My_job_flow",
                              master_type= "m4.4xlarge",
                              master_instances= 1,
                              worker_type="m4.4xlarge",
                              worker_instances=3,
                              cluster_region= "eu-west-1a",
                              bootstrap_path=None)

#job creation (passed to EmrAddStepsOperator)
my_first_job = utils_airflow.create_spark_job("My_first_job",
                                file_path= "s3://some_bucket/subfolder/pyspark_job.py",
                                dependencies_path="s3://some_bucket/dependencies/dependencies.zip",
                                executor_memory="8g",
                                shuffle_partitions=10,
                                memory_fraction= 0.8)

#default args for DAG
default_args = {
    'owner': 'Airflow',
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
    "email": ["sotogarcia.r@icloud.com"],
    "email_on_failure": True,
    "email_on_retry":True
}

#workflow

with DAG(dag_id="my_first_dag",
         start_date=datetime(2021,4,8),
         schedule_interval=timedelta(days=1),
         end_date=datetime(2022, 4, 8),
         default_args=default_args) as dag:


    upload_data = PythonOperator(task_id="upload_data_s3",
                                 python_callable=utils_airflow.upload_data,
                                 op_kwargs={"aws_conn": "my_aws_conn",
                                            "bucket": "bucket",
                                            "local_file": "some_file_path"})

    check_data_exists_task = PythonOperator(task_id='check_data_exists',
                                            python_callable=utils_airflow.check_data_exists,
                                            op_kwargs= {"aws_conn": "my_aws_conn",
                                                        "bucket":"my_bucket",
                                                        "prefix": "file_name"},
                                            provide_context=False)

    create_job_flow_task = EmrCreateJobFlowOperator(task_id='create_job_flow',
                                                    aws_conn_id='aws_default',
                                                    emr_conn_id='emr_default',
                                                    job_flow_overrides=cluster_conf)

    add_step_task = EmrAddStepsOperator(task_id='My_first_job',
                                        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_job_flow', key='return_value') }}",
                                        aws_conn_id='my_aws_conn',
                                        steps=my_first_job)

    watch_prev_step_task = EmrStepSensor(task_id='watch_prev_step',
                                        job_flow_id="{{task_instance.xcom_pull(task_ids='create_job_flow', key='return_value')}}",
                                        step_id="{{task_instance.xcom_pull(task_ids='My_first_job', key='return_value')}}",
                                        aws_conn_id='aws_default')

    terminate_job_flow_task = EmrTerminateJobFlowOperator(task_id='terminate_job_flow',
                                                          job_flow_id="{{task_instance.xcom_pull(task_ids='create_job_flow', key='return_value')}}",
                                                          aws_conn_id='aws_default',
                                                          trigger_rule="all_done")

#dependencies

check_data_exists_task >> create_job_flow_task
create_job_flow_task >> add_step_task
add_step_task >> watch_prev_step_task
watch_prev_step_task >> terminate_job_flow_task

