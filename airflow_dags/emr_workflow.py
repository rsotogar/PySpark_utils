
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

#airflow operators

from airflow import DAG
from airflow.providers.amazon.aws.operators.emr_create_job_flow import EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.operators.emr_add_steps import EmrAddStepsOperator
from airflow.providers.amazon.aws.operators.emr_terminate_job_flow import EmrTerminateJobFlowOperator
from airflow.providers.amazon.aws.sensors.emr_step import EmrStepSensor

#add white colour to cell
EmrCreateJobFlowOperator.ui_color = "#f9f7f7"
EmrAddStepsOperator.ui_color = "#f9f7f7"
EmrStepSensor.ui_color = "#f9f7f7"
EmrTerminateJobFlowOperator.ui_color = "#f9f7f7"


#CUSTOM FUNCTIONS
def create_spark_job(job_name, file_path, dependencies_path, executor_memory, memory_fraction, shuffle_partitions,executor_cores, executor_instances,driver_memory = None):
    '''

    :param job_name: the name of the Spark job to run
    :param file_path: location of the Spark job in the file system
    :param dependencies_path: location of dependencies needed to run the job
    :param executor_memory: how much memory to allocate to each executor
    :param memory_fraction: the fraction of memory to allocate to compute and storage
    :param shuffle_partitions: the number of partitions of an aggregated spark dataframe
    :param driver_memory: the amount of memory allocated to the driver
    :return: a list containing a dictionary with the required configuration
    '''
    if driver_memory is not None:
        spark_step = [
            {
                "Name": job_name,
                "ActionOnFailure": "CONTINUE",
                "HadoopJarStep": {
                    "Jar": "command-runner.jar",
                    "Args": ["spark-submit", "--deploy-mode", "client",
                                   "--conf", f"spark.driver.memory={driver_memory}",
                                   "--conf", f"spark.executor.memory={executor_memory}",
                                   "--conf", f"spark.memory.fraction={memory_fraction}",
                                   "--conf", f"spark.sql.shuffle.partitions={shuffle_partitions}",
                                   "--conf", f"spark.sql.legacy.parquet.int96RebaseModeInWrite=CORRECTED",
                                   "--conf", f"spark.executor.cores={executor_cores}",
                                   "--conf", f"spark.executor.instances={executor_instances}",
                                   "--py-files", dependencies_path, file_path]
                }
            }
        ]
    else:
        spark_step = [
            {
                "Name": job_name,
                "ActionOnFailure": "CONTINUE",
                "HadoopJarStep": {
                    "Jar": "command-runner.jar",
                    "Args": ["spark-submit", "--deploy-mode", "client",
                             "--conf", f"spark.driver.memory=1g",
                             "--conf", f"spark.executor.memory={executor_memory}",
                             "--conf", f"spark.memory.fraction={memory_fraction}",
                             "--conf", f"spark.sql.shuffle.partitions={shuffle_partitions}",
                             "--conf", f"spark.sql.legacy.parquet.int96RebaseModeInWrite=CORRECTED",
                             "--conf", f"spark.executor.cores={executor_cores}",
                             "--conf", f"spark.executor.instances={executor_instances}",
                             "--py-files", dependencies_path, file_path]
                }
            }
        ]
    return spark_step

def cluster_config(jobflow_name,master_type, worker_type, master_instances,
                   worker_instances, cluster_region, bootstrap_path=None):
    if bootstrap_path is not None:
        cluster_conf = {"Name": jobflow_name,
                        "LogUri": "s3://aws-logs-186198881893-us-east-1/elasticmapreduce/",
                        "ReleaseLabel": "emr-6.3.0",
                        "Instances": {
                            "InstanceGroups": [
                                {
                                    "Name": "Master nodes",
                                    "Market": "ON_DEMAND",
                                    "InstanceRole": "MASTER",
                                    "InstanceType": master_type,
                                    "InstanceCount": master_instances
                                },
                                {
                                    "Name": "Slave nodes",
                                    "Market": "ON_DEMAND",
                                    "InstanceRole": "CORE",
                                    "InstanceType": worker_type,
                                    "InstanceCount": worker_instances
                                }
                            ],
                            "Ec2KeyName": "data-spark-keypair",
                            "KeepJobFlowAliveWhenNoSteps": True,
                            'EmrManagedMasterSecurityGroup': 'sg-09e3865d536c7a9a7',
                            'EmrManagedSlaveSecurityGroup': 'sg-062c6b6fd1ed3338d',
                            'Placement': {
                                'AvailabilityZone': cluster_region,
                            },

                        },
                        "BootstrapActions": [
                            {
                                'Name': 'copy config to local',
                                'ScriptBootstrapAction': {
                                    'Path': bootstrap_path  #path to the bootstrap script
                                }
                            }
                        ],

                        "Applications": [
                            { 'Name': 'hadoop' },
                         { 'Name': 'spark' }
                        ],
                        "VisibleToAllUsers": True,
                        "JobFlowRole": "EMR_EC2_DefaultRole",
                        "ServiceRole": "EMR_DefaultRole",
                        "Tags": [
                            {
                                "Key": "app",
                                "Value": "analytics"
                            },
                            {
                                "Key": "environment",
                                "Value": "development"
                            }
                        ]
                        }
    else:
        cluster_conf = {"Name": jobflow_name,
                        "LogUri": "s3://aws-logs-186198881893-us-east-1/elasticmapreduce/",
                        "ReleaseLabel": "emr-6.3.0",
                        "Instances": {
                            "InstanceGroups": [
                                {
                                    "Name": "Master nodes",
                                    "Market": "ON_DEMAND",
                                    "InstanceRole": "MASTER",
                                    "InstanceType": master_type,
                                    "InstanceCount": master_instances
                                },
                                {
                                    "Name": "Slave nodes",
                                    "Market": "ON_DEMAND",
                                    "InstanceRole": "CORE",
                                    "InstanceType": worker_type,
                                    "InstanceCount": worker_instances
                                }
                            ],
                            "Ec2KeyName": "data-spark-keypair",
                            "KeepJobFlowAliveWhenNoSteps": True,
                            'EmrManagedMasterSecurityGroup': 'sg-09e3865d536c7a9a7',
                            'EmrManagedSlaveSecurityGroup': 'sg-062c6b6fd1ed3338d',
                            'Placement': {
                                'AvailabilityZone': cluster_region,
                            },

                        },
                        "Applications": [
                            {'Name': 'hadoop'},
                            {'Name': 'spark'}
                        ],
                        "VisibleToAllUsers": True,
                        "JobFlowRole": "EMR_EC2_DefaultRole",
                        "ServiceRole": "EMR_DefaultRole",
                        "Tags": [
                            {
                                "Key": "app",
                                "Value": "analytics"
                            },
                            {
                                "Key": "environment",
                                "Value": "development"
                            }
                        ]
                        }

    return cluster_conf



#cluster configuration (passed to EmrCreateJobFlowOperator)
my_cluster = cluster_config("Change_stream_pipelines",
                              master_type= "m4.large",
                              master_instances= 1,
                              worker_type= "m4.large",
                              worker_instances= 2,
                              cluster_region= "us-east-1a",
                              bootstrap_path= "s3://data-spark-pipelines-code/bootstrap-path/boot-script.sh")



pipelines = ["events", "users", "eventsstaged", "activities", "feeds", "attendances", "friendships", "media", "downtoclown", "agents", "interests", "comments"]
jobs = []
step_operators = []
watcher_operators = []



for pipeline in pipelines:
    job = create_spark_job(f"{pipeline}_pipeline",
                           file_path=f"s3://data-spark-pipelines-code/{pipeline}-pipeline/initial_processor.py",
                           dependencies_path="s3://data-spark-pipelines-code/utils/libs.zip",
                           executor_memory="4g",
                           shuffle_partitions=4,
                           executor_cores=2,
                           executor_instances=2,
                           memory_fraction=0.6)
    jobs.append(job)



#default args for DAG
default_args = {
    'owner': 'Airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=3),
    "email": ["ramon@go.social"],
    "email_on_failure": True,
    "email_on_retry": False,
    "provide_context": True,
}



#workflow

with DAG(dag_id="Change-stream-pipelines",
         schedule_interval=timedelta(hours=4),
         start_date=datetime(2021,9,1, hour=8, minute=0, second=0),
         default_args=default_args) as dag:


    create_spark_cluster = EmrCreateJobFlowOperator(task_id='start_emr_cluster',
                                                    aws_conn_id='aws_default',
                                                    emr_conn_id='emr_default',
                                                    job_flow_overrides=my_cluster)

    for job, pipeline in zip(jobs, pipelines):
        pipeline_operator = EmrAddStepsOperator(task_id=f'{pipeline}_pipeline',
                                                job_flow_id="{{ task_instance.xcom_pull(task_ids='start_emr_cluster', key='return_value')}}",
                                                aws_conn_id='aws_default',
                                                trigger_rule="all_done",
                                                steps=job)
        step_operators.append(pipeline_operator)

    events_operator, users_operator, staged_operator, activities_operator, \
    feeds_operator, attendances_operator, friends_operator, media_operator, \
    downtoclown_operator, agents_operator, interests_operator, comments_operator = step_operators

    for pipeline in pipelines:
        watcher_operator = EmrStepSensor(task_id=f"{pipeline}_sensor",
                                         job_flow_id="{{ task_instance.xcom_pull(task_ids='start_emr_cluster', key='return_value')}}",
                                         step_id= f"{{{{ task_instance.xcom_pull(task_ids='{pipeline}_pipeline', key='return_value')[0] }}}}",
                                         target_states=["COMPLETED"])
        watcher_operators.append(watcher_operator)

    events_watcher, users_watcher, staged_watcher, activities_watcher, \
    feeds_watcher, attendances_watcher, friends_watcher, media_watcher, \
    downtoclown_watcher, agents_watcher, interests_watcher, comments_watcher = watcher_operators


    terminate_job_flow_task = EmrTerminateJobFlowOperator(task_id='terminate_job_flow',
                                                          job_flow_id="{{task_instance.xcom_pull(task_ids='start_emr_cluster', key='return_value')}}",
                                                          aws_conn_id='aws_default',
                                                          trigger_rule="all_done")

    #dependencies
    create_spark_cluster >> users_operator >> users_watcher >> staged_operator >> staged_watcher >> \
    activities_operator >> activities_watcher >> feeds_operator >> feeds_watcher >> \
    attendances_operator >> attendances_watcher >> friends_operator >> friends_watcher >> \
    media_operator >> media_watcher >> downtoclown_operator >> downtoclown_watcher >> \
    agents_operator >> agents_watcher >> events_operator >> events_watcher >> \
    interests_operator >> interests_watcher >> comments_operator >> comments_watcher >> terminate_job_flow_task