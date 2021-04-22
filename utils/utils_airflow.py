'''

This utils file contains helper functions to enhance readability in an Airflow DAG by simplifying
EMR cluster and Spark job creation

'''


import logging

from airflow.providers.amazon.aws.hooks.s3 import S3Hook

log_start = "Starting to "
log_finish = "Finished "

#EMR-related helper functions

def create_spark_job(job_name,file_path, dependencies_path, executor_memory,driver_memory, shuffle_partitions):
    spark_step= [
        {
            "Name": job_name,
            "ActionOnFailure": "CONTINUE",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": ["spark-submit", "--deploy-mode", "client",
                               "--conf", f"spark.driver.memory= {driver_memory}",
                               "--conf", f"spark.executor.memory={executor_memory}",
                               "--conf", f"spark.sql.shuffle.partitions={shuffle_partitions}",
                               "--py-files", dependencies_path, file_path]
            }
        }
    ]
    return spark_step

def cluster_config(jobflow_name,master_type, worker_type, master_instances, worker_instances, cluster_region, bootstrap_path = None):
    if bootstrap_path is not None:
        cluster_conf = {"Name": jobflow_name,
                        "LogUri": "s3://emr-pipeline-id-XXX/XXX/",
                        "ReleaseLabel": "emr-5.19.0",
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
                            "Ec2KeyName": "PEMFILENAME",
                            "KeepJobFlowAliveWhenNoSteps": True,
                            'EmrManagedMasterSecurityGroup': 'sg-XXXXXXXXX',
                            'EmrManagedSlaveSecurityGroup': 'sg-XXXXXXXXXX',
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
                        "LogUri": "s3://emr-pipeline-id-XXX/XXX/",
                        "ReleaseLabel": "emr-5.19.0",
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
                            "Ec2KeyName": "PEMFILENAME",
                            "KeepJobFlowAliveWhenNoSteps": True,
                            'EmrManagedMasterSecurityGroup': 'sg-XXXXXXXXX',
                            'EmrManagedSlaveSecurityGroup': 'sg-XXXXXXXXXX',
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


#python functions (to be used in the Python operator in a DAG file)

def check_data_exists(aws_conn, bucket, prefix):
    logging.info(f'{log_start} check that data exists in s3 bucket: {bucket}')
    source_s3 = S3Hook(aws_conn_id=aws_conn)
    keys = source_s3.list_keys(bucket_name=bucket,
                               prefix=prefix)
    logging.info('keys {}'.format(keys))
    logging.info(f'{log_finish} check that data exists in s3 bucket: {bucket}')


def upload_data(aws_conn,local_file,file_key, bucket):
    try:
        s3 = S3Hook(aws_conn)
        logging.info(f"{log_start} uploading data onto S3 bucket")
        s3.load_file(filename=local_file, key=file_key ,bucket_name=bucket, replace=True)
        logging.info(f"{log_finish} uploading data onto S3 bucket")
    except Exception as e:
        logging.info(e)
        print("Unable to upload data onto S3. Please review logs")