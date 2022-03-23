"""
This helper file contains helper functions that will improve readability in Airflow DAGs.
"""


# EMR-related helper functions

def create_spark_job(
        job_name,
        file_path,
        dependencies_path,
        executor_memory,
        memory_fraction,
        shuffle_partitions,
        driver_memory="1g"):
    """
    :param job_name: the name of the Spark job to run
    :param file_path: location of the Spark job in the file system
    :param dependencies_path: location of dependencies needed to run the job
    :param executor_memory: how much memory to allocate to each executor
    :param memory_fraction: the fraction of memory to allocate to compute and storage
    :param shuffle_partitions: the number of partitions of an aggregated spark dataframe
    :param driver_memory: the amount of memory allocated to the driver. Defaults to 1gb.
    :return: a list containing a dictionary with the required configuration
    """
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
                         "--py-files", dependencies_path, file_path]
            }
        }
    ]
    return spark_step


def cluster_config(
        jobflow_name,
        master_type,
        worker_type,
        master_instances,
        worker_instances,
        bootstrap_path,
        emr_release,
        master_sg,
        worker_sg,
        cluster_region,
        log_uri
):
    cluster_conf = {
        "Name": jobflow_name,
        "LogUri": log_uri,
        "ReleaseLabel": emr_release,
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
            'EmrManagedMasterSecurityGroup': master_sg,
            'EmrManagedSlaveSecurityGroup': worker_sg,
            'Placement': {
                'AvailabilityZone': cluster_region,
            },

        },
        "BootstrapActions": [
            {
                'Name': 'copy config to local',
                'ScriptBootstrapAction': {
                    'Path': bootstrap_path  # path to the bootstrap script
                }
            }
        ],
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
