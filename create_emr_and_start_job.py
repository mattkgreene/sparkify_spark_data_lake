import boto3
import configparser
from datetime import datetime
import os

# config = configparser.ConfigParser()
# config.read('dl.cfg')

# os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
# os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

def create_cluster():
    """
    This function does a few things:
    
    First, it reads in the configuration file.
    Second, it creates a boto3 emr client with the
    AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY given
    to instantiate the EMR instance under a specific IAM role.
    Third, it creates a boto3 resource in order to upload the 
    emr_etl.py file to the s3 bucket, which the EMR instance will
    pull from in order to get the python script.
    Fourth, it uses the emr boto3 client connection to
    run the function run_job_flow, which is how the EMR is
    able to be created, accept multiple parameters, and get the
    steps necessary in order to complete x amount of jobs.
    
    *** Important Note *** 
    I was only able to get this part to work by using the EMR
    Release emr-6.1.0 and hadoop 3.3.1 on the ETL side.  I am not sure
    why this worked as there was no discussion about it, but oh well.
    
    See the run_job_flow function for how the EMR is configured.
    
    Finally, a print statement is used at the end to give the user a 
    JobFlowId which could be used in future processes if necessary.
    
    Parameters:
        None
    Outputs:
        None
    """
    
    # simple config parser statements to read the values from the
    # configuration file
    config = configparser.ConfigParser()
    config.read('dl.cfg')
    
    S3_BUCKET=config['S3']['S3_BUCKET']
    S3_KEY=config['S3']['S3_KEY']
    
    S3_URI = 's3://{bucket}/{key}'.format(bucket=S3_BUCKET, key=S3_KEY)
    
    # creating a boto3 EMR client so that we can create a cluster with our
    # IAM Role/User
    connection = boto3.client(
        'emr',
        region_name='us-west-2',
        aws_access_key_id=config['AWS']['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key=config['AWS']['AWS_SECRET_ACCESS_KEY'],
    )
    
    # creating a boto3 s3 resource so we can access our s3 instance and
    # write the etl file there, so the EMR cluster can access the python script
    s3 = boto3.resource('s3',
                       aws_access_key_id=config['AWS']['AWS_ACCESS_KEY_ID'],
                       aws_secret_access_key=config['AWS']['AWS_SECRET_ACCESS_KEY'],
                       region_name='us-west-2')
    # upload etl file to s3 bucket
    s3.meta.client.upload_file("emr_etl.py", S3_BUCKET, S3_KEY)
    
    # use the run_job_flow function to create a EMR cluster with various parameters
    cluster_id = connection.run_job_flow(
        Name='spark_udacity_job',
        LogUri='s3://aws-logs-407701519615-us-west-2/elasticmapreduce/auto_run_logs/',
        ReleaseLabel='emr-6.1.0',
        Applications=[
            {
                'Name': 'Spark'
            },
        ],
        Instances={
            'InstanceGroups': [
                {
                    'Name': "Master nodes",
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'MASTER',
                    'InstanceType': 'm4.xlarge',
                    'InstanceCount': 1,
                },
                {
                    'Name': "Slave nodes",
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'CORE',
                    'InstanceType': 'm4.xlarge',
                    'InstanceCount': 10,
                },
            ],
            'Ec2KeyName': 'spark-cluster',
            'KeepJobFlowAliveWhenNoSteps': True,
            'TerminationProtected': False,
            'Ec2SubnetId': 'subnet-a7a638df',
        },
        Steps=[
            {
                'Name': 'Setup Debugging',
                'ActionOnFailure': 'TERMINATE_CLUSTER',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': ['state-pusher-script']
                }
            },
            {
                'Name': 'Copy ETL File',
                'ActionOnFailure': 'CANCEL_AND_WAIT',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': ['aws', 's3', 'cp', S3_URI, '/home/hadoop/']
                }
            },
            {
                'Name': 'Run Spark',
                'ActionOnFailure': 'CONTINUE',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': ['spark-submit', '/home/hadoop/emr_etl.py']
                }
            }
        ],
        VisibleToAllUsers=True,
        JobFlowRole='EMR_EC2_DefaultRole',
        ServiceRole='EMR_DefaultRole',
        Configurations=[
            {
                 "Classification": "spark-env",
                 "Configurations": [
                   {
                     "Classification": "export",
                     "Properties": {
                        "PYSPARK_PYTHON": "/usr/bin/python3"
                      }
                   }
                ]
              }
        ]
    )

    print ('cluster jobflowid = ', cluster_id['JobFlowId'])

    
create_cluster()