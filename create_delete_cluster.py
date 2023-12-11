import pandas as pd
import boto3
import json
import configparser
import psycopg2
import time

from botocore.exceptions import ClientError

config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

def create_resources(config):
    """
    Parses key and secret from config and creates AWS resources ec2, s3, iam, redshift
    """
    
    config.read_file(open('dwh.cfg'))
    KEY = config.get('AWS','KEY')
    SECRET = config.get('AWS','SECRET')
    
    ec2 = boto3.resource('ec2',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                    )

    s3 = boto3.resource('s3',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                   )

    iam = boto3.client('iam',
                     region_name='us-west-2',
                     aws_access_key_id=KEY,
                     aws_secret_access_key=SECRET
                  )

    redshift = boto3.client('redshift',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                       )
    
    return ec2, s3, iam, redshift

def create_role(iam, config):
    """
    Creates role that allows to call AWS services and uploads this role into config
    """
    config.read_file(open('dwh.cfg'))
    DWH_IAM_ROLE_NAME = config.get("DWH", "DWH_IAM_ROLE_NAME")
    
    try:
        print("1.1 Creating a new IAM Role") 
        dwhRole = iam.create_role(
            Path='/',
            RoleName=DWH_IAM_ROLE_NAME,
            Description = "Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                   'Effect': 'Allow',
                   'Principal': {'Service': 'redshift.amazonaws.com'}}],
                 'Version': '2012-10-17'})
        )    
    except Exception as e:
        print(e)
    
    
    print("1.2 Attaching Policy")

    iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                       PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                      )['ResponseMetadata']['HTTPStatusCode']

    print("1.3 Get the IAM role ARN")
    roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']

    config['IAM_ROLE']['arn'] = "\'" + roleArn + "\'"
    
    with open('dwh.cfg', 'w') as configfile:
        config.write(configfile)
        
    return roleArn
        

def create_cluster(role_arn, redshift, config):
    """
    Creates redshift cluster, then checks every 20 seconds if it is available; when it becomes available, informs a user. Finally, the function saves endpoint of a cluster as a host to the config file.
    """
    # upload config
    config.read_file(open('dwh.cfg'))
    
    # create cluster
    try:
        redshift.create_cluster(        
            #HW
            ClusterType=config.get("DWH","DWH_CLUSTER_TYPE"),
            NodeType=config.get("DWH","DWH_NODE_TYPE"),
            NumberOfNodes=int(config.get("DWH","DWH_NUM_NODES")),

            #Identifiers & Credentials
            DBName=config.get("CLUSTER","DWH_DB"),
            ClusterIdentifier=config.get("DWH","DWH_CLUSTER_IDENTIFIER"),
            MasterUsername=config.get("CLUSTER","DWH_DB_USER"),
            MasterUserPassword=config.get("CLUSTER","DWH_DB_PASSWORD"),

            #Roles (for s3 access)
            IamRoles=[role_arn]
        )
    except Exception as e:
        print(e)
    
    # check every 20 seconds if it is available
    while True:
        response = redshift.describe_clusters(ClusterIdentifier=config.get("DWH","DWH_CLUSTER_IDENTIFIER"))
        cluster_status = response['Clusters'][0]['ClusterStatus']

        if cluster_status.lower() == 'available':
            print("Cluster is now available.")
            break
        
        else:
            print("Cluster is not available yet")
        
        time.sleep(20)
    
    # saves host of a cluster to the config
    myClusterProps = redshift.describe_clusters(ClusterIdentifier=config.get("DWH","DWH_CLUSTER_IDENTIFIER"))['Clusters'][0]
    
    config['CLUSTER']['Host'] = myClusterProps['Endpoint']['Address']
    with open('dwh.cfg', 'w') as configfile:
        config.write(configfile)
        
def delete_cluster(redshift, config):
    """
    Deletes a cluster and checks every 20 seconds if a cluster is deleted. When a cluster is deleted, the function informs a user about that.
    """
    config.read_file(open('dwh.cfg'))
    DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
    
    redshift.delete_cluster( ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)
    
    while True:
        
        try:
            myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'ClusterNotFound':
                print("Cluster is successfully deleted")
                break
                
        time.sleep(20)
        
def main():
    user_response = input("Do you want to create or delete the cluster? (create/delete): ").strip().lower()

    if user_response == 'create':
        
        config = configparser.ConfigParser()
        ec2, s3, iam, redshift = create_resources(config)
        print('resources are created')

        role_arn = create_role(iam, config)
        print('role is created')

        create_cluster(role_arn, redshift, config)
        
    elif user_response == 'delete':
        
        config = configparser.ConfigParser()
        ec2, s3, iam, redshift = create_resources(config)
        delete_cluster(redshift, config)
   
    else:
        
        print("please, write 'create' or 'delete' depending on your needs")

if __name__ == "__main__":
    main()
    
    

                
            
    

