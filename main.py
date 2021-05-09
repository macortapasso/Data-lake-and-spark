import os
import subprocess
import configparser
import boto3
from botocore.exceptions import ClientError
import json
import logging


logging.basicConfig(
    level=logging.INFO,
    filename="app.log",
    filemode="w",
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def create_aws_client(str_client):
    """
    Create AWS client.

    args:
        str_client: type of client: 's3', 'iam', 'emr'

    return:
        boto3.client
    """
    client = boto3.client(str_client, region_name="us-west-2")
    logging.info(f"{str_client} was returned.")
    return client


def create_role(iam_client, ROLE_NAME):
    """
    Create a IAM role in AWS

    args:
        iam_client: boto3.client iam
        ROLE_NAME: string to name the role

    return:
        role id
    """
    try:
        role = iam_client.create_role(
            Path="/",
            RoleName=ROLE_NAME,
            Description="Allows to read and write in S3 to execute Data Lake ETL.",
            AssumeRolePolicyDocument=json.dumps(
                {
                    "Statement": [
                        {
                            "Action": "sts:AssumeRole",
                            "Effect": "Allow",
                            "Principal": {"Service": "redshift.amazonaws.com"},
                        }
                    ],
                    "Version": "2012-10-17",
                }
            ),
        )
        role_name = role.get_role(RoleName=ROLE_NAME)["Role"]["Arn"]
        logging.info(f"{role} was created.")
        return role_name
    except ClientError as e:
        if e.response["Error"]["Code"] == "EntityAlreadyExists":
            logging.info(f"Role {ROLE_NAME} already exists.")
        else:
            logging.error(f"create_role - {e}")


def create_bucket(s3_client, bucket_name):
    """
    Create S3 bucket.
    """
    try:
        reponse = s3_client.create_bucket(
            ACL="public-read-write",
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "us-west-2"},
        )
        logging.info(f"Bucket {bucket_name} created.")
    except ClientError as e:
        if e.response["Error"]["Code"] == "BucketAlreadyOwnedByYou":
            logging.info(f"Bucket {bucket_name} already owned by you.")
        else:
            logging.error(f"Create_bucket - {e}")


def update_cfg_file(file, section, system, value):
    """
    Function to update ".cfg" file.

    args:
        file: .cfg file
        section: section in .cfg file []
        system: key value
        value: value
    """
    config = configparser.ConfigParser()
    config.read(file)
    with open(file, "w") as f:
        config.set(section, system, value)
        config.write(f)


def delete_bucket(s3_client, bucket):
    """
    Function to delete all objects from a bucket in S3.

    args:
        s3_client: boto3.client obj
        bucket: bucket name
    """
    try:
        objects = {}
        for content in s3_client.list_object_versions(Bucket=bucket)["Versions"]:
            objects["Key"] = content["Key"]
            objects["VersionId"] = content["VersionId"]
            s3_client.delete_objects(Bucket=bucket, Delete={"Objects": [objects]})
        response = s3_client.delete_bucket(Bucket=bucket)
        logging.info(f"Bucket {bucket} was deleted.")
    except ClientError as e:
        logging.error(f"delete_bucket - {e}")


def upload_file_s3(s3_client, file, bucket):
    """
    Function to upload a file in a S3 bucket.

    args:
    s3_client: boto3.client obj
    file: string file name
    bucket: bucket name
    """
    try:
        response = s3_client.upload_file(file, bucket, file)
        logging.info(f"File {file} was uploaded in {bucket}.")
    except ClientError as e:
        logging.error(f"Failed in upload the file {file} - {e}")


if __name__ == "__main__":
    # Read config file to get parameters
    config = configparser.ConfigParser()
    config.read_file(open("dl.cfg"))

    # AWS parameters to use "Programmatic access"
    key = config.get("AWS", "aws_access_key_id")
    secret = config.get("AWS", "aws_secret_access_key")

    # Set AWS credentions as enviroment variables
    os.environ["AWS_ACCESS_KEY_ID"] = key
    os.environ["AWS_SECRET_ACCESS_KEY"] = secret

    # Create Boto3 clients
    s3 = create_aws_client("s3")
    iam = create_aws_client("iam")

    # Create IAM role
    role_name = config.get("IAM_ROLE", "role_name")
    role = create_role(iam, role_name)
    update_cfg_file("dl.cfg", "IAM_ROLE", "arm", "{}{}{}".format("'", role, "'"))

    # Create buckets and upload ETL file in S3
    bucket_to_host_etl = config.get("S3", "python_script")
    bucket_to_host_output = config.get("S3", "output")
    create_bucket(s3, bucket_to_host_etl)
    create_bucket(s3, bucket_to_host_output)
    upload_file_s3(s3, "etl.py", bucket_to_host_etl)

    # Run Spark Job in AWS EMR with shell script
    erm = subprocess.check_output(["sh", "./emr_bootstrap.sh", "s3://udacity-de-python-etl-script/etl.py"])

    # Delete buckets
    if False:
        delete_bucket(s3, bucket_to_host_etl)
        delete_bucket(s3, bucket_to_host_output)
