import os
import subprocess
import configparser

"""
    File to run locally the ETL process . It requires the Spark instalation.
"""

# Read config file to get parameters
config = configparser.ConfigParser()
config.read_file(open('dl.cfg'))

# AWS parameters to use "Programmatic access"
key = config.get('AWS','aws_access_key_id')
secret = config.get('AWS','aws_secret_access_key')

# Set AWS credentions as enviroment variables
os.environ['AWS_ACCESS_KEY_ID'] = key 
os.environ['AWS_SECRET_ACCESS_KEY'] = secret

# Call etl.py
os.system('python etl.py')