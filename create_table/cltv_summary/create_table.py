
import boto3
from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError, PartialCredentialsError, ParamValidationError, WaiterError
import pandas as pd
import os
import loguru
from loguru import logger
import io
import time


# -------------------------------------
# Variables
# -------------------------------------

REGION = 'us-east-1'
DATABASE = 'prymal'
CRAWLER_NAME = 'shopify_cltv'


# Transformation SQL Query as code (path)
QUERY_PATH = 'create_table/cltv_summary/create_table.sql'

# AWS Credentials
AWS_ACCESS_KEY_ID=os.environ['AWS_ACCESS_KEY']
AWS_SECRET_ACCESS_KEY=os.environ['AWS_ACCESS_SECRET']


# -------------------------------------
# Functions
# -------------------------------------

# Check S3 Path for Existing Data
# -----------

def check_path_for_objects(bucket: str, s3_prefix:str):

    logger.info(f'Checking for existing data in {bucket}/{s3_prefix}')

    try:

        # Create s3 client
        s3_client = boto3.client('s3', 
                                region_name = REGION,
                                aws_access_key_id=AWS_ACCESS_KEY_ID,
                                aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

        # List objects in s3_prefix
        result = s3_client.list_objects_v2(Bucket=bucket, Prefix=s3_prefix )

        # Instantiate objects_exist
        objects_exist=False

        # Set objects_exist to true if objects are in prefix
        if 'Contents' in result:
            objects_exist=True

            logger.info('Data already exists!')

        return objects_exist

    except NoCredentialsError:
                # Handle missing AWS credentials
            logger.error("No AWS credentials found. Please configure your credentials.")

    except PartialCredentialsError as e:
        # Handle incomplete AWS credentials
        logger.error(f"Partial AWS credentials error: {e}")

    except ClientError as e:
        # Handle S3-specific errors
        if e.response['Error']['Code'] == 'NoSuchBucket':
            logger.error(f"The specified bucket does not exist: {e}")
        elif e.response['Error']['Code'] == 'NoSuchKey':
            logger.error(f"The specified object key does not exist: {e}")
        else:
            logger.error(f"AWS S3 Error: {e}")

    except BotoCoreError as e:
        # Handle general BotoCore errors (e.g., network issues)
        logger.error(f"BotoCore Error: {e}")

    except Exception as e:
        # Handle other exceptions
        logger.error(f"Other Exception: {e}")

# Delete Existing Data from S3 Path
# -----------

def delete_s3_prefix_data(bucket:str, s3_prefix:str):


    logger.info(f'Deleting existing data from {bucket}/{s3_prefix}')

    # Create an S3 client
    s3_client = boto3.client('s3', 
                            region_name = REGION,
                            aws_access_key_id=AWS_ACCESS_KEY_ID,
                            aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

    try:
                                                                                
        # Use list_objects_v2 to list all objects within the specified prefix
        objects_to_delete = s3_client.list_objects_v2(Bucket=bucket, Prefix=s3_prefix)

        # Extract the list of object keys
        keys_to_delete = [obj['Key'] for obj in objects_to_delete.get('Contents', [])]

        # Check if there are objects to delete
        if keys_to_delete:
            # Delete the objects using 'delete_objects'
            response = s3_client.delete_objects(
                Bucket=bucket,
                Delete={'Objects': [{'Key': key} for key in keys_to_delete]}
            )
            logger.info(f"Deleted {len(keys_to_delete)} objects")
        else:
            logger.info("No objects to delete")

        return response

    except NoCredentialsError:
                # Handle missing AWS credentials
            logger.error("No AWS credentials found. Please configure your credentials.")

    except PartialCredentialsError as e:
        # Handle incomplete AWS credentials
        logger.error(f"Partial AWS credentials error: {e}")

    except ClientError as e:
        # Handle S3-specific errors
        if e.response['Error']['Code'] == 'NoSuchBucket':
            logger.error(f"The specified bucket does not exist: {e}")
        elif e.response['Error']['Code'] == 'NoSuchKey':
            logger.error(f"The specified object key does not exist: {e}")
        else:
            logger.error(f"AWS S3 Error: {e}")

    except BotoCoreError as e:
        # Handle general BotoCore errors (e.g., network issues)
        logger.error(f"BotoCore Error: {e}")

    except Exception as e:
        # Handle other exceptions
        logger.error(f"Other Exception: {e}")


# Function to run glue crawlers
# -----------

def run_glue_crawler(crawler_name:str):

    logger.info(f'Running glue crawler: {crawler_name}')

    # Create an AWS Glue client
    glue_client = boto3.client('glue', 
                          region_name = REGION,
                          aws_access_key_id=AWS_ACCESS_KEY_ID,
                          aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

    try:
        # Trigger the Crawler run using the 'start_crawler' method
        response = glue_client.start_crawler(Name=crawler_name)

        logger.info(f"Crawler {crawler_name} started successfully.")

    except NoCredentialsError:
             # Handle missing AWS credentials
            logger.error("No AWS credentials found. Please configure your credentials.")

    except PartialCredentialsError as e:
        # Handle incomplete AWS credentials
        logger.error(f"Partial AWS credentials error: {e}")

    except ClientError as e:
        # Handle S3-specific errors
        if e.response['Error']['Code'] == 'NoSuchBucket':
            logger.error(f"The specified bucket does not exist: {e}")
        elif e.response['Error']['Code'] == 'NoSuchKey':
                logger.error(f"The specified object key does not exist: {e}")
        # log CrawlerRunningException error
        elif e.response['Error']['Code'] == 'CrawlerRunningException':
            logger.info(f"Crawler {crawler_name} is already running.. unable to trigger a new run at this time")

        else:
            logger.error(f"AWS S3 Error: {e}")



    except BotoCoreError as e:
        # Handle general BotoCore errors (e.g., network issues)
        logger.error(f"BotoCore Error: {e}")

    except Exception as e:
        # Handle other exceptions
        logger.error(f"Other Exception: {e}")

    # Return response
    return response

def read_query_to_string(path: str):
# -----------

    # Initialize an empty string to store the SQL query
    query_str = ''

    # Use a try-except block to handle file I/O exceptions
    try:
        with open(path, 'r') as sql_file:
            # Read the entire contents of the file into the string
            query_str = sql_file.read()
    except FileNotFoundError:
        logger.error(f"The file '{path}' was not found.")
    except Exception as e:
        logger.error(f"An error occurred while reading the file: {str(e)}")

    # Return query as string
    return query_str


def run_athena_query(query:str, database: str):

        
    # Initialize Athena client
    athena_client = boto3.client('athena', 
                                 region_name='us-east-1',
                                 aws_access_key_id=AWS_ACCESS_KEY_ID,
                                 aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

    # Execute the query
    try:
        response = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={
                'Database': database
            },
            ResultConfiguration={
                'OutputLocation': 's3://prymal-ops/athena_query_results/'  # Specify your S3 bucket for query results
            }
        )

        query_execution_id = response['QueryExecutionId']

        # Wait for the query to complete
        state = 'RUNNING'

        while (state in ['RUNNING', 'QUEUED']):
            response = athena_client.get_query_execution(QueryExecutionId = query_execution_id)
            logger.info(f'Query is in {state} state..')
            if 'QueryExecution' in response and 'Status' in response['QueryExecution'] and 'State' in response['QueryExecution']['Status']:
                # Get currentstate
                state = response['QueryExecution']['Status']['State']

                if state == 'FAILED':
                    logger.error('Query Failed!')
                elif state == 'SUCCEEDED':
                    logger.info('Query Succeeded!')

                    
    except ParamValidationError as e:
        logger.error(f"Validation Error (potential SQL query issue): {e}")
        # Handle invalid parameters in the request, such as an invalid SQL query

    except WaiterError as e:
        logger.error(f"Waiter Error: {e}")
        # Handle errors related to waiting for query execution

    except ClientError as e:
        error_code = e.response['Error']['Code']
        error_message = e.response['Error']['Message']
        
        if error_code == 'InvalidRequestException':
            logger.error(f"Invalid Request Exception: {error_message}")
            # Handle issues with the Athena request, such as invalid SQL syntax
            
        elif error_code == 'ResourceNotFoundException':
            logger.error(f"Resource Not Found Exception: {error_message}")
            # Handle cases where the database or query execution does not exist
            
        elif error_code == 'AccessDeniedException':
            logger.error(f"Access Denied Exception: {error_message}")
            # Handle cases where the IAM role does not have sufficient permissions
            
        else:
            logger.error(f"Athena Error: {error_code} - {error_message}")
            # Handle other Athena-related errors

    except Exception as e:
        logger.error(f"Other Exception: {str(e)}")
        # Handle any other unexpected exceptions


# ============================================================================
# EXECUTE CODE
# ============================================================================

# Read sql from .sql to string
QUERY_STR = read_query_to_string(path=QUERY_PATH)

# Log Athena query 
logger.info(QUERY_STR)

# Run Athena query
response = run_athena_query(query=QUERY_STR, database=DATABASE)

 