import lifetimes
from lifetimes.utils import summary_data_from_transaction_data
from lifetimes.utils import calibration_and_holdout_data
from lifetimes import BetaGeoFitter

import sklearn
from sklearn.metrics import mean_squared_error

import boto3
from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError, PartialCredentialsError, ParamValidationError, WaiterError

import pandas as pd
import os
import io
from io import StringIO, BytesIO

from loguru import logger

from datetime import timedelta


AWS_ACCESS_KEY_ID=os.environ['AWS_ACCESS_KEY']
AWS_SECRET_ACCESS_KEY=os.environ['AWS_ACCESS_SECRET']


REGION = 'us-east-1'




# -------------------
# FUNCTIONS
# -------------------


# FUNCTION TO EXECUTE ATHENA QUERY AND RETURN RESULTS
# ----------

def run_athena_query(query:str, database: str, region:str):

        
    # Initialize Athena client
    athena_client = boto3.client('athena', 
                                 region_name=region,
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
            

        # OBTAIN DATA

        # --------------



        query_results = athena_client.get_query_results(QueryExecutionId=query_execution_id,
                                                MaxResults= 1000)
        


        # Extract qury result column names into a list  

        cols = query_results['ResultSet']['ResultSetMetadata']['ColumnInfo']
        col_names = [col['Name'] for col in cols]



        # Extract query result data rows
        data_rows = query_results['ResultSet']['Rows'][1:]



        # Convert data rows into a list of lists
        query_results_data = [[r['VarCharValue'] for r in row['Data']] for row in data_rows]



        # Paginate Results if necessary
        while 'NextToken' in query_results:
                query_results = athena_client.get_query_results(QueryExecutionId=query_execution_id,
                                                NextToken=query_results['NextToken'],
                                                MaxResults= 1000)



                # Extract quuery result data rows
                data_rows = query_results['ResultSet']['Rows'][1:]


                # Convert data rows into a list of lists
                query_results_data.extend([[r['VarCharValue'] for r in row['Data']] for row in data_rows])


        results_df = pd.DataFrame(query_results_data, columns = col_names)
        
        return results_df


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


# Delete Existing Data from S3 Path
# -----------

def delete_s3_prefix_data(bucket:str, s3_prefix:str):


  logger.info(f'Deleting existing data from {bucket}/{s3_prefix}')

  # Create an S3 client
  s3_client = boto3.client('s3', 
                          region_name = REGION,
                          aws_access_key_id=AWS_ACCESS_KEY_ID,
                          aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

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

# --------------
# Function to run Athena query , not return results
# --------------


def run_athena_query_no_results(query:str, database: str):

        
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



# -----------------------
# READ IN STORED MODEL
# -----------------------

# S3 bucket and object information
bucket_name = "prymal-analytics"
object_key = "CLTV/bgnbd_clv_model_v1.pkl"

# Create an S3 client
s3 = boto3.resource('s3', 
                region_name = REGION,
                aws_access_key_id=AWS_ACCESS_KEY_ID,
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY
                )

# Download the .pkl file from S3
s3.meta.client.download_file(bucket_name, object_key, 'model.pkl')

# Load model using lifetimes BetaGeoFitter()
bgf = BetaGeoFitter()
bgf.load_model('model.pkl')



# ------------------
# QUERY DATA & CONSTRUCT TRANSACTIONS 
# ------------------


# Query transaction data using Athena


DATABASE = 'prymal'
REGION = 'us-east-1'

# Construct query to pull data by product
# ----

QUERY = f"""SELECT CASE WHEN (email IS NULL OR email = '')
                    THEN 'Email Not Captured' ELSE email END
            , order_date
            , order_id
            , SUM(price*quantity)
            FROM "prymal"."shopify_line_items"
            WHERE DATE(order_date) > DATE(DATE_ADD('year',-3,CAST(current_date AS TIMESTAMP)))     -- last 3 years
            GROUP BY CASE WHEN (email IS NULL OR email = '')
                    THEN 'Email Not Captured' ELSE email END
                    , order_date
                    , order_id

            """

# Query datalake to get quantiy sold per sku for the last 120 days
# ----

result_df = run_athena_query(query=QUERY, database=DATABASE, region=REGION)
result_df.columns = ['email','order_date','order_id','total_price']

# Store transactions
transactions = result_df.copy()

# Convert purchase date to datetime
transactions['order_date'] = pd.to_datetime(transactions['order_date'])
              
# Convert total spend to float
transactions['total_price'] = transactions['total_price'].apply(lambda x: float(x))

# Prepare data for Lifetimes
rfm = summary_data_from_transaction_data(transactions, 'email', 'order_date', monetary_value_col='total_price')

# -----------------------
# MODEL INFERENCE
# -----------------------

prediction_period = 60

# Generate an RFM dataframe for each customer
full_summary = summary_data_from_transaction_data(transactions, 'email', 'order_date', monetary_value_col='total_price')

# Filter to only include repeat customers (where repeat 'frequency' > 0) as we don't know enough about new customers to make predictions
full_summary = full_summary.loc[full_summary['frequency'] > 0]

# Predict probability that customer is "alive"
full_summary['p_alive'] = bgf.conditional_probability_alive(frequency=full_summary['frequency'], 
                                                                  recency=full_summary['recency'], 
                                                                  T=full_summary['T'])


# Predict number of purchases in t days
full_summary['n_predicted_purchases'] = bgf.predict(t=prediction_period, 
                                    frequency=full_summary['frequency'], 
                                    recency=full_summary['recency'], 
                                    T=full_summary['T'])

# Calculate each customer's AOV
customer_aov = transactions.groupby('email')['total_price'].median()
customer_aov = pd.DataFrame(customer_aov).reset_index()
customer_aov.columns = ['email','aov']

# Merge AOV column to RFM dataframe
full_summary.reset_index(inplace=True)
full_summary_df = full_summary.merge(customer_aov,
                            how='left',
                            on='email')


# Calculate customer level CLV (CLV = p_alive * n_predicted_purchases * AOV)
full_summary_df['predicted_clv'] =  full_summary_df['p_alive'] * full_summary_df['n_predicted_purchases'] * full_summary_df['aov']


# --------------------
# WRITE MODEL SUMMARY TO ATHENA
# --------------------

# Create s3 client
s3_client = boto3.client('s3', 
                          region_name = REGION,
                          aws_access_key_id=AWS_ACCESS_KEY_ID,
                          aws_secret_access_key=AWS_SECRET_ACCESS_KEY
                          )

# Set bucket
BUCKET = os.environ['S3_PRYMAL_ANALYTICS']


partition_date = pd.to_datetime('today').strftime('%Y-%m-%d')


# Configure S3 Prefix
S3_PREFIX_PATH = f"shopify/cltv/rfm_inference/partition_date={partition_date}/cltv_rfm_inference_{partition_date}.csv"

# Check if data already exists for this partition
data_already_exists = check_path_for_objects(bucket=BUCKET, s3_prefix=S3_PREFIX_PATH)

# If data already exists, delete it .. 
if data_already_exists == True:
   
   # Delete data 
   delete_s3_prefix_data(bucket=BUCKET, s3_prefix=S3_PREFIX_PATH)


logger.info(f'Writing to {S3_PREFIX_PATH}')


with io.StringIO() as csv_buffer:
    full_summary_df.to_csv(csv_buffer, index=False)

    response = s3_client.put_object(
        Bucket=BUCKET, 
        Key=S3_PREFIX_PATH, 
        Body=csv_buffer.getvalue()
    )

    status = response['ResponseMetadata']['HTTPStatusCode']

    if status == 200:
        logger.info(f"Successful S3 put_object response for PUT ({S3_PREFIX_PATH}). Status - {status}")
    else:
        logger.error(f"Unsuccessful S3 put_object response for PUT ({S3_PREFIX_PATH}. Status - {status}")


# --------------------
# RUN 'ATHENA ALTER TABLE' TO UPDATE TABLE 
# --------------------


QUERY = f"""

ALTER TABLE shopify_cltv_rfm_inference ADD
  PARTITION (partition_date = {partition_date})
  
"""

run_athena_query_no_results(query=QUERY, database='prymal-analytics')

