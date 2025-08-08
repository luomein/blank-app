import boto3
import botocore
import secrets
import time
#sys.path.append( '../common/aws/' )
#import athena_basic

import aws_config


def get_query_execution_id( query_string , confg ) :
    
    #athena_client = boto3.Session(profile_name= confg['boto3_profile'] ).client('athena' , region_name = 'us-east-1')
    athena_client = confg['boto3_session'].client('athena' , region_name = 'us-east-1')
    
    params = {
     ##'region' : '<fill it with your region>',
     'database' : confg['athena_database'],
     'bucket' : confg['athena_bucket_name'],
     'path'  : 'query_result',
     'query': query_string
     }
## This function executes the query and returns the query execution ID
    response_query_execution_id = athena_client.start_query_execution(
        ClientRequestToken = secrets.token_hex(16) , 
        QueryString = params['query'],
        QueryExecutionContext = {
            'Database' : params['database']
        },
        ResultConfiguration = {
            'OutputLocation': 's3://' + params['bucket'] + '/' + params['path']
        }
        , WorkGroup= confg['athena_WorkGroup']
    )
    return response_query_execution_id
def get_query_result(response_query_execution_id   , confg , wait_time = 100) :
    
    #athena_client = boto3.Session(profile_name= confg['boto3_profile'] ).client('athena' , region_name = 'us-east-1')
    athena_client = confg['boto3_session'].client('athena' , region_name = 'us-east-1')
    
    iterations = 150
    print(response_query_execution_id)
    while (iterations>0):
        time.sleep(wait_time)
        
        response_get_query_details = athena_client.get_query_execution(
        QueryExecutionId = response_query_execution_id['QueryExecutionId']
        )
        status = response_get_query_details['QueryExecution']['Status']['State']
        print(iterations , status)
        if (status == 'FAILED') or (status == 'CANCELLED') :
            print(response_get_query_details)
            break
            
        elif status == 'SUCCEEDED':
            location = response_get_query_details['QueryExecution']['ResultConfiguration']['OutputLocation']
            print('SUCCEEDED' , location)
            ## Function to get output results
            #response_query_result = athena_client.get_query_results(
            #    QueryExecutionId = response_query_execution_id['QueryExecutionId'], 
        #MaxResults=1000
            #)
            #result_data = response_query_result['ResultSet']
            #print("location: ", location)
            #print("data: ", len( result_data['Rows'] ) ) 
            return location
            #return location, result_data
            break
        else:
            time.sleep(wait_time)
            iterations = iterations - 1
#S3_cleanup.clean_up()


def download_s3_file(confg , s3_location , local_location ):
    #s3 = boto3.Session(profile_name= confg['boto3_profile'] ).client('s3')
    s3 = confg['boto3_session'].client('s3')
    ## s3://da-poc-batch/query_result/1a4df143-fba6-466a-9ec2-79e582c809e7
    BUCKET_NAME = s3_location.replace("s3://", '').split('/')[0]
    OBJECT_NAME = s3_location.replace("s3://", '').replace(f'{BUCKET_NAME}/', '')
    print( BUCKET_NAME ,  OBJECT_NAME )
    s3.download_file( BUCKET_NAME ,  OBJECT_NAME , local_location)

# +
#download_s3_file(aws_config.get_account2() , 's3://da-poc-batch/query_result/d183addb-a4b3-4ea8-9687-401bee921bbc.csv' , 'test.csv' )
# -


