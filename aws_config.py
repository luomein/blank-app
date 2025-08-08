# +
import boto3
import botocore

#sys.path.append( '/Users/meiyinlo/Documents/coding/ULINK/ai/common/aws/' )
#import aws_config
# -

def get_account1():
    confg = set_confg_obj("ulink-ai-analysis"  , 's3://ulink-ai-analysis/' , 'sampledb' , 'primary' , 'account1' )
     
    return confg


def get_account2():
    confg = set_confg_obj("da-poc-batch" , 's3://da-poc-batch/'  , 'prod_qnap' , 'mein-da' , 'default' )
    return confg


def get_account2_alpha():
    confg = set_confg_obj("da-poc-batch" , 's3://da-poc-batch/'  , 'alpha_qnap' , 'mein-da' , 'default' )
    return confg


def get_bucket_name(s3_location):
    BUCKET_NAME = s3_location.replace("s3://", '').split('/')[0]
    return BUCKET_NAME


def get_tenant_config(tenant_info) :
    confg = dict()
     
    confg['athena_bucket_name'] = get_bucket_name( tenant_info['aws_athena_output_bucket'] ) ## "ulink-ai-analysis" ## account1 , "qnap-prod-diskhealth-destination" ## account2
    confg['s3_download_bucket_replace_string']  =  f's3://{confg["athena_bucket_name"]}' ##  's3://ulink-ai-analysis/' ### 
    confg['athena_database']  = tenant_info['aws_athena_database'] ## 'sampledb'
    confg['athena_WorkGroup']  = tenant_info['athena_workgroup'] ## 'primary'
    confg['boto3_session']  = boto3.Session() ##'account1'
    
    return confg


def set_confg_obj(athena_bucket_name , s3_download_bucket_replace_string , athena_database , athena_WorkGroup , boto3_profile ):
    confg = dict()
    confg['athena_bucket_name'] = athena_bucket_name ## "ulink-ai-analysis" ## account1 , "qnap-prod-diskhealth-destination" ## account2
    confg['s3_download_bucket_replace_string']  = s3_download_bucket_replace_string ##  's3://ulink-ai-analysis/' ### 
    confg['athena_database']  = athena_database ## 'sampledb'
    confg['athena_WorkGroup']  = athena_WorkGroup ## 'primary'
    #confg['boto3_profile']  = boto3_profile ##'account1'
    confg['boto3_session'] = boto3.Session(profile_name= boto3_profile )
    
    return confg

# +
###get_account1()
# -


