import zipfile
import shutil
from glob import glob

import pandas as pd
import json

import sys
import os

import athena_basic
#import aws_config


def mkdir_if_not_exists(f):
   if not os.path.exists(f):
       os.makedirs(f)
   return f

def unzip_and_copy(zip_file, unzip_folder , target_bin_file, copy_to_data_folder):
  print(f'{zip_file}, {unzip_folder}, {target_bin_file}')
  with zipfile.ZipFile(zip_file , 'r') as zip_ref:
    zip_ref.extractall(unzip_folder)
    for filename in glob(f'{unzip_folder}/**/{target_bin_file}', recursive=True):
      print(filename)
      shutil.copy(filename, copy_to_data_folder)
      break

def exec_script_and_download_data(query_string, confg,local_location, wait_time = 5):
    execution_id = athena_basic.get_query_execution_id( query_string , confg )
    result = athena_basic.get_query_result(execution_id   , confg , wait_time = wait_time)
    if result is None : 
        return result
    athena_basic.download_s3_file(confg , result , local_location )
    return result

## non_anonymous_sata, anonymous_sata
## da_sata_parquet
def get_query_string(avro_table_name, parquet_table_name , yyyymmddHH, serial_number ) :
    query_string = f'''
select packetdescriptor.pdDate, packetdescriptor.pdTime, packetdescriptor.binaryfile , packetdescriptor.binaryfilepath, p.year , p.month, p.day, p.hour , p.idfy_serial_number
from
{avro_table_name} as a
inner join (

select year , month, day, hour , idfy_serial_number
from {parquet_table_name} where
(year || month || day || hour = '{yyyymmddHH}' ) and
idfy_serial_number = '{serial_number}' 
) as p on p.year = a.year and p.month = a.month and p.day = a.day and p.hour = a.hour
and p.idfy_serial_number = a.packetdescriptor.pddriveserialnumber
where (a.year || a.month || a.day || a.hour = '{yyyymmddHH}' )
'''
    return query_string
def download_bin_files(serial_number #, drive_model 
                       , interface
                       , yyyymmddHH, confg
                       , s3_bin_file_bucket =  'qnap-prod-diskhealth-destination'):
 
 anonymous_avro_table_name = f'anonymous_{interface}'
 non_anonymous_avro_table_name = f'non_anonymous_{interface}'   
 parquet_table_name = f'da_{interface}_parquet'
 query_string = get_query_string(non_anonymous_avro_table_name, parquet_table_name 
                                 , yyyymmddHH, serial_number ) 
 

 data_folder = 'bin'
 local_location = 'athena_query_result.csv'
 #confg = aws_config.get_account2(db = db)
 result = exec_script_and_download_data(query_string, confg , local_location )
 binaryfile_info = pd.read_csv(local_location)
 if len(binaryfile_info) == 0 :
     query_string = get_query_string(anonymous_avro_table_name, parquet_table_name 
                                 , yyyymmddHH, serial_number ) 
     result = exec_script_and_download_data(query_string, confg , local_location )
     binaryfile_info = pd.read_csv(local_location)
 compressed_data = mkdir_if_not_exists('compressed_data')
 unzip_folder = mkdir_if_not_exists('unzip_data')
 drive_bin_folder = mkdir_if_not_exists(f'{data_folder}/{serial_number}/{yyyymmddHH}')
 print(len(binaryfile_info))
 for index, row in binaryfile_info.iterrows():
  binaryfilepath = row['binaryfilepath']
  binaryfile = row['binaryfile']
  zip_file = f'{compressed_data}/{binaryfile}.zip'
  athena_basic.download_s3_file(confg , f's3://{s3_bin_file_bucket}/{binaryfilepath}' , zip_file )
  unzip_and_copy(zip_file, unzip_folder , binaryfile, drive_bin_folder)

 return len(binaryfile_info)

def get_query_string_for_dataset(selected_interface , start_date , end_date , serial_number  ):
    
        
    script = f'''
    select *
    from da_{selected_interface}_parquet
    where year || '-' || month || '-' || day between '{start_date}' and '{end_date}'
    and idfy_serial_number = '{serial_number}'
    order by pd_date , pd_time
    '''
    return script

def depreciated_code():
  if isfile(example_local_location):    
    example_df = pd.read_csv(example_local_location)
    st.dataframe( example_df )
    example_drive_serial = example_df.idfy_serial_number.values[0]
    check_example_path = f"bin/{example_drive_serial}/{selected_date}"
    
    if os.path.isdir(check_example_path):
      onlyfiles = [f for f in listdir(check_example_path) if isfile(join(check_example_path, f))]  
  
      with open(f"{check_example_path}/{onlyfiles[0]}", "rb") as file:
        st.download_button(
        label="download example bin file",
        data=file,
        file_name=onlyfiles[0],
         
        ) 
