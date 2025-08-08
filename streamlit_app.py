import streamlit as st
import pandas as pd
import sys

#lib_path = '/Users/luomein/Documents/code/Docker/PythonJobs/common/aws'
#sys.path.append(lib_path)

#import athena_basic
#import aws_config
import boto3
import os
from os import listdir
from os.path import isfile, join
import datetime

from streamlit_tool import StreamlitOutputRedirector
import bin_file_downloader
import random_sample
import flatten_smart
from st_aggrid import AgGrid, GridOptionsBuilder

grid_response = None
#selected_drive_serial = ''

#local_location = ''

with st.expander("AWS Credential"):
   ## st.header("AWS Credential", divider="gray")

   aws_access_key_id = st.text_input("aws_access_key_id", '')
   if aws_access_key_id == "":
    st.markdown("<div style='background-color:#ffe6e6;padding:0.5em;'>⚠️ aws_access_key_id can not be empty</div>", unsafe_allow_html=True) 
   aws_secret_access_key = st.text_input("aws_secret_access_key", '')
   if aws_secret_access_key == "":
    st.markdown("<div style='background-color:#ffe6e6;padding:0.5em;'>⚠️ aws_secret_access_key can not be empty</div>", unsafe_allow_html=True)  
   #aws_access_key_id = st.text_input("aws_access_key_id", '')
   #aws_secret_access_key = st.text_input("aws_secret_access_key", '')

def set_confg_obj(athena_bucket_name , s3_download_bucket_replace_string , athena_database , athena_WorkGroup , boto3_session_obj ):
    confg = dict()
    confg['athena_bucket_name'] = athena_bucket_name ## "ulink-ai-analysis" ## account1 , "qnap-prod-diskhealth-destination" ## account2
    confg['s3_download_bucket_replace_string']  = s3_download_bucket_replace_string ##  's3://ulink-ai-analysis/' ### 
    confg['athena_database']  = athena_database ## 'sampledb'
    confg['athena_WorkGroup']  = athena_WorkGroup ## 'primary'
    #confg['boto3_profile']  = boto3_profile ##'account1'
    confg['boto3_session'] = boto3_session_obj ##boto3.Session(profile_name= boto3_profile )
    
    return confg

client = boto3.client(
    's3',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    #aws_session_token=SESSION_TOKEN
)

session = boto3.Session(
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    #aws_session_token=SESSION_TOKEN
)


selected_interface = st.selectbox(
    "interface: SATA, NVMe, SCSI",
    ["sata","nvme","scsi"],
    index=0,
    #placeholder="Select a saved email or enter a new one",
    ##accept_new_options=True,
    )
selected_tenant = st.selectbox(
    "tenant: prod_qnap, prod_ulink_nas, prod_ulink_pc",
    ["prod_qnap","prod_ulink_nas","prod_ulink_pc"],
    index=0,
    #placeholder="Select a saved email or enter a new one",
    ##accept_new_options=True,
    )

if aws_access_key_id != "" and aws_secret_access_key != "":
  config = set_confg_obj("da-poc-batch" , 's3://da-poc-batch/'  , selected_tenant , 'mein-da' ,session)


## tab1, tab2, tab3 = st.tabs(["Cat", "Dog", "Owl"])
tabDataset , tabSample  = st.tabs([   "Data downloader" , "Random Sample"   ])

#with tabOutput :
#    output_placeholder = st.empty()

with tabSample :
    #st.header("Example Data", divider="gray")
    #st.header("Input Query", divider="gray")
    #select_is_license = st.radio(
    #    "License",
    #    #key="visibility",
    #    options=["license data", "anonymous data"],
    #)
    
    select_template = st.selectbox(
    "Template",
        random_sample.get_templates_by_interface(selected_interface),
    #[f'{i}' for i in  range(1,20)],
    #index=None,
    #placeholder="Select a saved email or enter a new one",
    accept_new_options=True,
    ) 
    select_feature = st.selectbox(
        "Feature" , random_sample.get_filtered_feature(select_template)
        , index=None,accept_new_options=False
    )
    btn_fetch_example_data = st.button("fetch", disabled = ( (aws_access_key_id == "" or aws_secret_access_key == "") ) )

    example_local_location = 'example.csv'
    if btn_fetch_example_data:
      tabSample_output_placeholder = st.empty()  
      sys.stdout = StreamlitOutputRedirector(tabSample_output_placeholder)  
      specify_date = ( datetime.datetime.now() + datetime.timedelta(days=-3)).strftime('%Y%m%d') 
      query_string = random_sample.get_query_string(selected_interface, select_template , specify_date , select_feature = select_feature )
    
      result = bin_file_downloader.exec_script_and_download_data(query_string, config,example_local_location, wait_time = 5)
      example_df = pd.read_csv(example_local_location)  
      if  len(example_df) == 0 :
          query_string = random_sample.get_query_string(selected_interface, select_template , specify_date , select_feature = select_feature, pd_slot_activated = 'false' )
    
          result = bin_file_downloader.exec_script_and_download_data(query_string, config,example_local_location, wait_time = 5)
          example_df = pd.read_csv(example_local_location)
        
      selected_drive_serial = example_df.idfy_serial_number.values[0]
      st.write("sample drive serial: ", selected_drive_serial)
      
      #year = example_df.year.values[0]  
      #month = example_df.month.values[0]    
      #day = example_df.day.values[0]    
      #start_date = f"{year}-{month:02d}-{day:02d}"
      #end_date = start_date
      # dt = datetime.strptime(date_str, '%Y-%m-%d')
      #default_start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d')
      #default_end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d')
      #local_location = f"dataset/{selected_drive_serial}_{start_date}_{end_date}.csv"
      #query_string = bin_file_downloader.get_query_string_for_dataset(selected_interface , start_date , end_date , selected_drive_serial  )
      #bin_file_downloader.exec_script_and_download_data(query_string, config,local_location, wait_time = 5)
  
      #st.dataframe( example_df )
      #example_drive_serial = example_df.idfy_serial_number.values[0]
      #check_example_path = f"bin/{example_drive_serial}/{selected_date}"
      #bin_file_downloader.download_bin_files(example_drive_serial 
      #                 , selected_interface
      #                 , selected_date , selected_date
      #            , config)
     

#selected_drive_serial = ''
with tabDataset :
    #st.header("Input Query", divider="gray")
    selected_drive_serial = st.text_input("drive serial", '')
    today = datetime.datetime.now().date()
    default_start_date = today - datetime.timedelta(days=33)
    default_end_date = today - datetime.timedelta(days=3)
    date_range = st.date_input(
    "Select a date range",
    value=(default_start_date, default_end_date),
    key="date_range_picker",
        format="YYYY-MM-DD",
    )##.strftime('%Y%m%d')
    start_date = date_range[0].strftime('%Y-%m-%d')
    end_date = date_range[-1].strftime('%Y-%m-%d')  
    local_location = f"dataset/{selected_drive_serial}_{start_date}_{end_date}.csv"  
    bin_file_downloader.mkdir_if_not_exists('dataset')

    btn_dataset = st.button("query" , key = "btn_dataset"
                            , disabled = ( (aws_access_key_id == "" or aws_secret_access_key == "") or (selected_drive_serial == "") ) )

    
    if btn_dataset :
      
      tabDataset_output_placeholder = st.empty()  
      sys.stdout = StreamlitOutputRedirector(tabDataset_output_placeholder)  
      
      query_string = bin_file_downloader.get_query_string_for_dataset(selected_interface , start_date , end_date , selected_drive_serial  )
      bin_file_downloader.exec_script_and_download_data(query_string, config,local_location, wait_time = 5)


if   isfile(local_location):      
      df = pd.read_csv(local_location)
      if selected_interface == "sata" :
         df = flatten_smart.flatten_smart(df)
      else :
          df.reset_index(drop = False , inplace=True)
      #df.drop(['id', 'col_c'], axis=1 , inplace=True)
      st.download_button(
         label="download table as CSV",
         data=flatten_smart.dataframe_to_csv_data(df),
         file_name=f"{selected_drive_serial}_{start_date}_{end_date}.csv",
         mime='text/csv',
      )
      ##st.dataframe( df )
      gb = GridOptionsBuilder.from_dataframe(df)
      gb.configure_default_column(editable=False)
      ##gb.configure_selection(selection_mode="single", use_checkbox=True)
      gb.configure_selection(selection_mode='single', use_checkbox=True)
      gb.configure_grid_options(alwaysShowHorizontalScroll=True)

      grid_options = gb.build()

      grid_response = AgGrid(
    df,
    gridOptions=grid_options,
    update_mode='SELECTION_CHANGED',
    allow_unsafe_jscode=True,
    theme='streamlit', key = 'tabDataset_grid',
          reload_data=False 
      )
      if grid_response is not None :
        #print(grid_response)
        #print(grid_response["selected_rows"])
        selected = grid_response["selected_rows"]
        #print(selected)
    
        
        if selected is not None:
         #st.dataframe( selected )
         st.write("selected: ")
         st.write(f"{selected.idfy_serial_number.values[0]}")
         st.write(f"{selected.pd_date.values[0]}")
         day_delta = ( datetime.datetime.today().date() - datetime.datetime.strptime(selected.pd_date.values[0], "%Y-%m-%d").date() ).days
         if day_delta >= 30:
             st.markdown("<div style='background-color:#ffe6e6;padding:0.5em;'>⚠️ bin files are kept for only 30 days. This date of bin file is out of range. Please choose another date.</div>", unsafe_allow_html=True) 
         btn_query = st.button("query bin file", disabled = (selected_drive_serial=='' or day_delta >= 30 or (aws_access_key_id == "" or aws_secret_access_key == "") ) )



         if btn_query :
           download_bin_file_output_placeholder = st.empty()  
           sys.stdout = StreamlitOutputRedirector(download_bin_file_output_placeholder)    
           bin_file_downloader.download_bin_files(selected_drive_serial 
                       , selected_interface
                       , f"{selected.year.values[0]}{selected.month.values[0]:02d}{selected.day.values[0]:02d}{selected.hour.values[0]:02d}" 

                  , config)

         check_path =  f"bin/{selected_drive_serial}/{selected.year.values[0]}{selected.month.values[0]:02d}{selected.day.values[0]:02d}{selected.hour.values[0]:02d}"




         if os.path.isdir(check_path):
           #btn_download = st.button("download bin file", disabled = (not os.path.isdir(check_path)) )   
           onlyfiles = [f for f in listdir(check_path) if isfile(join(check_path, f))]  
  
           with open(f"{check_path}/{onlyfiles[0]}", "rb") as file:
             st.download_button(
        label="download bin file",
        data=file,
        file_name=onlyfiles[0],
        ##mime="image/png",
        )   
         else :
              st.write("no data yet")














  
