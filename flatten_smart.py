import pandas as pd
import json
from io import StringIO

import re
def convert_to_json(s):
    # Replace '=' with ':'
    s = re.sub(r'(\w+)=', r'"\1":', s)
    # Ensure proper double quotes around keys/values
    s = s.replace("'", '"')  # just in case there are single quotes
    return json.loads(s)

def json_normalize_convert_to_json(s):
    return pd.json_normalize( convert_to_json(s)['sata002smartdatas'] )

def get_sata002smartdatas(s):
    return convert_to_json(s)['sata002smartdatas'] 

## data = pd.read_csv("/Users/luomein/Documents/code/streamlit/dataset/e4dd45f5a6d70ca6413a6b80c80159792e0d51c47cbe1e780b2f0ec331724915_2025-07-04_2025-08-03.csv")
##data["sata_smart_data_parsed"] = data["sata_smart_data"].apply(get_sata002smartdatas)    


#st.download_button(
#    label="Download CSV",
#    data=csv_data,
#    file_name='data.csv',
#    mime='text/csv'
#)    

def dataframe_to_csv_data(df):
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_data = csv_buffer.getvalue()

        return csv_data



def flatten_smart(data):
    data["sata_smart_data_parsed"] = data["sata_smart_data"].apply(get_sata002smartdatas)    
    data['id'] = data.index

    rows = []
    for idx, row in data.iterrows():
      row_id = row["id"]
      for entry in row["sata_smart_data_parsed"]:
        sata_id = entry.get("sata002smartid")
        ## f"{k}_{sata_id}": json.dumps(v) if isinstance(v, dict) else v  
        flat = {f"{k}_{sata_id}": json.dumps(v) if isinstance(v, dict) else v   for k, v in entry.items() if k != "sata002smartid"}
        flat["id"] = row_id
        rows.append(flat)

    # 3. Build new DataFrame
    flattened_df = pd.DataFrame(rows)

    # 4. Group by id and combine into single row
    result = flattened_df.groupby("id").first().reset_index()

    final_df = pd.merge(data, result, on="id", how="inner")
    final_df.drop(['id', 'sata_smart_data_parsed'], axis=1 , inplace=True)
    final_df.reset_index(drop = False , inplace=True)

    assert len(result) == len(data)
    assert len(result) == len(final_df)

    #print(data.shape, result.shape , final_df.shape)


    return final_df