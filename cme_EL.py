from prefect import task, flow, get_run_logger
from prefect.blocks.system import Secret
import requests
import csv , json
import pandas as pd
import json
from base64 import b64encode
import zipfile, io
import sqlalchemy as sa
from datetime import datetime,timedelta


@task
def get_data():
    cme_api_secret_block = Secret.load("cme-api-password")
    today=datetime.today().strftime('%Y%m%d') - timedelta(days=1)
    query = {
    'dataset':'eod', 
    'foiindicator':'fut',
    'yyyymmdd':today
    }
    response = requests.get(
    "https://datamine.cmegroup.com/cme/api/v1/list",
    params=query,
    auth=requests.auth.HTTPBasicAuth('API_GREENFACT', cme_api_secret_block.get())).json()

    arr = response['files']
    return arr

@task
def create_dataframe(arr):
    appended_data = pd.DataFrame()
    cme_api_secret_block = Secret.load("cme-api-password")
    credentials=f'API_GREENFACT:{cme_api_secret_block.get()}'
    for i in arr:
        data = pd.DataFrame(pd.read_csv(i['url'],
            storage_options={'Authorization': b'Basic %s' % b64encode(credentials.encode('ascii'))}, 
            compression='gzip',
            header=0, 
            index_col=False))
        
        appended_data = pd.concat([appended_data, data])
    return appended_data

@task
def upload_data(data_frame):
    #sql_types =  {"Trade Date" : sa.types.Date(),"Last Trade Date": sa.types.Date()}
    db_secret_block = Secret.load("db-password")
    engine = sa.create_engine(f'postgresql://postgres:{db_secret_block.get()}@db.wuitaitdzsskcihzjefc.supabase.co:5432/postgres')
    data_frame.to_sql('cme_sample_flow_data', con=engine,if_exists="append",index=False)

@flow    
def pipeline():
    arr = get_data()
    if len(arr) == 0:
        raise Warning("No new data to process")
    data_frame = create_dataframe(arr)
    upload_data(data_frame)

if __name__ == "__main__":
    pipeline()