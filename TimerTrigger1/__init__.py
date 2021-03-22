import datetime, time
import logging
import pandas as pd
import fastparquet
import os


import azure.functions as func

#alice_blue
import alice_blue
from alice_blue import *

#pydrive
import pickle
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from apiclient.http import MediaFileUpload
from google.oauth2.credentials import Credentials


#global variables

#holds live streaming data
live_data=[] 
#websocket 
socket_opened = False



#pydrive
def service_object():
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.

    # If modifying these scopes, delete the file token.json.
    SCOPES = ['https://www.googleapis.com/auth/drive']

    #token.json resides in overall function directory instead of individual function apps
    creds = Credentials.from_authorized_user_file('token.json', SCOPES)

    #refreshes with refresh_token if creds is expired
    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())

    
    service = build('drive', 'v3', credentials=creds)

    return service


def upload(file_name,file_path):
    
    #service_object
    service=service_object()

    #folder_id
    folder_object = service.files().list(q="mimeType='application/vnd.google-apps.folder'  and name='first5min'",
                                         spaces='drive',
                                         fields='files(name,id)').execute()
    
    folder_id = folder_object.get('files')[0]['id']

    #upload
    file_metadata = {
    'name': [file_name],
    'parents': [folder_id]}
    media = MediaFileUpload(file_path, mimetype='text/parquet', resumable=True)
    service.files().create(body=file_metadata,media_body=media, fields='id').execute()
    




#dataprocess
def instruments_list():
    scriptpath = os.path.abspath(__file__)
    scriptdir = os.path.dirname(scriptpath)
    file_name='instruments1.pickle'
    file_path = os.path.join(scriptdir, file_name)

    with open(file_path, 'rb') as f:
        ins_list=pickle.load(f)

    return ins_list

def convert(live_data):
    df=pd.DataFrame(live_data[:])
    df.drop(['instrument'],inplace=True, axis = 1) #parquet makes error while converting that column value
    df.columns.astype('str') #parquet needs the column type to be string
    
    scriptpath = os.path.abspath(__file__)
    scriptdir = os.path.dirname(scriptpath)
    #datetime module is imported in __init__.py
    file_name = str(datetime.date.today()) + '-' + 'm' #format: yyyy-mm-dd-x ; x={'m': 'MARKET_DATA', 's': 'SNAPQUOTE'}
    file_path = os.path.join(scriptdir, file_name)
    df.to_parquet(file_path,compression='gzip') #saved in curr directory

    return file_name, file_path




def main(mytimer: func.TimerRequest) -> None:
    #IST 5hr 30min ahead of UTC
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()
    
    
    #write your here
    status='Unsuccessfule'
    
    status=run_it()

    logging.info('Python timer trigger function run is %s by %s', status,utc_timestamp)


#websocket
def run_it():

    #status
    status='Unsuccessful'

    #credentials
    username='242661'
    password='&Khyati0'
    api_secret='sGWyHQ7B0PneyCkQ38No7CPKSpJdFkN1uzghvvdD976Pcvju8B9KdPznQKe6GHP5'
    app_id='CetEoSQctj'
    twoFA='khyati'

    #access token and alice object
    access_token = AliceBlue.login_and_get_access_token(username=username, password=password, twoFA=twoFA,  api_secret=api_secret, app_id=app_id)

    
    alice = AliceBlue(username=username, password=password, access_token=access_token, master_contracts_to_download=['NSE', 'BSE'])
 

    #get instrument list
    instruments=instruments_list()

    

    #web socket
    
    def event_handler_quote_update(message):
        global live_data
        live_data.append(message)

    def open_callback():
        global socket_opened
        socket_opened = True

    alice.start_websocket(subscribe_callback=event_handler_quote_update,
                        socket_open_callback=open_callback,
                        run_in_background=True)
    while(socket_opened==False):
        pass


    #instrument subscription
    for x in instruments:
        alice.subscribe(alice.get_instrument_by_symbol('NSE', x), LiveFeedType.MARKET_DATA)


    #wait for 4 min 30 seconds [270s]
    #this constrain is because azure fn time limit is 5min
    #remaining 30s is for dataprocessing and upload
    #time module is imported in __init__.py
    time.sleep(10)



    #data processing
    file_name, file_path=convert(live_data)

    
    #Google drive API
    upload(file_name, file_path)


    #delete the data file once uploaded
    os.remove(file_path)

    #execution is finished
    status='Successful'

    return status





