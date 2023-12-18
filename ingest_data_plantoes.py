#!/usr/bin/env python
# coding: utf-8

import datetime
from dateutil import relativedelta
import requests
import pandas as pd
import numpy as np
import os
import pickle

from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
import requests
import gzip

import os
import argparse

from time import time

import pandas as pd
from sqlalchemy import create_engine

SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly', 'https://www.googleapis.com/auth/drive']

def get_credentials():
    creds = None
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)

    if not creds or not creds.valid:
        # If creds is expired but has a refresh token, refresh the creds
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        # Otherwise, get the creds from the OAuth2.0 flow
        else:
            flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)

        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    return creds



def download_google_sheet_to_dataframe(file_id, service, sheet_name):
    request = service.spreadsheets().values().get(spreadsheetId=file_id, range=sheet_name)
    response = request.execute()

    if 'values' in response:
        data = response['values']
        header_row = data[0]
        data_rows = data[1:]
        
        max_columns = max(len(header_row), max(len(row) for row in data_rows))
        
        if len(header_row) < max_columns:
            header_row.extend([''] * (max_columns - len(header_row)))
        
        for row in data_rows:
            if len(row) < max_columns:
                row.extend([''] * (max_columns - len(row)))

        return pd.DataFrame(data_rows, columns=header_row)
    else:
        print(f'No data found in the sheet/tab "{sheet_name}".')
        return None

def load_save_data(file_id, sheet_names, pickle_filename, threshold=6):
    creds = get_credentials()
    drive_service = build('drive', 'v3', credentials=creds)
    sheets_service = build('sheets', 'v4', credentials=creds)   

    
    dataframes = []

    for sheet_name in sheet_names:
        df = download_google_sheet_to_dataframe(file_id, sheets_service, sheet_name)
        df['hospital'] = sheet_name
        df = df.replace({'': np.nan, None: np.nan})
        df = df.dropna(thresh=threshold)

        if df is not None:
            dataframes.append(df)

    concatenated_df = pd.concat(dataframes, axis=0, ignore_index=True)
    
    with open(pickle_filename, 'wb') as f:
        pickle.dump(concatenated_df, f)

    # print(f'All sheets have been concatenated and saved as a pickle file: {pickle_filename}')
    return concatenated_df



def clean_data(data):
    data = data.reset_index(drop=True)
    data['Valor a ser pago'] = data['Valor a ser pago'].str.replace('R$ ', '').str.replace('.', '').str.replace(',00', '')
    data['Valor combinado'] = data['Valor combinado'].str.replace('R$ ', '').str.replace('.', '').str.replace(',00', '')
    data['Valor adicional'] = data['Valor adicional'].str.replace('R$ ', '').str.replace('.', '').str.replace(',00', '')
    data['À vista'] = data['À vista'].replace({'TRUE': True, 'FALSE': False})    	    

    return data

def change_types(df: pd.DataFrame) -> pd.DataFrame:
    df['Data'] = pd.to_datetime(df['Data'], format='%d/%m/%Y')
    df['Hora entrada'] = pd.to_datetime(df['Hora entrada'], format='%H:%M')
    df['Hora saída'] = pd.to_datetime(df['Hora saída'], format='%H:%M')
    df['CRM'] = df['CRM'].astype('str')
    df['Valor a ser pago'] = pd.to_numeric(df['Valor a ser pago'])
    df['Valor combinado'] = pd.to_numeric(df['Valor combinado'])
    df['Valor adicional'] = pd.to_numeric(df['Valor adicional'])    
    
    return df

def time_management(data):
    data['Hora saída'] = pd.to_datetime(data['Hora saída'], format='mixed')
    data['Hora entrada'] = pd.to_datetime(data['Hora entrada'], format='mixed')

    data['Total Horas'] = data['Hora saída'] - data['Hora entrada']
    data['Total Horas'] = data['Total Horas'].dt.total_seconds() / 3600
    data['Total Horas'] = abs(data['Total Horas'].round(0))

    return data

def main():
    # adicionais = load_save_data(file_id = '1wFFs_Szh4cTEjLF1SDZmY4ovc0zInQF4l6X0hKi0w5g',
    #                   sheet_names = ['ADICIONAIS'],
    #                   pickle_filename = 'data/adicionais.pkl', threshold=2)

    data = load_save_data(file_id = '1C1oDy6KN0nXZOpj8EhCKAeLzJW-J0UxHP4jzZf5yLZQ',
                      sheet_names = ['Brasilândia', 'HGP', 'HRIM', 'HSP', 'MBoi', 'SP Plus'],
                      pickle_filename = 'data/concatenated_data.pkl')
    data = clean_data(data)
    data = change_types(data)
    data = time_management(data)    
    

    engine = create_engine('postgresql://root:{PASSWORD}@{DEV_SERVER}/{TABLE}')
    engine.connect()

    t_start = time()

    data.to_sql(name='fechamento_plantoes', con=engine, if_exists='replace')

    t_end = time()

    data = load_save_data(file_id = '1wFFs_Szh4cTEjLF1SDZmY4ovc0zInQF4l6X0hKi0w5g',
                      sheet_names = ['ADICIONAIS'],
                      pickle_filename = 'data/adicionais_data.pkl')
    
    t_start = time()

    data.to_sql(name='plantoes_adicionais', con=engine, if_exists='replace')

    t_end = time()

    print("Finished ingesting data into the postgres database in %.3f seconds" % (t_end - t_start))

main()