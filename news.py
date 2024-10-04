# This script is used to retrieve economic calendar for the upcoming week every sunday and update the calendar in an sqlite database

import investpy
import sqlite3
import datetime
import pandas as pd
import numpy as np
import os
import time
from prefect import task, flow
import json

curr_date = datetime.datetime.now().date()

# Database Schema

# Table: economic_calendar
# Columns: event_date, event_time, event_name, event_country, event_importance, event_actual, event_forecast, event_previous

# Function to create the database
@task
def create_database():
    # Connect to the database
    conn = sqlite3.connect('alert_bots.db')
    c = conn.cursor()
    
    # Create the economic_calendar table
    c.execute('''
        CREATE TABLE IF NOT EXISTS economic_calendar
        (
            event_datetime text,
            event_name text,
            event_currency text,
            event_zone text,
            event_importance text,
            event_actual text,
            event_forecast text,
            event_previous text
        )
    ''')
    
    # Commit the changes
    conn.commit()
    
    # Close the connection
    conn.close()

def load_calendar_config():
    try:
        with open('./data/calendar_config.json', 'r') as f:
            return json.load(f).values()
    except:
        return ['All'], ['All'], ['All']

# Function to retrieve economic calendar for the upcoming week
@task
def get_next_week_calendar():
    # Get the economic calendar for the upcoming week Paris time
    eow = (curr_date + datetime.timedelta(days=(6 - curr_date.weekday())))
        
    countries, currencies, importances = load_calendar_config()
    
    countries = countries if countries != ['All'] else None
    currencies = currencies if currencies != ['All'] else None
    importances = importances if importances != ['All'] else None
    
    print(countries, currencies, importances)
    
    news = investpy.news.economic_calendar(time_zone='GMT', countries=countries, importances=importances, from_date=curr_date.strftime('%d/%m/%Y'), to_date=eow.strftime('%d/%m/%Y'))
    
    # countrie, importances and currencies params are not working in the api call, need to sort through the data by ourselves
    if countries is not None:
        news = news[news['zone'].isin(countries)]
    if currencies is not None:
        news = news[news['currency'].isin(currencies)]
    if importances is not None:
        news = news[news['importance'].isin(importances)]
    
    news.reset_index(drop=True, inplace=True)
    
    # convert to Europe/Paris time
    news['datetime'] = pd.to_datetime(pd.to_datetime(news['date'], format='%d/%m/%Y').dt.strftime('%Y-%m-%d') + ' ' + news['time'].str.replace('All Day', '00:00') + ':00').dt.tz_localize('GMT').dt.tz_convert('Europe/Paris').dt.strftime('%Y-%m-%d %H:%M:%S')
    
    # Connect to the database
    conn = sqlite3.connect('alert_bots.db')
    c = conn.cursor()
    
    # drop all entries in the table from the current date to the end of the week
    c.execute(
        f'''
        DELETE FROM economic_calendar
        WHERE event_datetime BETWEEN '{curr_date}' AND '{eow}'
        '''
    )
    
    print(news)
    
    # Insert the data into the database
    for i in range(news.shape[0]):        
        c.execute('''
            INSERT INTO economic_calendar
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (news['datetime'][i], news['event'][i], news['currency'][i], news['zone'][i], news['importance'][i], news['actual'][i], news['forecast'][i], news['previous'][i]))
        
    # Commit the changes
    conn.commit()
    
    # Close the connection
    conn.close()


if __name__ == '__main__':
    # Create the database
    # conn = sqlite3.connect('alert_bots.db')
    # c = conn.cursor()
    # c.execute('DROP TABLE IF EXISTS economic_calendar')
    # conn.commit()
    # conn.close()
    
    # create_database()
    
    # # Get the economic calendar for the upcoming week
    get_next_week_calendar()
    
    # # Schedule the script to run every sunday
    # while True:
    #     if datetime.datetime.now().weekday() == 6:
    #         # Get the economic calendar for the upcoming week
    #         get_next_week_calendar()
    #         time.sleep(86400)
    #     else:
    #         time.sleep(86400)