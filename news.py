# This script is used to retrieve economic calendar for the upcoming week every sunday and update the calendar in an sqlite database

import investpy
import sqlite3
import sqlitecloud
import datetime
import pandas as pd
import numpy as np
import json

curr_date = datetime.datetime.now().date()

# Database Schema

# Table: economic_calendar
# Columns: event_date, event_time, event_name, event_country, event_importance, event_actual, event_forecast, event_previous

# Function to create the database
def create_table():
    # Connect to the database
    # conn = sqlite3.connect('alert_bots.db')
    conn = sqlitecloud.connect('sqlitecloud://cq8ymfazhk.sqlite.cloud:8860/alert_bots?apikey=BeK74nihl8qWNYShYmbJ584DknSnaH2Bi49Nui2OQvE')
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
        return ['All'], ['All'], ['All'], datetime.time(0, 0), datetime.time(23, 59)

def load_calendar_config_from_db():
    conn = sqlitecloud.connect('sqlitecloud://cq8ymfazhk.sqlite.cloud:8860/alert_bots?apikey=BeK74nihl8qWNYShYmbJ584DknSnaH2Bi49Nui2OQvE')
    c = conn.cursor()
    try:
        c.execute('''
            SELECT config FROM calendar_config WHERE u_id = 1
        ''')
        return json.loads(c.fetchone()[0]).values()
    except:
        return ['All'], ['All'], ['All'], datetime.time(0, 0), datetime.time(23, 59)

# Function to retrieve economic calendar for the upcoming week
def get_next_week_calendar():
    # Get the economic calendar for the upcoming week Paris time
    # eow = (curr_date + datetime.timedelta(days=(6 - curr_date.weekday())))
    offset = 7
    
    eow = curr_date + datetime.timedelta(days=offset)
        
    countries, currencies, importances, min_time, max_time = load_calendar_config_from_db()
    
    countries = countries if countries != ['All'] else None
    currencies = currencies if currencies != ['All'] else None
    importances = importances if importances != ['All'] else None
    
    print(curr_date, eow)
    
    news = investpy.news.economic_calendar(time_zone='GMT +2:00', countries=countries, importances=importances, from_date=curr_date.strftime('%d/%m/%Y'), to_date=eow.strftime('%d/%m/%Y'))
    
    # countrie, importances and currencies params are not working in the api call, need to sort through the data by ourselves
    if countries is not None:
        news = news[news['zone'].isin(countries)]
    if currencies is not None:
        news = news[news['currency'].isin(currencies)]
    if importances is not None:
        news = news[news['importance'].isin(importances)]
    print(min_time, max_time)
    news = news.loc[(news['time'] >= min_time) & (news['time'] <= max_time)]
        
    news.reset_index(drop=True, inplace=True)
    
    # convert to Europe/Paris time
    news['datetime'] = pd.to_datetime(pd.to_datetime(news['date'], format='%d/%m/%Y').dt.strftime('%Y-%m-%d') + ' ' + news['time'].str.replace('All Day', '00:00') + ':00').dt.strftime('%Y-%m-%d %H:%M:%S')
    
    # Connect to the database
    # conn = sqlite3.connect('alert_bots.db')
    # conn = sqlitecloud.connect('sqlitecloud://cq8ymfazhk.sqlite.cloud:8860?apikey=BeK74nihl8qWNYShYmbJ584DknSnaH2Bi49Nui2OQvE')
    conn = sqlitecloud.connect('sqlitecloud://cq8ymfazhk.sqlite.cloud:8860/alert_bots?apikey=BeK74nihl8qWNYShYmbJ584DknSnaH2Bi49Nui2OQvE')
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
    
    create_table()
    
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