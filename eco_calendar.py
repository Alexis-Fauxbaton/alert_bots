import datetime
import os.path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import sqlite3
import sqlitecloud
import pandas as pd

from prefect import task, flow

conn = sqlitecloud.connect('sqlitecloud://cq8ymfazhk.sqlite.cloud:8860/alert_bots?apikey=BeK74nihl8qWNYShYmbJ584DknSnaH2Bi49Nui2OQvE')
c = conn.cursor()

@task
def delete_events(service):
    # conn = sqlite3.connect('alert_bots.db')
    # c = conn.cursor()
    
    curr_date = datetime.datetime.now().date()
    
    # offset = (6 - curr_date.weekday())
    
    offset = 7
    
    time_min = datetime.datetime.combine(curr_date, datetime.time.min).isoformat() + 'Z'
    time_max = datetime.datetime.combine((curr_date + datetime.timedelta(days=offset)), datetime.time.max).isoformat() + 'Z'
    
    events_result = (
        service.events()
        .list(
            calendarId="primary",
            timeMin=time_min,
            timeMax=time_max,
            singleEvents=True,
            orderBy="startTime",
        )
        .execute()
    )
    events = events_result.get("items", [])
            
    for event in events:
        print(f"deleting event: {event['summary']}")
        service.events().delete(calendarId='primary', eventId=event['id']).execute()

@task
def create_events(service):
    # Connect to the database
    # conn = sqlite3.connect('alert_bots.db')
    # c = conn.cursor()
    
    curr_date = datetime.datetime.now().date()
    
    # gather upcoming events from the database up to next sunday included in Paris time and format them for Google Calendar
    
    # offset = (6 - curr_date.weekday())
    
    offset = 7
    
    next_news = c.execute(
        f'''
        SELECT * FROM economic_calendar
        WHERE event_datetime BETWEEN '{curr_date}' AND '{curr_date + datetime.timedelta(days=offset)}'
        ORDER BY event_datetime
        '''
    ).fetchall()

    next_news = pd.DataFrame(
        next_news,
        columns=['event_datetime', 'event_name', 'event_currency', 'event_zone', 'event_importance', 'event_actual', 'event_forecast', 'event_previous']
    )

    for _, row in next_news.iterrows():
        print(row['event_datetime'].replace(' ', 'T'))
        event = {
            'summary': row['event_name'],
            'location': row['event_zone'],
            'colorId': 11 if row['event_importance'] == 'high' else 6 if row['event_importance'] == 'medium' else 1,
            'description': f"Currency: {row['event_currency']}\nImportance: {row['event_importance']}\nActual: {row['event_actual']}\nForecast: {row['event_forecast']}\nPrevious: {row['event_previous']}",
            'start': {
                'dateTime': row['event_datetime'].replace(' ', 'T'),
                'timeZone': 'Europe/Paris',
            },
            'end': {
                'dateTime': row['event_datetime'].replace(' ', 'T'),
                'timeZone': 'Europe/Paris',
            },
        }

        event = service.events().insert(calendarId='primary', body=event).execute()

        print(f'event created: {event.get("htmlLink")}')
    
    next_news = list()
    
    return next_news
        

@flow
def main():
    creds = None
    SCOPES = ['https://www.googleapis.com/auth/calendar']
    
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json')
        
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file('./utils/googleapi_credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        with open('token.json', 'w') as token:
            token.write(creds.to_json())
            
    try:
        service = build('calendar', 'v3', credentials=creds)
                
        delete_events(service)
                
        events = create_events(service)

        events = []

        # Prints the start and name of the next 10 events
        for event in events:
            start = event["start"].get("dateTime", event["start"].get("date"))
            print(start, event["summary"])

    except HttpError as error:
        print(f"An error occurred: {error}")

if __name__ == "__main__":
    main()