import streamlit as st
import investpy
import json
import datetime
import sqlitecloud
from daily import main
import subprocess

# start the prefect server but keep a way to stop it
prefect_server = subprocess.Popen(['prefect', 'server', 'start'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    

def run_main():
    main

conn = sqlitecloud.connect('sqlitecloud://cq8ymfazhk.sqlite.cloud:8860/alert_bots?apikey=BeK74nihl8qWNYShYmbJ584DknSnaH2Bi49Nui2OQvE')
c = conn.cursor()

# conn.execute('''DROP TABLE IF EXISTS calendar_config''')

# create the config table if it does not exist

c.execute('''
    CREATE TABLE IF NOT EXISTS calendar_config
    (
        u_id INTEGER PRIMARY KEY UNIQUE,
        config TEXT
    )
''')

st.title('Economic Calendar Configuration')

@st.cache_resource()
def get_ecocal_sample():
    past_news = investpy.news.economic_calendar(time_zone='GMT', from_date='01/01/2024', to_date='31/01/2024')
    return past_news

past_news = get_ecocal_sample()

# get all available countries by sorting through all news from last month
countries = sorted(past_news['zone'].unique().tolist()) + ['All']
currencies = sorted(past_news['currency'].fillna(value='Non spécifié').unique().tolist()) + ['All']
importances = sorted(past_news['importance'].fillna(value='Non spécifié').unique().tolist()) + ['All']

st.write('Select news filters for the custom economic calendar')

def load_config():
    try:
        with open('./data/calendar_config.json', 'r') as f:
            d = json.load(f)
            d['min_time'] = datetime.time.fromisoformat(d['min_time'])
            d['max_time'] = datetime.time.fromisoformat(d['max_time'])
            return d.values()
    except Exception as e:
        print("Could not load config", e)
        return ['All'], ['All'], ['All'], datetime.time(0, 0), datetime.time(23, 59)

def load_config_from_db():
    try:
        c.execute('''
            SELECT config FROM calendar_config WHERE u_id = 1
        ''')
        d = json.loads(c.fetchone()[0])
        print("JSON", d)
        d['min_time'] = datetime.time.fromisoformat(d['min_time'])
        d['max_time'] = datetime.time.fromisoformat(d['max_time'])
        return d.values()
    except Exception as e:
        print("Could not load config from db", e)
        return ['All'], ['All'], ['All'], datetime.time(0, 0), datetime.time(23, 59)

default_country, default_currency, default_importance, default_min_time, default_max_time = load_config_from_db()

selected_countries = st.multiselect('Countries', countries, default=default_country)
selected_currencies = st.multiselect('Currencies', currencies, default=default_currency)
selected_importances = st.multiselect('Importances', importances, default=default_importance)
selected_min_time = st.time_input('Minimum time', value=default_min_time).isoformat()
selected_max_time = st.time_input('Maximum time', value=default_max_time).isoformat()

# save the configuration as a json file on pressing the save button

config = {
    'countries': selected_countries,
    'currencies': selected_currencies,
    'importances': selected_importances,
    'min_time': selected_min_time,
    'max_time': selected_max_time
}

def save_config():
    with open('./data/calendar_config.json', 'w') as f:
        json.dump(config, f)

def save_config_to_db():
    # try to update the row with the new config, if it does not exist, insert it
    try:
        c.execute('''
            INSERT INTO calendar_config (u_id, config) VALUES (?, ?)
        ''', (1, json.dumps(config)))
    except Exception as e:
        c.execute('''
            UPDATE calendar_config SET config = ? WHERE u_id = 1
        ''', (json.dumps(config),))
    conn.commit()

if st.button('Save Calendar Configuration'):
    with st.spinner('Saving configuration...'):
        # save_config()
        save_config_to_db()
        st.write('Configuration saved successfully')
        

if st.button('Update Economic Calendar'):
    with st.spinner('Updating calendar...'):
        main()
        st.write('Calendar updated successfully')
