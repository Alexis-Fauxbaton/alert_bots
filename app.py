import streamlit as st
import investpy
import json

st.title('Economic Calendar Configuration')

past_news = investpy.news.economic_calendar(time_zone='GMT', from_date='01/01/2024', to_date='31/01/2024')

print(past_news)

# get all available countries by sorting through all news from last month
countries = sorted(past_news['zone'].unique().tolist()) + ['All']
print(past_news['currency'].fillna(value='Non spécifié').unique().tolist())
currencies = sorted(past_news['currency'].fillna(value='Non spécifié').unique().tolist()) + ['All']
importances = sorted(past_news['importance'].fillna(value='Non spécifié').unique().tolist()) + ['All']

st.write('Select countries')

def load_config():
    try:
        with open('./data/calendar_config.json', 'r') as f:
            return json.load(f).values()
    except:
        return ['All'], ['All'], ['All']


default_country, default_currency, default_importance = load_config()

selected_countries = st.multiselect('Countries', countries, default=default_country)
selected_currencies = st.multiselect('Currencies', currencies, default=default_currency)
selected_importances = st.multiselect('Importances', importances, default=default_importance)

# save the configuration as a json file on pressing the save button

config = {
    'countries': selected_countries,
    'currencies': selected_currencies,
    'importances': selected_importances
}

def save_config():
    with open('./data/calendar_config.json', 'w') as f:
        json.dump(config, f)

if st.button('Save Calendar Configuration'):
    save_config()
    st.write('Configuration saved successfully')