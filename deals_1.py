#!/usr/bin/env python
# coding: utf-8

# In[16]:


import pandas as pd
from dotenv import load_dotenv
import pytz
import requests
import gzip
import json
import os
from io import BytesIO
import streamlit as st
from datetime import datetime, timedelta, date, time

load_dotenv()  # Load variables from .env file

# In[17]:
api_key = os.getenv("MIXPANEL_API_KEY")
project_id = os.getenv("MIXPANEL_PROJECT_ID")

st.title("Mixpanel Deal Summary Dashboard")
start_date = st.date_input("Start Date", datetime.now() - timedelta(days=7))
end_date = st.date_input("End Date", datetime.now())


# In[18]:


start_dt = pytz.timezone('Asia/Kathmandu').localize(datetime.combine(start_date, time.min))  # 00:00:00 of start_date
end_dt = pytz.timezone('Asia/Kathmandu').localize(datetime.combine(end_date, time.max)) 


# In[19]:


events_to_export = ["Deal Won"]

event_array_json = json.dumps(events_to_export)

url = (
    f"https://data-eu.mixpanel.com/api/2.0/export?project_id={project_id}&from_date=2025-01-01&to_date={date.today().strftime('%Y-%m-%d')}&event="
    + event_array_json
)


headers = {
    "accept": "text/plain",
    "authorization": f"Basic {api_key}",
}

response = requests.get(url, headers=headers)

print(response.status_code)
if response.status_code == 200:
    # Parse the JSON response
    data_json = [json.loads(line) for line in response.text.strip().split("\n")]

    # Save data to a JSON file
    with open("int.json", "w", encoding="utf-8") as jsonfile:
        json.dump(data_json, jsonfile, ensure_ascii=False)

    print("Exported data saved as .json")
else:
    print("Error:", response.status_code)

certain_json1 = pd.read_json('int.json')
# Flatten the 'properties' column
properties_df1 = pd.json_normalize(certain_json1['properties'])

# Concatenate the flattened properties with the original DataFrame
df_deals = pd.concat([certain_json1.drop(columns=['properties']), properties_df1], axis=1)


# In[20]:


events_to_export = ["New Payment Made"]

event_array_json = json.dumps(events_to_export)

url = (
   f"https://data-eu.mixpanel.com/api/2.0/export?project_id={project_id}&from_date=2025-01-01&to_date={date.today().strftime('%Y-%m-%d')}&event="
    + event_array_json
)

headers = {
    "accept": "text/plain",
    "authorization": "Basic {api_key}",
}

response = requests.get(url, headers=headers)

print(response.status_code)
if response.status_code == 200:
    # Parse the JSON response
    data_json = [json.loads(line) for line in response.text.strip().split("\n")]

    # Save data to a JSON file
    with open("int.json", "w", encoding="utf-8") as jsonfile:
        json.dump(data_json, jsonfile, ensure_ascii=False)

    print("Exported data saved as .json")
else:
    print("Error:", response.status_code)

certain_json1 = pd.read_json('int.json')
# Flatten the 'properties' column
properties_df1 = pd.json_normalize(certain_json1['properties'])

# Concatenate the flattened properties with the original DataFrame
df_payment = pd.concat([certain_json1.drop(columns=['properties']), properties_df1], axis=1)


# In[21]:


# df = pd.read_csv('deals.csv')
df_deals.drop_duplicates(subset='$insert_id', inplace=True)


# In[22]:


df_deals['email'] = df_deals.apply(
    lambda row: row['distinct_id'] if '@' in str(row['distinct_id']) else row['$distinct_id_before_identity'], axis=1)
df_deals['time'] = pd.to_datetime(df_deals['time'], unit='s', utc=True)
df_deals['time'] = df_deals['time'].dt.tz_convert('Asia/Kathmandu')
df_deals = df_deals[['event', 'email', 'time', 'Deal Pipeline']]


# In[23]:


df_deals = df_deals[~df_deals['Deal Pipeline'].str.contains('Customer|Expired|Cancelled', na=False)]


# In[ ]:





# In[24]:


deals_new = df_deals[~df_deals['Deal Pipeline'].str.contains('Converted', na=False)]
deals_new =deals_new.sort_values(by='time', ascending=False)


# In[25]:


deals_converted = df_deals[df_deals['Deal Pipeline'].str.contains('Converted', na=False)]
deals_converted = deals_converted.sort_values(by='time', ascending=False)


# In[26]:


# start_date = pd.to_datetime(start_date).replace(hour=0, minute=0, second=0).tz_localize('Asia/Kathmandu')
# end_date = pd.to_datetime(end_date).replace(hour=23, minute=59, second=59).tz_localize('Asia/Kathmandu')


# In[28]:


filtered_new_deals = deals_new[(deals_new['time'] >= pd.to_datetime(start_dt)) & (deals_new['time'] <= pd.to_datetime(end_dt))]
filtered_new_deals.reset_index(drop=True, inplace=True)


# In[29]:


filtered_converted_deals = deals_converted[(deals_converted['time'] >= pd.to_datetime(start_dt)) & (deals_converted['time'] <= pd.to_datetime(end_dt))]
filtered_converted_deals.reset_index(drop=True, inplace=True)


# In[30]:


# amount = pd.read_csv('amountandrefunds.csv')
df_payment = df_payment.drop_duplicates(subset='$insert_id')
df_payment['email'] = df_payment.apply(
    lambda row: row['distinct_id'] if '@' in str(row['distinct_id']) else row['$distinct_id_before_identity'], axis=1)
df_payment['time'] = pd.to_datetime(df_payment['time'], unit='s', utc=True)
df_payment['time'] = df_payment['time'].dt.tz_convert('Asia/Kathmandu')


# In[31]:


df_payment = df_payment[['event', 'Amount', 'email', 'time']]
df_payment = df_payment[df_payment['event'] == 'New Payment Made']


# In[32]:


filtered_payment = df_payment[(df_payment['time'] >= pd.to_datetime(start_dt)) & (df_payment['time'] <= pd.to_datetime(end_dt))]
filtered_payment.sort_values(by='time', ascending=True, inplace=True)
filtered_payment.reset_index(drop=True, inplace=True)


# In[33]:


filtered_payment_renamed = filtered_payment.rename(columns={'event': 'payment_event', 'time': 'payment_time'})
payment_new_deals = filtered_new_deals.merge(filtered_payment_renamed, on='email', how='inner')
payment_new_deals = payment_new_deals[['event', 'email', 'Deal Pipeline', 'Amount', 'payment_time']]

# In[34]:


filtered_payment_renamed = filtered_payment.rename(columns={'event': 'payment_event', 'time': 'payment_time'})
payment_converted_deals = filtered_converted_deals.merge(filtered_payment_renamed, on='email', how='inner')
payment_converted_deals = payment_converted_deals[['event', 'email', 'Deal Pipeline', 'Amount', 'payment_time']]

# In[35]:


yesterday = datetime.now().date() - timedelta(days=1)
start_dt = pd.Timestamp(yesterday).replace(hour=0, minute=0, second=0)
end_dt = pd.Timestamp(yesterday).replace(hour=23, minute=59, second=59)

# If your datetime column is timezone-aware (like 'Asia/Kathmandu'), localize start/end too
start_dt = start_dt.tz_localize('Asia/Kathmandu')
end_dt = end_dt.tz_localize('Asia/Kathmandu')

# Apply the filter
filtered_new_deals_yesterday = deals_new[(deals_new['time'] >= start_dt) & (deals_new['time'] <= end_dt)]
filtered_new_deals_yesterday.reset_index(drop=True, inplace=True)

filtered_converted_deals_yesterday = deals_converted[(deals_converted['time'] >= pd.to_datetime(start_dt)) & (deals_converted['time'] <= pd.to_datetime(end_dt))]
filtered_converted_deals_yesterday.reset_index(drop=True, inplace=True)

filtered_payment_yesterday = df_payment[(df_payment['time'] >= pd.to_datetime(start_dt)) & (df_payment['time'] <= pd.to_datetime(end_dt))]
filtered_payment_yesterday_renamed = filtered_payment_yesterday.rename(columns={'event': 'payment_event', 'time': 'payment_time'})
payment_new_deals_yesterday = filtered_new_deals_yesterday.merge(filtered_payment_yesterday_renamed, on='email', how='inner')
payment_converted_deals_yesterday = filtered_converted_deals_yesterday.merge(filtered_payment_yesterday_renamed, on='email', how='inner')

payment_new_deals_yesterday = payment_new_deals_yesterday[['event', 'email', 'Deal Pipeline', 'Amount', 'payment_time']]
payment_converted_deals_yesterday = payment_converted_deals_yesterday[['event', 'email', 'Deal Pipeline', 'Amount', 'payment_time']]

# In[36]:


summary = {
    'Total Deals Won From Already Converted Users' : len(filtered_converted_deals),
    'Total Payment From Already Converted Users' : payment_converted_deals['Amount'].astype(float).sum(),
    'Total Deals Won From New Deals' : len(filtered_new_deals),
    'Total Payment From New Deals' : payment_new_deals['Amount'].astype(float).sum(),
        '-------------------------------------------------------' : '------------',
    'Total Deals Won From Already Converted Users (Yesterday)' : len(filtered_converted_deals_yesterday),
    'Total Payment From Already Converted Users (Yesterday)' : payment_converted_deals_yesterday['Amount'].astype(float).sum(),
    'Total Deals Won From New Deals (Yesterday)' : len(filtered_new_deals_yesterday),
    'Total Payment From New Deals (Yesterday)' : payment_new_deals_yesterday['Amount'].astype(float).sum()
}

summary_df = pd.DataFrame(list(summary.items()), columns=['Metric', 'Value'])


# In[37]:


st.dataframe(summary_df, use_container_width=True)


# In[38]:


with st.expander("🔍 Payment From Already Converted Users"):
    st.dataframe(payment_converted_deals)

with st.expander("🔍 Deals Won From Already Converted Users"):
    st.dataframe(filtered_converted_deals)

with st.expander("🔍 Payment From New Deals"):
    st.dataframe(payment_new_deals)

with st.expander("🔍 Deals Won From New Deals"):
    st.dataframe(filtered_new_deals)

with st.expander("🕒 Payment From Already Converted Users Yesterday"):
    st.dataframe(payment_converted_deals_yesterday)

with st.expander("🕒 Deals Won From Already Converted Users Yesterday"):
    st.dataframe(filtered_converted_deals_yesterday)

with st.expander("🕒 Payment From New Deals Yesterday"):
    st.dataframe(payment_new_deals_yesterday)

with st.expander("🕒 Deals Won From New Deals Yesterday"):
    st.dataframe(filtered_new_deals_yesterday)






