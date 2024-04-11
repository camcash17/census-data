# Databricks notebook source
# MAGIC %pip install census
# MAGIC %pip install us
# MAGIC %pip install censusdata

# COMMAND ----------

import os
import json
import pandas as pd
import numpy as np
import re
import requests

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

import censusdata
from census import Census
from us import states

# COMMAND ----------

# create a SparkSession
spark = SparkSession.builder.getOrCreate()
census_key = ''
selected_year = 2021
selected_table = 'S2502'

c = Census(census_key, year=selected_year)

# COMMAND ----------

state_abbreviations = {
    'AL': 'Alabama',
    'AK': 'Alaska',
    'AZ': 'Arizona',
    'AR': 'Arkansas',
    'CA': 'California',
    'CO': 'Colorado',
    'CT': 'Connecticut',
    'DE': 'Delaware',
    'FL': 'Florida',
    'GA': 'Georgia',
    'HI': 'Hawaii',
    'ID': 'Idaho',
    'IL': 'Illinois',
    'IN': 'Indiana',
    'IA': 'Iowa',
    'KS': 'Kansas',
    'KY': 'Kentucky',
    'LA': 'Louisiana',
    'ME': 'Maine',
    'MD': 'Maryland',
    'MA': 'Massachusetts',
    'MI': 'Michigan',
    'MN': 'Minnesota',
    'MS': 'Mississippi',
    'MO': 'Missouri',
    'MT': 'Montana',
    'NE': 'Nebraska',
    'NV': 'Nevada',
    'NH': 'New Hampshire',
    'NJ': 'New Jersey',
    'NM': 'New Mexico',
    'NY': 'New York',
    'NC': 'North Carolina',
    'ND': 'North Dakota',
    'OH': 'Ohio',
    'OK': 'Oklahoma',
    'OR': 'Oregon',
    'PA': 'Pennsylvania',
    'RI': 'Rhode Island',
    'SC': 'South Carolina',
    'SD': 'South Dakota',
    'TN': 'Tennessee',
    'TX': 'Texas',
    'UT': 'Utah',
    'VT': 'Vermont',
    'VA': 'Virginia',
    'WA': 'Washington',
    'WV': 'West Virginia',
    'WI': 'Wisconsin',
    'WY': 'Wyoming',
    'PR': 'Puerto Rico'
}

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog census;
# MAGIC create schema if not exists clean;
# MAGIC create schema if not exists raw;
# MAGIC use schema raw;

# COMMAND ----------

# get raw data from census api and store to / read from volume
def get_census_data(census_api, fields, file_path, table):
    spark.sql(f"CREATE VOLUME IF NOT EXISTS raw.{table}")

    if os.path.isfile(file_path):
        with open(file_path, "r") as file:
            raw_data = json.load(file)
    else:
        directory = os.path.dirname(file_path)
        if not os.path.exists(directory):
            os.makedirs(directory)

        if fields['type']:
            raw_data = census_api(fields['type'], fields['year'], selected_table)
        
        else:
            if "state_fips" in fields:
                if "county_fips" in fields:   
                    raw_data = census_api((*fields['params'],), '*', '*', year=selected_year)
                else:
                    raw_data = census_api((*fields['params'],), '*', year=selected_year)
            else:
                raw_data = census_api((*fields['params'],), year=selected_year)

        with open(file_path, "w") as file:
            json.dump(raw_data, file)
    
    return pd.DataFrame(raw_data)

# COMMAND ----------

# cleanup data frames
def cleanup_data(data, type):
    column_list = []

    if type == 'county':
        column_list = ['county', 'state']
        data[column_list] = data['NAME'].str.split(', ', expand=True)
        data = data.drop(['GEO_ID'], axis=1)   

    elif type == 'state':
        column_list = ['state']
        data[column_list[0]] = data['NAME']
        data = data.drop(['GEO_ID'], axis=1)   

    column_list.append('country')
    data['country'] = 'United States'

    column_list.append('year')
    data['year'] = selected_year
    data = data[column_list + [col for col in data.columns if col not in column_list]]

    if type == 'us':
        data = data.drop(['GEO_ID', 'us'], axis=1)
        
    data = data.drop('NAME', axis=1)
    data.fillna(0.0, inplace=True)

    return data

# COMMAND ----------

def handle_census_request_data(area_data):
    new_columns = area_data.iloc[0].tolist()
    area_data.columns = new_columns
    area_data = area_data.iloc[1:].copy()

    area_data[["area_name", "state"]] = area_data["NAME"].str.split(",", expand=True)[[0, 1]]

    area_data["type"] = area_data["state"].str[-10:]
    area_data["state"] = area_data["state"].str.slice(1, 3)

    # Add new columns
    area_data["county"] = ""
    area_data["country"] = "United States"
    area_data["year"] = selected_year
    area_data["micro_area"] = area_data.apply(lambda row: row["area_name"] if row["type"] == "Micro Area" else "", axis=1)
    area_data["metro_area"] = area_data.apply(lambda row: row["area_name"] if row["type"] == "Metro Area" else "", axis=1)

    area_data = area_data.drop(["GEO_ID", "NAME", "metropolitan statistical area/micropolitan statistical area", "area_name", "type"], axis=1)
    area_data = area_data.loc[:, ~area_data.columns.str.endswith(('EA', 'M', 'MA'))]

    area_data = area_data[["micro_area", "metro_area", "county", "state", "country", "year"] + [col for col in area_data.columns if col not in ["micro_area", "metro_area", "county", "state", "country", "year"]]]

    area_data["state"] = area_data["state"].map(state_abbreviations)
    area_data.fillna('0.0', inplace=True)

    return area_data

# COMMAND ----------

# parse and sanitze all label names
def get_label_names(labels):
    values_list = []
    
    for value in labels:
        splits = value.split("!!")
        sanitized_splits = []
        for split in splits:
            sanitized_split = re.sub(r'[^a-zA-Z0-9_]+', '_', split)
            sanitized_splits.append(sanitized_split)
        
        values_list.append('!!'.join(sanitized_splits))
    
    return values_list

# COMMAND ----------

# use censusdata for table schema
file_path = f'/Volumes/census/raw/{selected_table}/{selected_year}/schema.json'
fields = {
    'type': 'acs5',
    'year': selected_year,
}

schema_data = get_census_data(censusdata.variable_info.censustable, fields, file_path, selected_table)

# COMMAND ----------

# grab schema details
all_column_names = schema_data.columns.tolist()
new_all_column_names = ['NAME'] + all_column_names

selected_table_name = schema_data.iloc[1, 0]

# gets unique values that are all uppercase (demographic categories)
def get_demographic_categories(schema):
    row_values = schema.iloc[0]
    unique_values = []
    
    for value in row_values:
        splits = value.split("!!")
        for split in splits:
            if isinstance(split, str) and split.isupper():
                unique_values.append(split)

    unique_values = list(set(unique_values))
    return unique_values

demographics = get_demographic_categories(schema_data)

# COMMAND ----------

# get area data
area_url = f'https://api.census.gov/data/{selected_year}/acs/acs5/subject?get=group({selected_table})&for=metropolitan%20statistical%20area/micropolitan%20statistical%20area:*&key={census_key}'

response = requests.get(area_url)

if response.status_code == 200:
    area_data = response.json()
    area_data = pd.DataFrame(area_data)

    area_data = handle_census_request_data(area_data)
else:
    print("Request failed with status code:", response.status_code)

# COMMAND ----------

# get county data
file_path = f'/Volumes/census/raw/{selected_table}/{selected_year}/counties.json'
fields = {
    'params': new_all_column_names,
}

county_data = get_census_data(c.acs5st.state_county, fields, file_path, selected_table)
county_data = cleanup_data(county_data, 'county')

# COMMAND ----------

# get state data
file_path = f'/Volumes/census/raw/{selected_table}/{selected_year}/states.json'
fields = {
    'params': new_all_column_names,
}

state_data = get_census_data(c.acs5st.state, fields, file_path, selected_table)
state_data = cleanup_data(state_data, 'state')

# COMMAND ----------

# get country data
file_path = f'/Volumes/census/raw/{selected_table}/{selected_year}/us.json'
fields = {
    'params': new_all_column_names,
}

us_data = get_census_data(c.acs5st.us, fields, file_path, selected_table)
us_data = cleanup_data(us_data, 'us')

# COMMAND ----------

# transform schema
def transform_schema(schema, type):
    new_schema = schema.copy()
    column_list = []

    if type == 'area':
        column_list = ['micro_area', 'metro_area', 'county', 'state', 'country', 'year']

    elif type == 'county':
        column_list = ['county', 'state', 'country', 'year']

    elif type == 'state':
        column_list = ['state', 'country', 'year']

    else:
        column_list = ['country', 'year']

    new_schema[column_list] = column_list
    new_schema = new_schema[column_list + [col for col in new_schema.columns if col not in column_list]]

    schema_dict = new_schema.T
    label_column_values = schema_dict['label'].tolist()
    new_label_names = get_label_names(label_column_values)
    schema_dict['clean_label'] = new_label_names

    schema_dict['new_label'] = schema_dict['clean_label'].apply(lambda x: "_for_".join([x.split("!!")[1], x.split("!!")[-1]]).lower() if len(x.split("!!")) > 1 else x)

    return schema_dict

# COMMAND ----------

# transform all schemas
area_schema = transform_schema(schema_data, 'area')
county_schema = transform_schema(schema_data, 'county')
state_schema = transform_schema(schema_data, 'state')
us_schema = transform_schema(schema_data, 'us')

# COMMAND ----------

# tranform census data
def tranform_data(data, schema):
    merged_data = pd.merge(data.T, schema, left_index=True, right_index=True, how='left').set_index('new_label').drop(['concept', 'predicateType', 'label', 'clean_label'], axis=1).T

    return merged_data

# COMMAND ----------

# merge and store census data
def merge_and_store(area, county, state, country):
    area_spark_df = spark.createDataFrame(area)

    county_spark_df = spark.createDataFrame(county)
    county_spark_df = county_spark_df.withColumn('micro_area', lit("")).withColumn('metro_area', lit(""))
    county_spark_df = county_spark_df.select('micro_area', 'metro_area', *county_spark_df.columns[:-2])

    state_spark_df = spark.createDataFrame(state)
    state_spark_df = state_spark_df.withColumn('micro_area', lit("")).withColumn('metro_area', lit("")).withColumn('county', lit(""))
    state_spark_df = state_spark_df.select('micro_area', 'metro_area','county', *state_spark_df.columns[:-3])

    country_spark_df = spark.createDataFrame(country)
    country_spark_df = country_spark_df.withColumn('micro_area', lit("")).withColumn('metro_area', lit("")).withColumn('county', lit("")).withColumn('state', lit(""))
    country_spark_df = country_spark_df.select('micro_area', 'metro_area','county', 'state', *country_spark_df.columns[:-4])

    combined_df = country_spark_df.union(state_spark_df).union(county_spark_df).union(area_spark_df)

    table_name = f'census.clean.table_{selected_table}'
    combined_df.write.mode('ignore').saveAsTable(table_name)

    return combined_df

# COMMAND ----------

# tranform and store all census data
clean_area_data = tranform_data(area_data, area_schema)
clean_county_data = tranform_data(county_data, county_schema)
clean_state_data = tranform_data(state_data, state_schema)
clean_us_data = tranform_data(us_data, us_schema)

all_data = merge_and_store(clean_area_data, clean_county_data, clean_state_data, clean_us_data)
