# Databricks notebook source
# MAGIC %pip install census
# MAGIC %pip install us
# MAGIC %pip install numpy

# COMMAND ----------

from census import Census
from us import states
import numpy as np

c = Census("33e4cdca31227ea08bbeef5c8edb6df7909018c7", year=2021)

# COMMAND ----------

import pandas as pd

data = {
    'Column Name': [
        'S2502_C05_001E',
        'S2502_C05_010E',
        'S2502_C05_003E',
        'S2502_C05_009E',
        'S2502_C05_005E',
        'S2502_C05_008E',
        'S2502_C05_004E',
        'S2502_C05_006E',
        'S2502_C05_007E',
    ],
    'Label': [
        'Renter-occupied housing units',
        'White alone, not Hispanic or Latino',
        'Black or African American',
        'Hispanic or Latino origin',
        'Asian',
        'Two or more races',
        'American Indian and Alaska Native',
        'Native Hawaiian and Other Pacific Islander',
        'Some other race',
    ]
}

labels = pd.DataFrame(data)
labels

# COMMAND ----------

# Get US / MI data for demographics
# Get US / MI data for % ROH units
# Either 27% of renter occupied housing units are black / or 57% of black occupied housing units are renters (report)

roh_mi_county_data = c.acs5st.state_county((
    'NAME',
    'S2502_C05_001E',
    'S2502_C05_010E',
    'S2502_C05_003E',
    'S2502_C05_009E',
    'S2502_C05_005E',
    'S2502_C05_008E',
    'S2502_C05_004E',
    'S2502_C05_006E',
    'S2502_C05_007E',
), states.MI.fips, '*')

oh_mi_county_data = c.acs5st.state_county((
    'NAME',
    'S2502_C01_001E',
    'S2502_C01_010E',
    'S2502_C01_003E',
    'S2502_C01_009E',
    'S2502_C01_005E',
    'S2502_C01_008E',
    'S2502_C01_004E',
    'S2502_C01_006E',
    'S2502_C01_007E',
), states.MI.fips, '*')

roh_mi_county_df = pd.DataFrame(roh_mi_county_data)
oh_mi_county_df = pd.DataFrame(oh_mi_county_data)

roh_mi_county = roh_mi_county_df.drop(['state', 'county'], axis=1)
oh_mi_county = oh_mi_county_df.drop(['state', 'county'], axis=1)

roh_mi_county_column_names = [col for col in roh_mi_county.columns if col != 'NAME']
oh_mi_county_column_names = [col for col in oh_mi_county.columns if col != 'NAME']

mi_county = roh_mi_county.copy()
mi_county[roh_mi_county_column_names] = np.divide(roh_mi_county[roh_mi_county_column_names], oh_mi_county[oh_mi_county_column_names]).multiply(100).round(2)

mi_county

# COMMAND ----------

roh_mi_data = c.acs5st.state((
    'NAME',
    'S2502_C05_001E',
    'S2502_C05_010E',
    'S2502_C05_003E',
    'S2502_C05_009E',
    'S2502_C05_005E',
    'S2502_C05_008E',
    'S2502_C05_004E',
    'S2502_C05_006E',
    'S2502_C05_007E',
), states.MI.fips)

oh_mi_data = c.acs5st.state((
    'NAME',
    'S2502_C01_001E',
    'S2502_C01_010E',
    'S2502_C01_003E',
    'S2502_C01_009E',
    'S2502_C01_005E',
    'S2502_C01_008E',
    'S2502_C01_004E',
    'S2502_C01_006E',
    'S2502_C01_007E',
), states.MI.fips)

roh_mi_df = pd.DataFrame(roh_mi_data)
oh_mi_df = pd.DataFrame(oh_mi_data)

roh_mi = roh_mi_df.drop(['state'], axis=1)
oh_mi = oh_mi_df.drop(['state'], axis=1)

roh_mi_column_names = [col for col in roh_mi.columns if col != 'NAME']
oh_mi_column_names = [col for col in oh_mi.columns if col != 'NAME']

mi = roh_mi.copy()
mi[roh_mi_column_names] = np.divide(roh_mi[roh_mi_column_names], oh_mi[oh_mi_column_names]).multiply(100).round(2)

mi

# COMMAND ----------

roh_us_data = c.acs5st.us((
    'NAME',
    'S2502_C05_001E',
    'S2502_C05_010E',
    'S2502_C05_003E',
    'S2502_C05_009E',
    'S2502_C05_005E',
    'S2502_C05_008E',
    'S2502_C05_004E',
    'S2502_C05_006E',
    'S2502_C05_007E',
))

oh_us_data = c.acs5st.us((
    'NAME',
    'S2502_C01_001E',
    'S2502_C01_010E',
    'S2502_C01_003E',
    'S2502_C01_009E',
    'S2502_C01_005E',
    'S2502_C01_008E',
    'S2502_C01_004E',
    'S2502_C01_006E',
    'S2502_C01_007E',
))

roh_us_df = pd.DataFrame(roh_us_data)
oh_us_df = pd.DataFrame(oh_us_data)

roh_us = roh_us_df.drop(['us'], axis=1)
oh_us = oh_us_df.drop(['us'], axis=1)

roh_us_column_names = [col for col in roh_us.columns if col != 'NAME']
oh_us_column_names = [col for col in oh_us.columns if col != 'NAME']

us = roh_us.copy()
us[roh_us_column_names] = np.divide(roh_us[roh_us_column_names], oh_us[oh_us_column_names]).multiply(100).round(2)

us_mi = us.append(mi, ignore_index=True)

attachment1 = us_mi.append(mi_county, ignore_index=True)

attachment1

# COMMAND ----------


