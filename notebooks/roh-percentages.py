# Databricks notebook source
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

selected_table = 'S2502'

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog census;
# MAGIC create schema if not exists occupied_housing_units;
# MAGIC use schema occupied_housing_units;

# COMMAND ----------

view_name = 'roh_percentage_total'
query = f"""
CREATE VIEW IF NOT EXISTS {view_name} AS
SELECT
    clean.table_{selected_table}.micro_area as micro_area,
    clean.table_{selected_table}.metro_area as metro_area,
    clean.table_{selected_table}.county as county,
    clean.table_{selected_table}.state as state,
    clean.table_{selected_table}.country as country,
    clean.table_{selected_table}.renter_occupied_housing_units_for_occupied_housing_units as roh_units,
    clean.table_{selected_table}.occupied_housing_units_for_occupied_housing_units as oh_units,
    roh_units / oh_units as roh_percentage
FROM clean.table_{selected_table}

"""
spark.sql(query)

# COMMAND ----------

view_name = 'roh_percentage_race'
query = f"""
CREATE VIEW IF NOT EXISTS {view_name} AS
SELECT
    clean.table_{selected_table}.micro_area as micro_area,
    clean.table_{selected_table}.metro_area as metro_area,
    clean.table_{selected_table}.county as county,
    clean.table_{selected_table}.state as state,
    clean.table_{selected_table}.country as country,
    
    clean.table_{selected_table}.renter_occupied_housing_units_for_white as roh_white,
    clean.table_{selected_table}.renter_occupied_housing_units_for_black_or_african_american as roh_black,
    clean.table_{selected_table}.renter_occupied_housing_units_for_american_indian_and_alaska_native as roh_native_american,
    clean.table_{selected_table}.renter_occupied_housing_units_for_asian as roh_asian,
    clean.table_{selected_table}.renter_occupied_housing_units_for_native_hawaiian_and_other_pacific_islander as roh_hawaiian,
    clean.table_{selected_table}.renter_occupied_housing_units_for_some_other_race as roh_other,
    clean.table_{selected_table}.renter_occupied_housing_units_for_two_or_more_races as roh_two_or_more,
    clean.table_{selected_table}.renter_occupied_housing_units_for_hispanic_or_latino_origin as roh_hispanic,
    clean.table_{selected_table}.renter_occupied_housing_units_for_white_alone_not_hispanic_or_latino as roh_white_alone,

    clean.table_{selected_table}.occupied_housing_units_for_white as oh_white,
    clean.table_{selected_table}.occupied_housing_units_for_black_or_african_american as oh_black,
    clean.table_{selected_table}.occupied_housing_units_for_american_indian_and_alaska_native as oh_native_american,
    clean.table_{selected_table}.occupied_housing_units_for_asian as oh_asian,
    clean.table_{selected_table}.occupied_housing_units_for_native_hawaiian_and_other_pacific_islander as oh_hawaiian,
    clean.table_{selected_table}.occupied_housing_units_for_some_other_race as oh_other,
    clean.table_{selected_table}.occupied_housing_units_for_two_or_more_races as oh_two_or_more,
    clean.table_{selected_table}.occupied_housing_units_for_hispanic_or_latino_origin as oh_hispanic,
    clean.table_{selected_table}.occupied_housing_units_for_white_alone_not_hispanic_or_latino as oh_white_alone,
    
    roh_white / oh_white as roh_percentage_white,
    roh_black / oh_black as roh_percentage_black,
    roh_native_american / oh_native_american as roh_percentage_native_american,
    roh_asian / oh_asian as roh_percentage_asian,
    roh_hawaiian / oh_hawaiian as roh_percentage_hawaiian,
    roh_other / oh_other as roh_percentage_other,
    roh_two_or_more / oh_two_or_more as roh_percentage_two_or_more,
    roh_hispanic / oh_hispanic as roh_percentage_hispanic,
    roh_white_alone / oh_white_alone as roh_percentage_white_alone
FROM clean.table_{selected_table}
"""
spark.sql(query)
