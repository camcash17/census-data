# Census Data Analysis for Affordable Housing Development

This project aims to develop a scalable and efficient data pipeline for analyzing publicly available Census data relevant to affordable housing development projects. The insights generated will assist real estate professionals, often with limited technical expertise, in making data-driven decisions.

## Project Background

**Report Purpose:** This project will generate a report providing valuable information to evaluate potential areas for developing or preserving affordable housing.

## Project Goals

* **Develop a Scalable Report:**  The report should be adaptable to different real estate development projects across the country and utilize any relevant Census dataset.
* **Census API Integration:** Understand interaction with the Census API(s) and document the process for easy integration.
* **Local Development Environment:** Create a user-friendly local development environment with a clear onboarding process.
* **Collaboration:** Establish efficient communication patterns, work allocation strategies, and decision-tracking mechanisms for a collaborative workflow.

## List Items

* **Report Design:** Determine the desired format and content of the PDF report.
* **Data Exploration:** Identify the specific data points most valuable for analysis.
* **Census Data Access:** Obtain a Census Bureau API access token (avoid storing in plain text).
* **Dev Environment Setup:** Create a local development environment with clear instructions.
* **Test Pipeline Construction:**
    * Implement calls to the Census API using the `census` library (https://pypi.org/project/census/).
    * Store retrieved data in an S3 bucket.
    * Load data into a pandas DataFrame for further manipulation.
    * Perform any necessary data cleaning and transformations.
    * Visualize the data using appropriate charts.
    * Deliver a parameterized dashboard with 2-3 key visualizations.
* **Scalability Improvements:** Optimize the pipeline to efficiently handle multiple Census datasets.
* **Cost Optimization:** Identify cost-effective strategies for data pipelines.
* **Machine Learning Exploration:** Investigate potential machine learning applications for future demographic forecasting.

## Resources

* Trello Board: Project management and task tracking
* Google Drive Folder: Document storage and collaboration
* Lucidchart Diagram: Workflow and data flow visualization
* GitHub Repo: Source code repository
* System Design Documentation: System architecture details

## Data Sources

* **Census Tables:**
    * DP05 – ACS Demographic and Housing Estimates
    * S1501 – Educational Attainment
    * S1901 – Income in the Past 12 Months (average per state, per month)
    * S1903 – Median Income in the Past 12 Months
    * S1701 – Poverty Status in the Past 12 Months
    * S2502 – Demographic Characteristics for Occupied Housing Units
    * B25106 – Tenure by Housing Cost as a Percent of Household Income in the Past 12 Months
    * DP04 – Selected Housing Characteristics
* **FFIEC:**
    * https://www.ffiec.gov
    * https://www.ffiec.gov/hmda/
    * https://ffiec.cfpb.gov

## Census API Learnings

* **Access Token:** Obtain an API access token from https://api.census.gov/data/key_signup.html (avoid storing in plain text).
* **API Calls:**
    * User Guide: https://www.census.gov/content/dam/Census/data/developers/api-user-guide/api-guide.pdf
    * All Groups for a Table: https://api.census.gov/data/2016/acs/acs5/subject/groups/S1901.html
    * All Variables for a Table: https://api.census.gov/data/2021/acs/acs1/subject/variables.html
    * Example Call: https://api.census.gov/data/2019/acs/acs1/subject?get=group(S0101)&for=state:*

## Tables Required

* FIPS code reference tables: https://www.census.gov/library/reference/code-lists/ansi.html
* Reference tables for variable names: https://api.census.gov/data/2016/acs/acs5/subject/groups/S1901/

## Example Visualizations

**Visualization 1**

![Screenshot 2024-04-11 at 9 05 15 AM](https://github.com/camcash17/census-data/assets/32401001/74e86aa3-264a-473d-a5bc-aaa9ea529db2)

**Visualization 2**

![Screenshot 2024-04-11 at 9 05 31 AM](https://github.com/camcash17/census-data/assets/32401001/981726ab-773c-4d92-a3ee-d313a502bc96)
