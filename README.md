# NordTask

This repository contains a DAG (Directed Acyclic Graph) implemented in Airflow to extract, transform and load Google Trends data. The DAG is designed to run weekly and extract the search term rankings for a set of countries for a specified time period. The extracted data is transformed and uploaded to a Google BigQuery table.

<strong>Overview</strong>

The trends_dag DAG consists of three tasks:

extract_trends: this task extracts the search term rankings for a set of countries and a specified time period using the Selenium web driver to scrape data from the Google Trends website.

transform_trends: this task transforms the extracted data into a Pandas dataframe format suitable for uploading to BigQuery.

upload_trends: this task uploads the transformed data to a Google BigQuery table.

The extract_trends task takes two optional parameters: date_start and date_end. These parameters specify the start and end dates of the time period for which to extract data. If these parameters are not specified, the task will extract data for the previous seven days. The dates can be specified in the format mm/dd/yy, mm/dd/yyyy, yyyy-mm-dd, or dd-Mon-yy.

The transform_trends task takes a single parameter countries, which is a string containing a list of dictionaries. Each dictionary contains the search term rankings for a single country and time period. The task transforms this data into a Pandas dataframe with the following columns: country, search_term, rank, share, date_start, date_end, upload_date. The upload_date column is added automatically and contains the current date.

The upload_trends task uploads the transformed data to a Google BigQuery table specified by the project_id, dataset_id, and table_id variables. The table must already exist in the specified dataset. The task uses the key_path variable to authenticate with the Google Cloud platform.


<strong>Prerequisites</strong>

Before running the DAG, make sure you have the following installed:

<li>Python 3</li>
<li>Airflow</li>
<li>Selenium</li>
<li>Pandas</li>
<li>Pandas GBQ</li>
<li>ChromeDriver</li>
<li>Google Cloud SDK</li>


<strong>Configuration</strong>

To run the DAG, you will need to update the following variables in the trends_dag.py file:


<li>path: the path to the ChromeDriver executable on your machine</li>
<li>project_id: the ID of your Google Cloud project</li>
<li>dataset_id: the ID of your BigQuery dataset</li>
<li>table_id: the ID of the table where you want to upload the data</li>
<li>key_path: the path to the JSON key file for your Google service account</li>
