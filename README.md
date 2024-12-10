# Spotify ETL Pipeline

## Overview
This project demonstrates an end-to-end ETL (Extract, Transform, Load) pipeline for Spotify playlist data. The pipeline includes extracting data via the Spotify API, transforming the data for analysis, and loading it into an Azure SQL Database. The project highlights automation using Azure Data Factory (ADF) and modular Python scripting.

## Project Workflow

1. Extract:

* Fetches playlist data using Spotify's API.
* Automates access token generation for secure API authentication.

2. Transform:

* Cleans and formats the data to prepare it for loading.
* Transforms raw JSON data into a structured CSV format.

3. Load:

* Uploads the transformed data into an Azure SQL Database.
* Utilizes pyodbc for seamless database interaction.

4. Automation (via Azure Data Factory):

* Uses Azure Data Factory to automate and schedule the ETL workflow.
* The modular Python script is designed for ADF integration.

## Architecture
The pipeline architecture includes the following components:

* Data Source: Spotify API for fetching playlist data.
* Compute: Local Python environment for processing and transformation.
* Storage: Azure SQL Database for storing processed data.
* Automation: Azure Data Factory for scheduling and managing the workflow.

![arch (5)](https://github.com/user-attachments/assets/5b6b4b6a-340a-4576-b4d7-dc0598395ec7)

## Files Included
1. Spotify_ETL.ipynb:

* A Jupyter Notebook documenting each step of the ETL process with outputs and markdown explanations.
* Designed for clarity and demonstration purposes.

2. spotify_etl_modular.py:

* A modular Python script for the ETL pipeline.
* Prepared for integration with Azure Data Factory.
* Automates token generation, extraction, transformation, and loading.

## Highlights

* Scalable: The modular approach allows for easy integration with cloud services.
* Efficient: Automates data extraction and transformation, saving manual effort.
* Extensible: Can be adapted for other data sources and analytics use cases.

## Future Work

* Develop a Power BI dashboard for data visualization.
* Add more complex transformations and business rules.
* Enable real-time data ingestion using Azure Event Hubs.

## License
This project is licensed under the MIT License.
