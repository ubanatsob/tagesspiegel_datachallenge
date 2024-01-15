# tagesspiegel_datachallenge

## Description

This project is a Python script for fetching data from the Visual Crossing Weather API based on specified parameters, transforms the data and storing it in Google BigQuery.

## Requirements

Before running the script, make sure to install the required dependencies by running:

```bash
pip install -r requirements.txt
``` 
## Usage

```bash
python api_service.py START_DATE END_DATE LOCATION API_KEY GOOGLE_APPLICATION_CREDENTIALS_PATH BIG_QUERY_TARGET_ID
``` 
Replace the placeholders with actual values:

- **START_DATE:** The start date for data retrieval.
- **END_DATE:** The end date for data retrieval.
- **LOCATION:** The location parameter for the API.
- **API_KEY:** Your API key for authentication.
- **GOOGLE_APPLICATION_CREDENTIALS_PATH:** The file path to your Google Cloud Platform service account credentials JSON file.
- **BIG_QUERY_TARGET_ID:** The target BigQuery dataset ID.


## Example

```bash
python api_service.py '2024-01-01' '2024-01-15' 'Berlin, Germany' your_api_key_here /path/to/credentials.json your_bigquery_dataset_id
``` 
