import asyncio
import csv
import logging
import subprocess
from time import sleep
import boto3
import pandas as pd
import requests
import math
from repair_link import get_product_sitemap

# Set up logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def read_sql_file(sql_file):
    with open(sql_file, 'r') as file:
        return file.read()

def execute_athena_query(query_string, athena_client, output_location='s3://dst-workbench/mykyta/athena-trash'):
    response = athena_client.start_query_execution(
        QueryString=query_string,
        ResultConfiguration={'OutputLocation': output_location}
    )
    execution_id = response['QueryExecutionId']
    return execution_id

def wait_for_query_completion(athena_client, execution_id):
    while True:
        query_execution = athena_client.get_query_execution(QueryExecutionId=execution_id)
        state = query_execution['QueryExecution']['Status']['State']
        if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            return state
        sleep(5)

def fetch_query_results_to_dataframe(athena_client, execution_id, column_names):
    # Initialize the list to hold query results
    query_results = []

    # Fetch the results
    next_token = None
    while True:
        response = athena_client.get_query_results(
            QueryExecutionId=execution_id,
            NextToken=next_token) if next_token else athena_client.get_query_results(
            QueryExecutionId=execution_id)

        # Add rows to the results list
        query_results.extend(response['ResultSet']['Rows'])

        # Check if there are more results
        next_token = response.get('NextToken', None)
        if not next_token:
            break

    # Transform the results into a DataFrame
    # Skip the first row if it contains headers
    if query_results:
        df = pd.DataFrame([r['Data'] for r in query_results[1:]], columns=column_names)
        # Convert list of dicts to dataframe, handling type conversion as needed
        for column in df.columns:
            df[column] = df[column].apply(lambda x: x['VarCharValue'] if x else None)
        return df
    else:
        return pd.DataFrame()

def check_404(url):
    try:
        # Attempt HTTPS first
        r = requests.get("https://www." + url)
        r.raise_for_status()  # Check for HTTP errors
    except requests.exceptions.RequestException as e:
        # Fallback to HTTP if HTTPS fails
        try:
            r = requests.get("http://www." + url)
            logging.warning(f"HTTPS request failed: %s", e)
        except:
            return True

    # Check if the status code is 404
    if r.status_code == 404:
        return True
    else:
        return False

def repair_link(products_links, domain_df):
    #take row from df with domains which have to be repaired
    if len(products_links) != 0:
        for index, row in domain_df.iterrows():
            product_handle_1 = row["producthandle"]
            product_handle_2 = row["url"].split("/")[-1]
            product_id = str(row["id"].split("_")[-1])
            variant_id = str(row["variantid"])
            element = dict()
            for product_link in products_links:
                element["id"] = row["id"]
                if product_handle_1 in product_link or product_id in product_link or variant_id in product_link or product_handle_2 in product_link:
                    element["url"] = product_link
            with open("data/output.csv", mode='a', newline='') as file:
                writer = csv.DictWriter(file, fieldnames=["id", "url"],
                                        delimiter=',')
                writer.writerow(element)

async def main():
    athena_client = boto3.client('athena', region_name='eu-central-1')
    query_string = read_sql_file("sql_query/get_metadata.sql")

    execution_id = execute_athena_query(query_string, athena_client)
    state = wait_for_query_completion(athena_client, execution_id)
    if state != 'SUCCEEDED':
        raise Exception(f"Query execution failed with state: {state}")
    print(f"Query executed successfully.")
    urls = fetch_query_results_to_dataframe(athena_client, execution_id, ["id", "url", "producthandle", "createdate", "variantid", "domain"])
    #urls.to_csv('data/urls.csv', index=False)
    # Creating a DataFrame with columns 'id' and 'url'
    data = {
        'id': [],
        'url': []
    }
    df = pd.DataFrame(data)
    # Saving the DataFrame to a CSV file
    file_path = 'data/output.csv'
    df.to_csv(file_path, index=False)
    # urls = pd.read_csv('data/urls.csv')
    # Get all unique domains
    unique_domains_list = urls['domain'].unique()
    unique_domains_list = unique_domains_list.tolist()
    # Iterate through domains
    for domain in unique_domains_list:
        try:
            logging.warning(f"Checking domain: %s", domain)
            # Get random sample of 10 elements
            try:
                sample = urls[urls["domain"] == domain].sample(n=10)
            except:
                sample = urls[urls["domain"] == domain]
            # Count number of 404 errors
            error_count = 0
            for i in range(len(sample)):
                url = sample["url"].tolist()[i]
                if check_404(url):
                    error_count += 1
            # Condition for detecting wrong URL structure
            if error_count >= math.ceil(len(sample)/2):
                products_links = await get_product_sitemap(domain)
                domain_df = urls[urls["domain"] == domain]
                logging.warning(f"%s's URL structure might be wrong. Error count: %s", domain, error_count)
                repair_link(products_links, domain_df)
            else:
                logging.warning(f"%s's URL structure is correct. Error count: %s", domain, error_count)
            urls = urls[urls["domain"] != domain]
        except Exception as e:
            logging.warning(f"An error occurred: %s", e)
    #Save output.csv on s3
    bucket_name = 'dst-workbench'
    file_key_1 = 'mykyta/repair_url/output.csv'
    full_sync_query = f'''
                    aws s3 cp data/output.csv s3://{bucket_name}/{file_key_1}
                    '''
    subprocess.call(full_sync_query, shell=True)

if __name__ == "__main__":
    asyncio.run(main())