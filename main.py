import asyncio
import csv
import logging
import os
import subprocess
import time

import boto3
import pandas as pd
import requests
import math
from repair_link import get_product_sitemap

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

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


async def wait_for_query_completion(athena_client, execution_id):
    while True:
        query_execution = athena_client.get_query_execution(QueryExecutionId=execution_id)
        state = query_execution['QueryExecution']['Status']['State']
        if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            return state
        await asyncio.sleep(5)  # Non-blocking sleep


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


async def check_404(url):
    try:
        # Attempt HTTPS first with verify=False to suppress warnings about unverified HTTPS requests
        r = requests.get("https://www." + url, verify=False)
        r.raise_for_status()  # Check for HTTP errors
    except requests.exceptions.RequestException as e:
        # Fallback to HTTP if HTTPS fails
        try:
            r = requests.get("http://www." + url, verify=False)
            logging.warning(f"HTTPS request failed: %s", e)
        except requests.exceptions.RequestException:
            return True

    # Check if the status code is 404
    return r.status_code == 404


def repair_link(products_links, domain_df):
    elements = []
    if len(products_links) != 0:
        for index, row in domain_df.iterrows():
            product_handle_1 = row["producthandle"]
            product_handle_2 = row["url"].split("/")[-1]
            product_id = str(row["id"].split("_")[-1])
            for product_link in products_links:
                element = {"id": row["id"]}
                if product_handle_1 in product_link or product_id in product_link or product_handle_2 in product_link:
                    element["url"] = product_link
                    elements.append(element)
                    break
            elements.append(element)

    # Write all elements to the file in one go
    with open("data/output.csv", mode='a', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=["id", "url"], delimiter=',')
        writer.writerows(elements)


async def run_query(athena_client, file_name, columns, output_df):
    query_string = read_sql_file(file_name)

    execution_id = execute_athena_query(query_string, athena_client)

    try:
        state = await wait_for_query_completion(athena_client, execution_id)
    except Exception as e:
        logging.error(f"Error while waiting for query completion: {e}")
        athena_client.stop_query_execution(QueryExecutionId=execution_id)
        return None

    if state != 'SUCCEEDED':
        logging.error(f"Query execution failed with state: {state}")
        return None
    logging.info(f"Query executed successfully.")
    if output_df:
        urls = fetch_query_results_to_dataframe(athena_client, execution_id, columns)
        return urls
    else:
        return None


def process_domain(domain, sample, products_links):
    elements = []
    for link in products_links:
        element = {
            "domain": domain,
            "url": link,
            "producthandle": link.split("/")[-1]
        }
        elements.append(element)
    return elements


async def find_incorrect_domains():
    # Read AWS credentials from environment variables
    aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')

    athena_client = boto3.client('athena', region_name='eu-central-1', aws_access_key_id=aws_access_key_id,
                                 aws_secret_access_key=aws_secret_access_key)
    #athena_client = boto3.client('athena', region_name='eu-central-1')
    # Drop old table
    await run_query(athena_client, "sql_query/drop_url_repair_meta_data.sql", None, False)
    # Create table
    await run_query(athena_client, "sql_query/create_url_meta_data.sql", None, False)
    # Get all domains
    urls = await run_query(athena_client, "sql_query/data_to_find_wrong_urls.sql", ["url", "domain"], True)

    # Handle case where the query fails
    if urls is None or urls.empty:
        logging.warning("No URLs found from query.")
        return

    # Get all unique domains
    unique_domains_list = urls['domain'].unique().tolist()

    # Table to store domains to repair
    file_path = 'data/incorrect_domains.csv'
    with open(file_path, mode='w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=["domain", "url", "producthandle"], delimiter=',')
        writer.writeheader()

    for domain in unique_domains_list:
        logging.warning(f"Checking domain: {domain}")
        sample = urls[urls["domain"] == domain]

        # Check URLs concurrently
        url_list = sample["url"].tolist()
        tasks = [check_404(url) for url in url_list]
        results = await asyncio.gather(*tasks)

        error_count = sum(results)
        if error_count >= math.ceil(len(sample) / 2):
            logging.info(f"Domain {domain} should be repaired")
            products_links = await get_product_sitemap(domain)
            logging.info("Retrieving all links")
            elements = process_domain(domain, sample, products_links)
            # Write all elements to the file in one go
            with open(file_path, mode='a', newline='') as file:
                writer = csv.DictWriter(file, fieldnames=["domain", "url", "producthandle"], delimiter=',')
                writer.writerows(elements)

    # Save output.csv on s3
    bucket_name = 'dst-workbench'
    file_key_1 = 'mykyta/broken_domains/incorrect_domains.csv'
    full_sync_query = f'''
                            aws s3 cp data/incorrect_domains.csv s3://{bucket_name}/{file_key_1}
                            '''
    subprocess.call(full_sync_query, shell=True)

    # Run msk repair to update data
    await run_query(athena_client, "sql_query/msk_repair.sql", None, False)
    #drop staging table
    try:
        await run_query(athena_client, "sql_query/drop_staging.sql", None, False)
    except:
        pass
    #create staging table
    await run_query(athena_client, "sql_query/create_url_staging.sql", None, False)
    #msck repair staging
    await run_query(athena_client, "sql_query/msck_repair_staging.sql", None, False)
    # drop main table
    try:
        await run_query(athena_client, "sql_query/drop_repair_url.sql", None, False)
    except:
        pass
    #create final table
    await run_query(athena_client, "sql_query/create_repair_url.sql", None, False)
    #insert data in the final table
    await run_query(athena_client, "sql_query/add_data_to_repair_url.sql", None, False)
    #msck repait final table
    await run_query(athena_client, "sql_query/msck_repair_url.sql", None, False)

if __name__ == "__main__":
    asyncio.run(find_incorrect_domains())
