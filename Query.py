import boto3
import time
import logging
from botocore.exceptions import ClientError

# Ref: https://frankcorso.dev/querying-athena-python.html

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

# Initialize Athena client
client = boto3.client('athena')


def run_query(query, database, output_location):
    try:
        response = client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': database},
            ResultConfiguration={'OutputLocation': output_location}
        )
        return response['QueryExecutionId']
    except ClientError as e:
        logger.error(f"Error running query: {e}")
        return None


def check_query_status(query_execution_id, retries=5):
    status = None
    for attempt in range(retries):
        try:
            response = client.get_query_execution(QueryExecutionId=query_execution_id)
            status = response['QueryExecution']['Status']['State']
            if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                break
        except ClientError as e:
            logger.error(f"Error checking query status: {e}")
            if attempt < retries - 1:
                time.sleep(5)  # wait for 5 seconds before retrying the request
            else:
                raise

        time.sleep(2)  # Wait for status

    if status == 'FAILED':
        raise Exception(f"Query failed: {response['QueryExecution']['Status']['StateChangeReason']}")
    elif status == 'CANCELLED':
        raise Exception("Query was cancelled.")
    return status


def fetch_results(query_execution_id):
    try:
        results = client.get_query_results(QueryExecutionId=query_execution_id)
        return results['ResultSet']
    except ClientError as e:
        logger.error(f"Error fetching query results: {e}")
        raise


def execute_athena_query(query, database, output_location):
    # logger.info(f"Running query: {query}")
    query_execution_id = run_query(query, database, output_location)
    logger.info(f"Query execution id: {query_execution_id}")

    if query_execution_id:
        status = check_query_status(query_execution_id)
        if status == 'SUCCEEDED':
            logger.info(f"Query execution succeeded: {query_execution_id}")
            query_results = fetch_results(query_execution_id)
            return query_results
        else:
            logger.error(f"Query did not succeed. Status: {status}")
    else:
        logger.error("Failed to start query execution.")
    return None


if __name__ == '__main__':
    # Configuration for Athena query
    DATABASE_NAME = 'curated_dev_adform'
    OUTPUT_LOCATION = 's3://aws-athena-query-results-227956463654-eu-central-1/'
    QUERY_STRING = '''
    SELECT * 
    FROM "curated_dev_adform"."dev_campaign_performance_daily_mediaplus_czech_republic" 
    ORDER BY 'date'
    LIMIT 10;
    '''

    # Run the query
    results = execute_athena_query(QUERY_STRING, DATABASE_NAME, OUTPUT_LOCATION)

    # Display the results
    if results:
        logger.info("Query Results:")
        for row in results['Rows']:
            logger.info(row['Data'])
