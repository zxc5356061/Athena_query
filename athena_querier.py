import time
import logging

import boto3
from botocore.exceptions import ClientError

# Ref: https://frankcorso.dev/querying-athena-python.html

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()


class AthenaQuerier:
    def __init__(self, database_name: str, output_location: str):
        self.client = boto3.client('athena')  # Initialize Athena client
        self.database = database_name
        self.output_location = output_location
        self.query_execution_id = None

    def run_query(self, query: str):
        try:
            response = self.client.start_query_execution(
                QueryString=query,
                QueryExecutionContext={'Database': self.database},
                ResultConfiguration={'OutputLocation': self.output_location}
            )
            return response['QueryExecutionId']
        except ClientError as e:
            logger.error(f"Error running query: {e}")
            return None

    def check_query_status(self, retries=10, wait_interval=2):
        status = None
        for attempt in range(retries):
            try:
                response = self.client.get_query_execution(QueryExecutionId=self.query_execution_id)
                status = response['QueryExecution']['Status']['State']
                if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                    break  # if "return status" -> Exception won't be triggered
            except ClientError as e:
                logger.error(f"Error checking query status: {e}")
                if attempt < retries - 1:
                    time.sleep(wait_interval)  # wait before retrying the request
                    # if attempt = retries -> ClientError

            time.sleep(wait_interval)  # Wait for status

        if status == 'FAILED':
            raise Exception(f"Query failed: {response['QueryExecution']['Status']['StateChangeReason']}")
        elif status == 'CANCELLED':
            raise Exception("Query was cancelled.")
        return status

    def fetch_results(self):
        try:
            results = self.client.get_query_results(QueryExecutionId=self.query_execution_id)
            return results['ResultSet']
        except ClientError as e:
            logger.error(f"Error fetching query results: {e}")
            raise

    def execute_athena_query(self, query: str):
        self.query_execution_id = self.run_query(query)
        logger.info(f"Query execution id: {self.query_execution_id}")

        if self.query_execution_id:
            status = self.check_query_status()
            logger.info(f"Query execution status: {status}")
            if status == 'SUCCEEDED':
                logger.info(f"Query execution succeeded: {self.query_execution_id}")
                query_results = self.fetch_results()
                return query_results
            else:
                logger.error(f"Query did not succeed. Status: {status}")
        else:
            logger.error("Failed to start query execution.")
        return None

    def execute_multiple_athena_queries(self, query_list: list, wait_interval=2):
        results = []
        for query in query_list:
            logger.info(f"Executing query: {query}")
            result = self.execute_athena_query(query)
            if result:
                results.append(result)
            time.sleep(wait_interval)  # to avoid throttling
        return results

