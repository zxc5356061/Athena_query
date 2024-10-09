import logging
from athena_querier import AthenaQuerier
import pandas as pd


def response_to_parquet(response: dict, parquet_file_path: str):
    column_headers = [col['VarCharValue'] for col in response['Rows'][0]['Data']]
    data_rows = []
    for row in response['Rows'][1:]:  # skip header
        data_rows.append([col['VarCharValue'] for col in row['Data']])

    df = pd.DataFrame(data_rows, columns=column_headers)
    df.to_parquet(parquet_file_path)


def main():
    # Configure logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger()

    file_path = '/Users/huangp/Documents/Data_team/DT_project/Athena_query/campaign_performance_query.sql'
    database_name = 'curated_dev_adform'
    output_location = 's3://aws-athena-query-results-227956463654-eu-central-1/'
    parquet_file_path = '/Users/huangp/Documents/Data_team/DT_project/Athena_query/campaign_performance.parquet'

    with open(file_path, 'r') as QUERY:
        querier = AthenaQuerier(database_name, output_location)
        query_response = querier.execute_athena_query(QUERY.read())

        # # Debugging
        # if query_response:
        #     for row in query_response['Rows']:
        #         logger.info(row['Data'])

        response_to_parquet(query_response, parquet_file_path)


if __name__ == '__main__':
    main()
