import logging
from athena_querier import AthenaQuerier

def to_run():
    # Configure logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger()

    file_path = '/Users/huangp/Documents/Data_team/DT_project/Athena_query/campaign_performance_query.sql'
    database_name = 'curated_dev_adform'
    output_location = 's3://aws-athena-query-results-227956463654-eu-central-1/'

    with open(file_path, 'r') as QUERY:
        # Run the query
        querier = AthenaQuerier(database_name, output_location)
        # results = querier.execute_multiple_athena_queries(QUERY_LIST)
        results = querier.execute_athena_query(QUERY.read())

        # Debugging
        if results:
            for row in results['Rows']:
                logger.info(row['Data'])


if __name__ == '__main__':
    to_run()
