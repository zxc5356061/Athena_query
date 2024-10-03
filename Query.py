import logging
import AthenaQuerier

if __name__ == '__main__':
    # Configure logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger()

    DATABASE_NAME = 'curated_dev_adform'
    OUTPUT_LOCATION = 's3://aws-athena-query-results-227956463654-eu-central-1/'
    QUERY_LIST = [
        '''
        SELECT * 
        FROM "curated_dev_adform"."dev_campaign_performance_daily_mediaplus_czech_republic" 
        ORDER BY date
        LIMIT 10;
        ''',
        '''
        SELECT COUNT(*) 
        FROM "curated_dev_adform"."dev_campaign_performance_daily_mediaplus_czech_republic";
        '''
    ]

    # Run the query
    querier = AthenaQuerier.AthenaQuerier(DATABASE_NAME, OUTPUT_LOCATION)
    results = querier.execute_multiple_athena_queries(QUERY_LIST)

    if results:
        for result in results:
            for row in result['Rows']:
                logger.info(row['Data'])
