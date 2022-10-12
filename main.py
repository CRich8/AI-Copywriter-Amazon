import pandas as pd
from google.cloud import bigquery
import pandas_gbq
import  os
from google.oauth2 import service_account
import master_asins_table_script2
import runQuery

gbq_project_id = 'tough-cascade-359115'
gbq_extracted_reviews_data = 'extracted_reviews_data'
gbq_raw_reviews_table = 'raw_reviews_table'
gbq_transformed_reviews_data = 'transformed_reviews_data'
gbq_master_asins_table = 'master_asins_table'
gbq_master_reviews_table = 'master_reviews_table'
gbq_raw_reviews_table_location = '{}.{}'.format(gbq_extracted_reviews_data, gbq_raw_reviews_table)
gbq_master_asins_table_location = '{}.{}'.format(gbq_transformed_reviews_data, gbq_master_asins_table)
gbq_master_reviews_table_location = '{}.{}'.format(gbq_transformed_reviews_data, gbq_master_reviews_table)

credentials = service_account.Credentials.from_service_account_file('C:/Users/Connor/Documents/ENG/cl_gcs_credentials.json')

# Check master reviews for client vertical - Input client search term(s) after asin_search_term =
def main():
    print('Querying BQ...')
    df1 = pd.read_gbq('SELECT asin_search_term FROM `tough-cascade-359115.transformed_reviews_data.master_reviews_table` WHERE asin_search_term = "hair straightener" LIMIT 500', gbq_project_id)
    if df1.empty:
        prompt_list = input('What are the search terms? ')
        with open('prompt_list.txt', 'w') as output:
            output.write(str(prompt_list))
        with open('prompt_list.txt', 'r') as output:
            while True:
                if output.read() == prompt_list:
                    break
        master_asins_table_script2.main()
    else:
        print(df1) 
main()
