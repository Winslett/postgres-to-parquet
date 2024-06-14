import psycopg2
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
import yaml
from urllib.parse import urlparse

from io import BytesIO

def load_config(config_file):
    with open(config_file, 'r') as file:
        config = yaml.safe_load(file)
    return config

def query_postgres(pg_params, sql_query):
    connection_params = {}

    if pg_params['uri'] is not None:
        parsed_url = urlparse(pg_params['uri'])

        connection_params['user'] = parsed_url.username
        connection_params['password'] = parsed_url.password
        connection_params['database'] = parsed_url.path[1:]
        connection_params['host'] = parsed_url.hostname
        connection_params['port'] = parsed_url.port if (parsed_url.port is not None) else 5432
    else:
        connection_params = pg_params


    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(**connection_params)
        # Execute the query and fetch the data into a pandas DataFrame
        df = pd.read_sql_query(sql_query, conn)
        # Close the connection
        conn.close()
        return df
    except Exception as e:
        print(f"Error querying PostgreSQL: {e}")
        return None

def save_to_parquet(df):
    try:
        # Convert the DataFrame to a Parquet file in memory
        table = pa.Table.from_pandas(df)
        output = BytesIO()
        pq.write_table(table, output)
        output.seek(0)
        return output
    except Exception as e:
        print(f"Error converting to Parquet: {e}")
        return None

def upload_to_s3(parquet_buffer, filename, s3_params): #s3_bucket, s3_path, aws_access_key_id, aws_secret_access_key, aws_region):
    try:
        # Initialize the S3 client
        s3_client = boto3.client('s3', region_name=s3_params['region'],
                                 aws_access_key_id=s3_params['access_key_id'],
                                 aws_secret_access_key=s3_params["secret_access_key"])
        # Upload the Parquet file to S3
        s3_file_key = s3_params['s3_path'] + filename
        s3_client.upload_fileobj(parquet_buffer, s3_params['s3_bucket'], s3_file_key)
        print(f"File uploaded to S3 bucket {s3_params['s3_bucket']} with key {s3_file_key}")
    except Exception as e:
        print(f"Error uploading to S3: {e}")

def main():
    # Load configuration from YAML file
    config = load_config('config.yml')

    pg_params = config['postgres']
    s3_params = config['aws']
    queries = config['query']

    # Query PostgreSQL
    print('Querying for distinct value')
    distinct_df = query_postgres(pg_params, queries['loop_query'])
    if distinct_df is not None:
        for index, loop_row in distinct_df.iterrows():
            # Query PostgreSQL
            print(f"..Querying for distinct value: {loop_row['distinct_value']}")
            sql_query = queries['export_query'].format(distinct_value = loop_row['distinct_value'])
            print(f"......Query: " + sql_query)
            query_df = query_postgres(pg_params, sql_query)
            if query_df is not None:
                # Convert to Parquet
                print(f"....Exporting to parquet")
                parquet_buffer = save_to_parquet(query_df)
                if parquet_buffer is not None:
                    print(f"....Uploading to S3")
                    # Upload to S3
                    upload_to_s3(parquet_buffer, loop_row['distinct_value'].strftime('%Y%m%d') + '.parquet', s3_params)

if __name__ == "__main__":
    main()

