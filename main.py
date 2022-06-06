import json
import os
import sys
import traceback

from utils import noblr_config
from utils import noblr_secrets
import psycopg2
import pandas as pd
import time
import argparse
import boto3


def main():

    content_object = s3_resource.Object(config.get_gw_bucket_name(), 'manifest.json')
    file_content = content_object.get()['Body'].read().decode('utf-8')
    manifest_json = json.loads(file_content)
    tables_keys = manifest_json.keys()
    for table_name in tables_keys:
        if table_name not in ["cc_contactorigvalue", "cc_outboundrecord", "cc_datachange", "heartbeat", "cc_usergroupstats"]:
            fingerprint_keys = manifest_json[table_name]['schemaHistory'].keys()
            for fingerprint in fingerprint_keys:
                prefix = f"{table_name}/{fingerprint}/"
                postgre_list = []
                try:
                    df = pd.read_sql(
                        f"select distinct gwcdac__timestampfolder from claims_raw_new.{table_name} where gwcdac__fingerprintfolder = '{fingerprint}';",con=conn)
                    postgre_list = df['gwcdac__timestampfolder'].tolist()
                except Exception:
                    print(f'table {table_name} not present in postgres..!')
                    pass
                s3_list = []
                folder_list = list_folders(config.get_gw_bucket_name(), prefix)
                for folder in folder_list:
                    s3_list.append(str(folder).split('/')[2])
                any_mismatch = list(set(s3_list).difference(postgre_list))
                if not any_mismatch:
                    pass
                    # print(f'All prefixes loaded for table name : {table_name} fingerprint : {fingerprint}')
                else:
                    print(f'for table name : {table_name} fingerprint : {fingerprint} these are the prefixes not loaded {any_mismatch}')

def list_folders(bucket_name, prefix):
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix, Delimiter='/')
    for content in response.get('CommonPrefixes', []):
        yield content.get('Prefix')


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    start = time.perf_counter()

    # initiate the parser
    parser = argparse.ArgumentParser()

    # add long and short argument
    parser.add_argument("--environment", "-e", help="select the environment to execute..!", choices=['dev', 'prod'],
                        default='dev')

    # read arguments from the command line
    args = parser.parse_args()

    # Instantiate conf class, close the conf file as soon as possible
    HOME = os.getcwd()
    configFileName = f'{HOME}/conf/config_{args.environment}.yml'
    configFile = open(configFileName, "r")
    config = noblr_config.NoblrConfig(configFile)
    configFile.close()

    # Create Noblr secrets connection
    secrets = noblr_secrets.NoblrSecrets(config.get_secret_name(), config.get_secret_region())
    secret = secrets.get_secret()

    # create s3 connection
    session = boto3.Session(
        aws_access_key_id=secret[config.get_gw_access_key()],
        aws_secret_access_key=secret[config.get_gw_secret_key()],
    )
    s3_resource = session.resource('s3')
    s3_client = session.client('s3')

    # Create postgres connection
    conn = None
    try:
        conn = psycopg2.connect(
            host=config.get_postgres_jdbcUrl(),
            database=config.get_postgres_jdbcDatabase(),
            user=secret[config.get_postgres_user()],
            password=secret[config.get_postgres_pwd()])
        main()
    except (Exception, psycopg2.DatabaseError) as error:
        error_msg = traceback.format_exc()
        print(error_msg)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')
        finish = time.perf_counter()
        print(f'Finished in {round(finish - start, 2)} second(s)')
