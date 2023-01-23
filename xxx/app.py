'''
1. Ensure DB is prepared
2. Checkin time to DB
3. Pull run-time from DB
4. Extract cwcoverage stats from PC on interval
5. Update DB
6. Update Cache
'''

from datetime import datetime, timedelta
from time import mktime
import time
import os
import io
from io import StringIO
import logging
import json
import requests
import pandas as pd
import psycopg2
from prismacloud.api import pc_api

logging.basicConfig(format='%(asctime)s %(message)s', level=logging.DEBUG)
ETL_NAME = 'defenders_coverage'
BACKEND_API = 'http://backend-api:5050'
RETENTION = 1
RUN_INTERVAL = 1
INTERVAL = 60
db_settings = {
    "host":     "postgres-edw",
    "database": "prisma",
    "user":     os.environ['POSTGRES_USER'],
    "password": os.environ['POSTGRES_PASSWORD'],
}


def db_write(conn, sql):
    """Uses received db connection and executes received sql"""
    logging.info('DB Write - %s', sql)
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
    except requests.exceptions.RequestException as error:
        conn.rollback()
        cursor.close()
        logging.error(error)
        return False
    conn.commit()
    cursor.close()
    return True


def db_connect(params_dict):
    """Creates and returns postgres connection"""
    logging.info('Creating DB Connection')
    l_conn = None
    try:
        l_conn = psycopg2.connect(**params_dict)
    except psycopg2.OperationalError as error:
        logging.error(error)
        return 1
    return l_conn


def init_coverage(conn):
    '''
    Initializing required tables for etl job
    '''
    logging.info('Initializing DB tables - coverage, etl_jobs')
    sql = '''
    CREATE TABLE IF NOT EXISTS reporting.coverage (
        provider varchar (16) NOT NULL,
        service varchar (24) NOT NULL,
        region varchar (24) NOT NULL,
        registry varchar (128) NOT NULL,
        credential varchar (64) NOT NULL,
        accountID varchar (64),
        name varchar (128),
        vminstance varchar (256),
        defended boolean NOT NULL,
        runtime varchar (16),
        version varchar (16),
        date_added DATE NOT NULL
    );
        CREATE TABLE IF NOT EXISTS reporting.etl_jobs (
        conn_name varchar (128) NOT NULL,
        conn_since TIMESTAMP NOT null,
        last_run TIMESTAMP NOT null,
        elapsed VARCHAR(8) NOT null,
        next_run TIMESTAMP NOT null,
        retention INT NOT null,
        int_time INT
    );
    GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA reporting TO prisma;
    '''
    if db_write(conn, sql):
        logging.info('...success')
    else:
        logging.info('...failed')


def add_etl_job():
    '''
    No ETL job for ETL_NAME found in DB.
    Add ETL job to DB.
    '''
    url = BACKEND_API + "/api/etljobs"
    curr_time = datetime.now()
    logging.info('Adding new etl job \'%s\' to DB', ETL_NAME)
    data = json.dumps({
        'conn_name': ETL_NAME, 'conn_since': curr_time,
        'next_run': curr_time, 'last_run': curr_time,
        'elapsed': '00:00:00', 'retention': RETENTION,
        'int_time': 1
    }, indent=4, default=str)
    try:
        response = requests.post(url, json=data, timeout=10)
    except requests.exceptions.RequestException as error:
        logging.error(error)
    if response.status_code != 201:
        logging.error('Error registering etl job with DB')
        return False
    logging.info('Successfully registered etl job with DB')
    return True


def time_to_run():
    '''
    Determine if it is time to run.
    Get ETL job info from API backend (create if not exists).
    Return true if current time gt next run time.
    '''
    global RUN_INTERVAL
    global RETENTION
    url = BACKEND_API + "/api/etljobs?etl_name=" + ETL_NAME
    logging.info('Pulling etl job config from api endpoint - %s', url)
    try:
        response = requests.get(url, timeout=10)
    except requests.exceptions.RequestException as error:
        logging.error(error)
    if response.status_code == 204:
        logging.info('API returned status of %s', response.status_code)
        logging.info('No etl job results found')
        if not add_etl_job():
            return False
        try:
            response = requests.get(url, timeout=10)
        except requests.exceptions.RequestException as error:
            logging.error(error)
        if response.status_code != 201:
            return False
    logging.info('ETL job instructions located')
    next_run = response.json()[0]['next_run']
    RETENTION = response.json()[0]['retention']
    RUN_INTERVAL = response.json()[0]['int_time']
    next_run = time.strptime(next_run, "%a, %d %b %Y %H:%M:%S %Z")
    next_run = datetime.fromtimestamp(mktime(next_run))
    if datetime.now() > next_run:
        logging.info('Initating data refresh')
        return True
    logging.info(
        'Not time to refresh data yet.  Sleeping for %s seconds', INTERVAL)
    return False


def get_pc_creds():
    '''
    Get PC credentials from backend api andpoint
    '''
    logging.info('Getting PC credentials from backend api')
    try:
        response = requests.get(
            'http://backend-api:5050/api/prismasettings', timeout=10)
        if response.status_code == 201:
            msg = ''
            if response.text != '':
                logging.info('Credentials successfully obtained')
                settings = response.json()
                api_url = settings["apiurl"]
                api_key = settings["apikey"]
                api_secret = settings["apisecret"]
            else:
                logging.info('DB Contained no Saved Settings')
        elif response.status_code == 204:
            msg = 'No Prisma Cloud credentials returned'
            logging.info(msg)
            return '', '', '', msg
        else:
            logging.error('Unknown response from backend api')
    except requests.exceptions.RequestException as error:
        logging.error(error)
        return '', '', '', 'Could not connect with Backend'
    return api_url, api_key, api_secret, msg


def validate_pc_creds(api_url, api_key, api_secret):
    '''
    Returns True if validation is successful
    '''
    logging.info('Validating PC credentials with PC through backend api')
    pc_url = (api_url + '/login')
    payload = {
        "username": api_key,
        "password": api_secret,
    }
    headers = {"content-type": "application/json; charset=UTF-8"}
    try:
        response = requests.post(pc_url, json=payload,
                                 headers=headers, timeout=10)
        if response.status_code == 200:
            logging.info(
                'Successfully validated Prisma Cloud credentials')
            return True
        logging.info('Unable to validate Prisma Cloud credentials')
        return False
    except requests.exceptions.RequestException as error:
        logging.error(error)
        return False


def get_coverage_df():
    logging.info('Retrieving coverage as a CSV')
    date_added = [datetime.now().strftime("%Y-%m-%d")]
    buffer = io.StringIO(pc_api.cloud_discovery_download())
    coverage_df = pd.read_csv(filepath_or_buffer=buffer)
    coverage_df.drop('Project', axis=1, inplace=True)
    coverage_df.drop('Image ID', axis=1, inplace=True)
    coverage_df.drop('FQDN', axis=1, inplace=True)
    coverage_df.drop('Resource Group', axis=1, inplace=True)
    coverage_df.drop('Running Tasks', axis=1, inplace=True)
    coverage_df.drop('Active Services', axis=1, inplace=True)
    coverage_df.drop('ARN', axis=1, inplace=True)
    coverage_df.drop('Last Modified', axis=1, inplace=True)
    coverage_df.drop('Created At', axis=1, inplace=True)
    coverage_df.drop('Additional Data', axis=1, inplace=True)
    coverage_df.drop('Status', axis=1, inplace=True)
    coverage_df.drop('Nodes', axis=1, inplace=True)
    coverage_df['date_added'] = date_added[0]
    return coverage_df


def purge_data():
    '''
    Purge data if older than number of days set in global RETENTION
    '''
    logging.info(
        'Purging database records older than %s days', RETENTION)
    sql = ("DELETE FROM reporting.coverage * WHERE date_added::date <= date \'" +
           ((datetime.now() - timedelta(days=RETENTION)).strftime('%Y-%m-%d')) + "\';")
    conn = db_connect(db_settings)
    db_write(conn, sql)
    logging.info('Closing DB Connection')
    conn.close()


def df_to_db(df_to_write):
    """
    Writes dataframe to database table
    """
    conn = db_connect(db_settings)
    table = 'coverage'
    logging.info('DF dump to table - %s', table)
    buffer = StringIO()
    df_to_write.to_csv(buffer, header=False, index=False)
    buffer.seek(0)
    cursor = conn.cursor()
    cursor.execute("SET search_path TO reporting")
    try:
        cursor.copy_from(buffer, table, sep=",")
        conn.commit()
    except requests.exceptions.RequestException as error:
        conn.rollback()
        cursor.close()
        logging.error(error)
    cursor.close()
    logging.info('Closing DB Connection')
    conn.close()
    return True


def update_etl(elapsed):
    logging.info('Updating ETL Job data')
    next_run = datetime.now() + timedelta(RUN_INTERVAL)
    conn = db_connect(db_settings)
    sql = (
        "UPDATE reporting.etl_jobs SET next_run = \'" +
        str(next_run) + "\', elapsed = \'" + str(elapsed) +
        "\' WHERE conn_name = \'" + ETL_NAME + "\'"
    )
    db_write(conn, sql)
    logging.info('Closing DB Connection')
    conn.close()


def main():
    '''
    Start loop with 1 minute check-in interval.
    Every checkin, look in database for next run time.
    If next run time has passed, execute next run.
    '''
    msg = ''
    conn = db_connect(db_settings)
    while conn == 1:
        time.sleep(5)
        conn = db_connect(db_settings)
    init_coverage(conn)
    logging.info('Closing DB Connection')
    conn.close()
    while True:
        if time_to_run():
            start_time = time.time()
            (api_url, api_key, api_secret, msg) = get_pc_creds()
            if msg == '':
                if validate_pc_creds(api_url, api_key, api_secret):
                    validated = True
                    pc_settings = {
                        "url":      api_url,
                        "identity": api_key,
                        "secret":   api_secret,
                    }
                    logging.info(
                        'Configuring prismacloud.api library settings')
                    pc_api.configure(pc_settings)
                else:
                    validated = False
            else:
                validated = False
            if validated:
                purge_data()
                df_to_db(get_coverage_df())
                elapsed = time.strftime(
                    "%H:%M:%S", time.gmtime(time.time() - start_time))
                update_etl(elapsed)
        logging.info('Sleeping for %s', INTERVAL)
        time.sleep(INTERVAL)


if __name__ == "__main__":
    main()
