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
import os
import io
from io import StringIO
import sys
import time
import logging
import json
import pandas as pd
import psycopg2
import requests
from direct_redis import DirectRedis
from prismacloud.api import pc_api

logging.basicConfig(
    format='%(levelname)s %(asctime)s %(message)s', level=logging.DEBUG)

ETL_NAME = 'defenders_coverage'
BACKEND_API = 'http://backend-api:5050'
REDIS_CACHE = 'redis-cache'
RETENTION = 35
RUN_INTERVAL = 7
INTERVAL = 60
DB_SETTINGS = {
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


def init_db():
    '''
    Connects to db with retry
    Inits DB tables, if not created
    '''
    conn = db_connect(DB_SETTINGS)
    while conn == 1:
        time.sleep(5)
        conn = db_connect(DB_SETTINGS)
    init_coverage(conn)
    logging.info('Closing DB Connection')
    conn.close()
    return True


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
        'int_time': RUN_INTERVAL
    }, indent=4, default=str)
    try:
        response = requests.post(url, json=data, timeout=10)
    except requests.exceptions.RequestException as error:
        logging.error(error)
    if response.status_code != 201:
        logging.error('Could not configure ETL attributes')
        sys.exit(500)
    logging.info('Successfully registered etl job with DB')
    return (curr_time, RUN_INTERVAL, RETENTION)


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


def get_etl_attributes():
    '''
    Attempts to get ETL attributes.  Creates if not exists.
    Returns next_run, run_interval, and retention.
    '''
    url = BACKEND_API + "/api/etljobs?etl_name=" + ETL_NAME
    logging.info('Pulling etl job config from api endpoint - %s', url)
    try:
        response = requests.get(url, timeout=10)
    except requests.exceptions.RequestException as error:
        logging.error(error)
    if response.status_code == 204:
        logging.info('API returned status of %s', response.status_code)
        logging.info('No etl job results found')
        (next_run, run_interval, retention) = add_etl_job()
    elif response.status_code == 201:
        logging.info('ETL job instructions located')
        next_run = response.json()[0]['next_run']
        next_run = time.strptime(next_run, "%a, %d %b %Y %H:%M:%S %Z")
        next_run = datetime.fromtimestamp(mktime(next_run))
        retention = response.json()[0]['retention']
        run_interval = response.json()[0]['int_time']
    else:
        logging.error('Could not configure ETL attributes')
        sys.exit(500)
    return next_run, run_interval, retention


def time_to_run(next_run):
    '''
    Receives the next rune time and responds True
    if current time is past next rune time.
    '''
    if datetime.now() > next_run:
        logging.info('Initating data refresh')
        return True
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
            if response.text != '':
                logging.info('Credentials successfully obtained')
                settings = response.json()
                api_url = settings["apiurl"]
                api_key = settings["apikey"]
                api_secret = settings["apisecret"]
            else:
                logging.info('DB Contained no Saved Settings')
        elif response.status_code == 204:
            logging.info('No Prisma Cloud credentials returned')
            return '', '', '', False
        else:
            logging.error('Unknown response from backend api')
    except requests.exceptions.RequestException as error:
        logging.error(error)
        return '', '', '', False
    valid = validate_pc_creds(api_url, api_key, api_secret)
    return api_url, api_key, api_secret, valid


def update_etl(start_time, elapsed, next_run):
    '''
    Update the existing ETL job row with previous elapsed time
    and the next run time.
    '''
    logging.info('Updating ETL Job data')
    conn = db_connect(DB_SETTINGS)
    sql = (
        "UPDATE reporting.etl_jobs SET next_run = \'" +
        str(next_run) + "\', elapsed = \'" + str(elapsed) +
        "\', last_run = \'" + str(start_time) +
        "\' WHERE conn_name = \'" + ETL_NAME + "\'"
    )
    db_write(conn, sql)
    logging.info('Closing DB Connection')
    conn.close()


def purge_data(retention):
    '''
    Remove records older than "retention" days from DB
    '''
    logging.info(
        'Purging database records older than %s days', retention)
    conn = db_connect(DB_SETTINGS)
    sql = ("DELETE FROM reporting.coverage * WHERE date_added::date <= date \'" +
           ((datetime.now() - timedelta(days=retention)).strftime('%Y-%m-%d')) + "\';")
    db_write(conn, sql)
    logging.info('Closing DB Connection')
    conn.close()


def get_coverage_df():
    '''
    Pull down coverage CSV from endpoint, drop a few
    columns, then return as dataframe.
    '''
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


def write_to_redis(rd_var, working_df):
    '''
    Receives the name of variable to store in redis cache
    plus the actual dataframe and writes to cache.
    '''
    logging.info('Creating connection to redis cache')
    redis_conn = DirectRedis(host=REDIS_CACHE, port=6379)
    logging.info('Pushing rollup dataframe into cache')
    while True:
        try:
            redis_conn.set(rd_var,
                           working_df)
            break
        except Exception as ex:
            logging.error(
                ex.args[0])
            time.sleep(5)
    logging.info(
        'Successfully stored dataframe in redis cache')
    return True


def df_to_db(df_to_write):
    """
    Writes dataframe to database table
    """
    conn = db_connect(DB_SETTINGS)
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


def main():
    '''
    Start loop with 1 minute check-in interval.
    Every checkin, look in database for next run time.
    If next run time has passed, execute next run.
    '''

    # Initialize DB Tables, if required
    init_db()

    # Gather ETL attributes, or create if non existant
    (next_run, run_interval, retention) = get_etl_attributes()
    logging.info('Next Run will happen after %s', next_run)
    logging.info('Runs will occur once ever %s day(s)', run_interval)
    logging.info(
        '%s days records will be maintained in the database', retention)

    # Begin loop which will be enetered when it is is time_to_run
    while True:
        if time_to_run(next_run):
            start_time = time.time()
            dt_start_time = datetime.fromtimestamp(start_time)
            (api_url, api_key, api_secret, valid) = get_pc_creds()
            if valid:
                pc_settings = {
                    "url":      api_url,
                    "identity": api_key,
                    "secret":   api_secret,
                }
                logging.info(
                    'Configuring prismacloud.api library settings')
                pc_api.configure(pc_settings)

                # Purge records older than "retention" days from db
                purge_data(retention)

                # Get coverage information, store as dataframe
                # then write to DB
                curr_coverage_df = get_coverage_df()
                df_to_db(curr_coverage_df)

                # Gather relevant data and store in redis as dataframe
                write_to_redis('curr_coverage', curr_coverage_df)

                # Store time of current run in elapsed for ETL job
                elapsed = time.strftime(
                    "%H:%M:%S", time.gmtime(time.time() - start_time))
                logging.info('Total time to run was %s', elapsed)

                # Update next run with start_time plus run_interval
                next_run = dt_start_time + timedelta(run_interval)

                # Update ETL job with new elapsed and next_run values
                update_etl(dt_start_time, elapsed, next_run)

        logging.info('Sleeping for %s', INTERVAL)
        time.sleep(INTERVAL)


if __name__ == "__main__":
    main()
