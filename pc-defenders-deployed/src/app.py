'''
1. Ensure DB is prepared
2. Checkin time to DB
3. Pull run-time from DB
4. Extract stats from PC on interval
5. Update DB
6. Update Cache
'''
from datetime import datetime, timedelta
from time import mktime
import time
import json
from io import StringIO
import os
import logging
import requests
import pandas as pd
import psycopg2
from direct_redis import DirectRedis
from prismacloud.api import pc_api

logging.basicConfig(format='%(asctime)s %(message)s', level=logging.DEBUG)
ETL_NAME = 'defenders_deployed'
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


def df_to_db(conn, df_to_write, table):
    """Writes dataframe to database table"""
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
        return False
    cursor.close()
    return True


def db_read(conn, sql):
    """Uses received db connection and executes received sql"""
    logging.info('DB Read - %s', sql)
    q_list = []
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
    except requests.exceptions.RequestException as error:
        cursor.close()
        logging.error(error)
        return False
    q_list = cursor.fetchall()
    cursor.close()
    return q_list


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


def init_defenders_table(conn):
    '''
    Initializing required tables for etl job
    '''
    logging.info('Initializing DB tables - defenders, etl_jobs')
    sql = '''
    CREATE TABLE IF NOT EXISTS reporting.defenders (
        hostname varchar (128) NOT NULL,
        version varchar (9) NOT NULL,
        type varchar (24) NOT NULL,
        category varchar (24) NOT NULL,
        connected varchar (24) NOT NULL,
        accountID varchar (64) NOT NULL,
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


def get_run_stats():
    '''
    Pull DB run-time stats
    '''
    url = "http://backend-api:5050/api/etljobs?etl_name=" + ETL_NAME
    logging.info('Pulling etl job config from api endpoint - %s', url)
    try:
        response = requests.get(url, timeout=10)
    except requests.exceptions.RequestException as error:
        logging.error(error)
    if response.status_code != 201:
        logging.info('API returned status of %s', response.status_code)
        logging.info('No etl job results found')
        return False
    logging.info('ETL job instructions located')
    return (response.json())[0]


def add_etl_job(conn_since, next_run, last_run, elapsed, retention, int_time):
    '''
    Add job to the database
    '''
    url = "http://backend-api:5050/api/etljobs"
    logging.info('Adding new etl job \'%s\' to DB', ETL_NAME)
    data = json.dumps({'conn_name': ETL_NAME, 'conn_since': conn_since,
                       'next_run': next_run, 'last_run': last_run,
                       'elapsed': elapsed, 'retention': retention,
                       'int_time': int_time}, indent=4, default=str)
    try:
        response = requests.post(url, json=data, timeout=10)
    except requests.exceptions.RequestException as error:
        logging.error(error)
    if response.status_code != 201:
        logging.error('Error registering etl job with DB')
        return False
    logging.info('Successfully registered etl job with DB')
    return True


def get_pc_creds():
    '''
    Get PC credentials from backend api andpoint
    '''
    logging.info('Getting PC creentials from backend api')
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
    logging.info('Validating PC creentials with PC through backend api')
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


def main():
    '''
    Start loop with 1 minute check-in interval.
    Every checkin, look in database for next run time.
    If next run time has passed, execute next run.

    Need to break this apart in the future to make more
    supportable.
    '''
    msg = ''
    interval = 60
    while True:
        conn = db_connect(db_settings)
        while conn == 1:
            time.sleep(5)
            conn = db_connect(db_settings)
        init_defenders_table(conn)
        etl_db_obj = get_run_stats()
        if etl_db_obj:
            next_run = etl_db_obj['next_run']
            next_run = time.strptime(next_run, "%a, %d %b %Y %H:%M:%S %Z")
            next_run = datetime.fromtimestamp(mktime(next_run))
            retention = etl_db_obj['retention']
            int_time = etl_db_obj['int_time']
        else:
            conn_since = datetime.now()
            next_run = conn_since
            last_run = conn_since
            retention = 30
            int_time = 1
            add_etl_job(conn_since, next_run, last_run,
                        '00:00:00', retention, int_time)
        if datetime.now() > next_run:
            logging.info('Refresh etl data initiated')
            start_time = time.time()
            date_added = [datetime.now().strftime("%Y-%m-%d")]
            (api_url, api_key, api_secret, msg) = get_pc_creds()
            if msg == '':
                pc_settings = {
                    "url":      api_url,
                    "identity": api_key,
                    "secret":   api_secret,
                }
                if validate_pc_creds(api_url, api_key, api_secret):
                    validated = True
                else:
                    validated = False
                if validated:
                    logging.info(
                        'Configuring prismacloud.api library settings')
                    pc_api.configure(pc_settings)

                    # Build dataframe from defenders api endpoint
                    logging.info(
                        'Pulling list of defenders from Prisma Cloud API')
                    defenders_api_lod = pc_api.defenders_list_read(
                        'connected=true')
                    logging.info(
                        'Building datafrom from defender list-of-dictionaries')
                    df_defenders = pd.DataFrame(
                        columns=['hostname', 'version', 'type', 'category', 'connected', 'accountID', 'date_added'])
                    for defender in defenders_api_lod:
                        if 'accountID' not in defender['cloudMetadata']:
                            defender['cloudMetadata']['accountID'] = 'aws'
                        df_defenders.loc[len(df_defenders.index)] = [
                            defender['hostname'], defender['version'], defender['type'], defender['category'],
                            defender['connected'], defender['cloudMetadata']['accountID'], date_added
                        ]

                    # Purge old records from DB
                    logging.info(
                        'Purging database records older than %s days', retention)
                    sql = ("DELETE FROM reporting.defenders * WHERE date_added::date < date \'" +
                           ((datetime.now() - timedelta(days=retention)).strftime('%Y-%m-%d')) + "\';")
                    db_write(conn, sql)

                    # Write df to defenders table
                    logging.info('Writing defender dataframe to table')
                    df_to_db(conn, df_defenders, "defenders")

                    # Pull all historical defender stats, store as dataframe
                    logging.info(
                        'Pulling rollup data from DB for push into cache')
                    sql = (
                        "SELECT category, date_added, version, connected, accountID FROM reporting.defenders")
                    data_list = db_read(conn, sql)
                    logging.info('Converting rollup data list into dataframe')
                    df_all_defenders = pd.DataFrame(
                        data_list, columns=['category', 'date_added', 'version', 'connected', 'accountID'])
                    df_defenders = pd.DataFrame(
                        data_list, columns=['category', 'date_added', 'version', 'connected', 'accountID']).groupby(
                            ['date_added', 'category', 'version', 'connected', 'accountID'])['category'].count().reset_index(name='total')

                    # Push defender dataframe to redis
                    logging.info('Creating connection to redis cache')
                    redis_conn = DirectRedis(host='redis-cache', port=6379)
                    logging.info('Pushing rollup dataframe into cache')
                    while True:
                        try:
                            redis_conn.set('df_all_defenders',
                                           df_all_defenders)
                            redis_conn.set('df_defenders', df_defenders)
                            break
                        except Exception as ex:
                            logging.error(
                                ex.args[0])
                            time.sleep(5)
                    logging.info(
                        'Successfully stored dataframe in redis cache')
                    # Update etl_jobs with new next_run
                    next_run = datetime.now() + timedelta(int_time)
                    elapsed = time.strftime(
                        "%H:%M:%S", time.gmtime(time.time() - start_time))
                    sql = (
                        "UPDATE reporting.etl_jobs SET next_run = \'" +
                        str(next_run) + "\', elapsed = \'" + str(elapsed) +
                        "\' WHERE conn_name = \'" + ETL_NAME + "\'"
                    )
                    logging.info(
                        'Updating etl job statistics with new next_run and elapsed')
                    db_write(conn, sql)
            else:
                logging.info(
                    'Sleeping until credentials are available and valid')
        else:
            logging.info(
                'Not time to refresh data yet.  Sleeping for %s seconds', interval)
        time.sleep(interval)


if __name__ == "__main__":
    main()
