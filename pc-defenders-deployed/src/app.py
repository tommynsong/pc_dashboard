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
import sys
import os
import requests
import pandas as pd
import psycopg2
from direct_redis import DirectRedis
from prismacloud.api import pc_api

ETL_NAME = 'defenders_deployed'
db_settings = {
    "host":     "postgres-edw",
    "database": "prisma",
    "user":     os.environ['POSTGRES_USER'],
    "password": os.environ['POSTGRES_PASSWORD'],
}


def db_write(conn, sql):
    """Uses received db connection and executes received sql"""
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
    except (Exception, psycopg2.DatabaseError) as msg:
        conn.rollback()
        cursor.close()
        print("Error in db_write function:")
        sys.exit(msg)
    conn.commit()
    cursor.close()
    return True


def df_to_db(conn, df_to_write, table):
    """Writes dataframe to database table"""
    buffer = StringIO()
    df_to_write.to_csv(buffer, header=False, index=False)
    buffer.seek(0)
    cursor = conn.cursor()
    cursor.execute("SET search_path TO reporting")
    try:
        cursor.copy_from(buffer, table, sep=",")
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as msg:
        conn.rollback()
        cursor.close()
        print("Error in df_to_db function:")
        sys.exit(msg)
    cursor.close()
    return True


def db_read(conn, sql):
    """Uses received db connection and executes received sql"""
    q_list = []
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
    except (Exception, psycopg2.DatabaseError) as msg:
        print("Error in db_read function:")
        sys.exit(msg)
    q_list = cursor.fetchall()
    cursor.close()
    return q_list


def db_connect(params_dict):
    """Creates and returns postgres connection"""
    conn = None
    try:
        conn = psycopg2.connect(**params_dict)
    except (Exception, psycopg2.DatabaseError) as msg:
        print("Error in db_connect function:")
        sys.exit(msg)
    return conn


def init_defenders_table(conn):
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
    db_write(conn, sql)


def get_run_stats():
    '''
    Pull DB run-time stats
    '''
    url = "http://backend-api:5050/api/etljobs?etl_name=" + ETL_NAME
    try:
        response = requests.get(url, timeout=10)
    except requests.exceptions.RequestException as msg:
        print(msg)
    if response.text == '':
        return ''
    else:
        return (response.json())[0]


def add_etl_job(conn_since, next_run, last_run, elapsed, retention, int_time):
    '''
    Add job to the database
    '''
    url = "http://backend-api:5050/api/etljobs"
    data = json.dumps({'conn_name': ETL_NAME, 'conn_since': conn_since,
                       'next_run': next_run, 'last_run': last_run,
                       'elapsed': elapsed, 'retention': retention,
                       'int_time': int_time}, indent=4, default=str)
    response = requests.post(url, json=data, timeout=10)
    if response.status_code != 201:
        return 'Error registering etl job with DB'


def get_pc_creds():
    try:
        response = requests.get(
            'http://backend-api:5050/api/prismasettings')
        if response.status_code == 201:
            msg = ''
            if response.text != '':
                settings = response.json()
                api_url = settings["apiurl"]
                api_key = settings["apikey"]
                api_secret = settings["apisecret"]
            else:
                msg = "DB Contained no Saved Settings"
        else:
            msg = "Could not load settings"
    except:
        return '', '', '', 'Could not connect with Backend'
    return api_url, api_key, api_secret, msg


def main():
    '''
    Start loop with 1 minute check-in interval.
    Every checkin, look in database for next run time.
    If next run time has passed, execute next run.
    '''
    msg = ''
    interval = 60
    while True:
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
        if msg:
            print(msg)
        elif datetime.now() > next_run:
            start_time = time.time()
            date_added = [datetime.now().strftime("%Y-%m-%d")]
            (api_url, api_key, api_secret, msg) = get_pc_creds()
            if msg == '':
                pc_settings = {
                    "url":      api_url,
                    "identity": api_key,
                    "secret":   api_secret,
                }
                pc_api.configure(pc_settings)

                # Build dataframe from defenders api endpoint
                defenders_api_lod = pc_api.defenders_list_read(
                    'connected=true')
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
                sql = ("DELETE FROM reporting.defenders * WHERE date_added::date < date \'" +
                       ((datetime.now() - timedelta(days=retention)).strftime('%Y-%m-%d')) + "\';")
                db_write(conn, sql)

                # Write df to defenders table
                df_to_db(conn, df_defenders, "defenders")

                # Pull all historical defender stats, store as dataframe
                sql = (
                    "SELECT category, date_added, version, connected, accountID FROM reporting.defenders")
                data_list = db_read(conn, sql)
                df_all_defenders = pd.DataFrame(
                    data_list, columns=['category', 'date_added', 'version', 'connected', 'accountID'])
                df_defenders = pd.DataFrame(
                    data_list, columns=['category', 'date_added', 'version', 'connected', 'accountID']).groupby(
                        ['date_added', 'category', 'version', 'connected', 'accountID'])['category'].count().reset_index(name='total')

                # Push defender dataframe to redis
                redis_conn = DirectRedis(host='redis-cache', port=6379)
                redis_conn.set('df_all_defenders', df_all_defenders)
                redis_conn.set('df_defenders', df_defenders)

                # Update etl_jobs with new next_run
                next_run = datetime.now() + timedelta(int_time)
                elapsed = time.strftime(
                    "%H:%M:%S", time.gmtime(time.time() - start_time))
                sql = (
                    "UPDATE reporting.etl_jobs SET next_run = \'" +
                    str(next_run) + "\' WHERE conn_name = \'" + ETL_NAME + "\'"
                )
                db_write(conn, sql)
            else:
                print(msg)
        time.sleep(interval)


if __name__ == "__main__":
    main()
