"""
1. Extract defender statistics from PC console
2. Update postgres with historical defender stats
3. Pull historical stats from db and transfer in dataframe
4. Load dataframe into redis for front-end reporting
"""

import os
import sys
from io import StringIO
from datetime import timedelta
from datetime import datetime
import pandas as pd
import psycopg2
from direct_redis import DirectRedis
from prismacloud.api import pc_api


pc_settings = {
    "url":      os.environ['PC_URL'],
    "identity": os.environ['PC_IDENTITY'],
    "secret":   os.environ['PC_SECRET'],
}

db_settings = {
    "host":     os.environ['DB_SERVER'],
    "database": "prisma",
    "user":     os.environ['DB_USER'],
    "password": os.environ['DB_PASSWORD'],
}

pc_api.configure(pc_settings)
RETENTION = 30

#
# Helpers
#


def db_connect(params_dict):
    """Creates and returns postgres connection"""
    conn = None
    try:
        conn = psycopg2.connect(**params_dict)
    except (Exception, psycopg2.DatabaseError) as msg:
        print("Error in db_connect function:")
        sys.exit(msg)
    return conn


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


#
# Main
#


def main():
    """Main Function"""
    # Initate DB connection
    conn = db_connect(db_settings)

    # Create Defender DB table, if it doesn't exist
    sql = '''
    CREATE TABLE IF NOT EXISTS reporting.defenders (
        hostname varchar (128) NOT NULL,
        version varchar (9) NOT NULL,
        type varchar (24) NOT NULL,
        category varchar (24) NOT NULL,
        connected varchar (24) NOT NULL,
        accountID varchar (32) NOT NULL,
        date_added DATE NOT NULL
    );
    GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA reporting TO prisma;
    '''
    db_write(conn, sql)

    # Obtain current time and add to column in dataframe
    current_time = datetime.now()
    date_added = [current_time.strftime("%Y-%m-%d")]
    date_added = "2022-12-25"

    # Build dataframe from defenders api endpoint
    defenders_api_lod = pc_api.defenders_list_read('connected=true')
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
           ((current_time - timedelta(days=RETENTION)).strftime('%Y-%m-%d')) + "\';")
    db_write(conn, sql)

    # Write df to defenders table
    df_to_db(conn, df_defenders, "defenders")

    # Pull all historical defender stats, store as dataframe
    sql = ("SELECT category, date_added, version, connected, accountID FROM reporting.defenders")
    data_list = db_read(conn, sql)
    df_defenders = pd.DataFrame(
        data_list, columns=['category', 'date_added', 'version', 'connected', 'accountID']).groupby(
            ['date_added', 'category', 'version', 'connected', 'accountID'])['category'].count().reset_index(name='total')

    # Push defender dataframe to redis
    redis_conn = DirectRedis(host='localhost', port=6379)
    redis_conn.set('df_defenders', df_defenders)
    conn.close()


if __name__ == "__main__":
    main()
