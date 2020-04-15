import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """Use for loop to load data from S3 to staging tables on Redshift."""
    
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Use for loop to load data from staging tables to fact and dimension tables on Redshift."""
    
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Create and connect to database on Redshift.
    Execute load_staging_tables and insert_tables functions.
    """
    
    # read dwh.cfg file
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # connect to Redshift database
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    # load data from S3 to staging tables, then load data from staging tables to fact and dimension tables
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    # close connection to Redshift database
    conn.close()


if __name__ == "__main__":
    main()