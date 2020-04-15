import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """Use for loop to drop staging, fact, and dimension tables."""
    
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """Use for loop to create staging, fact, and dimension tables."""
    
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Create and connect to database on Redshift.
    Execute drop_tables and create_tables functions.
    """
    
    # read dwh.cfg file
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # connect to Redshift database
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    # drop staging, fact, and dimension tables if exists, then create them
    drop_tables(cur, conn)
    create_tables(cur, conn)

    # close connection to Redshift database
    conn.close()


if __name__ == "__main__":
    main()