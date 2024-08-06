import configparser
import psycopg2
import os
from sql_queries import create_staging_table_queries, drop_table_queries, copy_table_queries, create_and_insert_normalized_table_queries


def drop_tables(cur, con):
    """
    Drop the tables if existed for initialization.
    INPUT:
        cur: cursor object
        con: connection object
    OUTPUT:
        None
    """
    for query in drop_table_queries:
        cur.execute(query)
        con.commit()


def create_staging_tables(cur, con):
    """
    Create staging tables based on the sql queries which contain the schema.
    INPUT:
        cur: cursor object
        con: connection object
    OUTPUT:
        None
    """
    for query in create_staging_table_queries:
        cur.execute(query)
        con.commit()
        
def load_staging_tables(cur, con):
    """
    Load staging tables based on the sql queries which contain the schema.
    INPUT:
        cur: cursor object
        con: connection object
    OUTPUT:
        None    
    """
    for query in copy_table_queries:
        cur.execute(query)
        con.commit()

def create_insert_normalized_tables(cur, con):
    """
    Create and insert normalized tables from the staging tables based on the relevant sql queries.
    INPUT:
        cur: cursor object
        con: connection object
    OUTPUT:
        None
    """
    for query in create_and_insert_normalized_table_queries:
        cur.execute(query)
        con.commit()

def main():
    """ Assign the parameters and load the functions """
    # CONFIG
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    KEY =config.get('AWS', 'KEY')
    SECRET =config.get('AWS', 'SECRET')

    DWH_ENDPOINT = config.get("CLUSTER", "HOST")
    DWH_DB = config.get("CLUSTER", "DB_NAME") 
    DWH_DB_USER = config.get("CLUSTER", "DB_USER") 
    DWH_DB_PASSWORD = config.get("CLUSTER", "DB_PASSWORD")
    DWH_PORT = config.get("CLUSTER", "DB_PORT") 

    # aws environment configure
    os.environ['AWS_ACCESS_KEY_ID']=KEY
    os.environ['AWS_SECRET_ACCESS_KEY']=SECRET

    # create connection and cursor objects
    con = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(DWH_ENDPOINT, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT))    
    cur = con.cursor()
    
    # run the functions
    drop_tables(cur, con)                      
    create_staging_tables(cur, con)
    load_staging_tables(cur, con)
    create_insert_normalized_tables(cur, con)                       

    con.close()


if __name__ == "__main__":
    main()