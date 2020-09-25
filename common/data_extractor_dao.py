
import psycopg2
from sqlalchemy import create_engine
from common.credentials import datawarehouse_db_config, datawarehouse_db_engine
from common.send_notification import send_custom_mail

"""
 This class is to create database access object,
 it provides access methods to perform database operations
"""


# Connecting PostgreSQL DB
def dbConnection():
    """ Creates the Postgre sql connection using the variable file.

    Args:
        No Arguments

    Returns:
        Postgre sql db connection.
    """
    conn = psycopg2.connect(**datawarehouse_db_config)
    return conn


def dbEngine():
    """ Creates the Postgre sql connection using the variable file.

    Args:
        No Arguments

    Returns:
        Postgre sql connection engine connection.
    """
    engine = create_engine(datawarehouse_db_engine)
    # print("Database opened successfully")
    return engine


def executeQuery(query):
    conn = dbConnection()
    try:
        cur = conn.cursor()
        cur.execute(query)
        conn.commit()
        return 'SUCCESS'
    except (Exception, psycopg2.Error) as error:
        print(error)
        send_custom_mail('database operation DW JIRA ETL', error)
        return error

    finally:
        # closing database connection.
        if(conn):
            cur.close()
            conn.close()


def executeQueryReturnId(query):
    conn = dbConnection()
    try:
        cur = conn.cursor()
        cur.execute(query)
        id = cur.fetchone()
        conn.commit()
        return id[0]
    except (Exception, psycopg2.Error) as error:
        print("Error while executing query", query, "Error", error)
        raise Exception

    finally:
        # closing database connection.
        if(conn):
            cur.close()
            conn.close()


def executeQueryAndReturnDF(query):
    """ Execute query on datawarehouse table and return query qiut put as pandas dataframe

    Args:
        query to be executed

    Returns:
        pandad dataframe: return query output as pandas data frame
    """
    import pandas as pd
    return pd.read_sql_query(query, con=dbEngine())


def execute_proc(procedure_name):
    """
     This method executes the procedure

    Args:
    String: Name of procedure to be executed in database
    Returns:
    No return variable
    """

    engine = dbConnection()
    cur = engine.cursor()
    cur.callproc(procedure_name)
    engine.commit()
