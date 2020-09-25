import psycopg2
from common.data_extractor_dao import dbConnection
from servicedesk.db_queries import update_case_origin_fact, fact_table_insert, fact_table_update, fact_table_map_update_date, fact_table_map_resolution_date, create_guid_mapping_status, update_contact_guid_mapping, fact_table_update_test_incidents_as_inactive, fact_table_update_BenE_incidents_as_inactive
from common.audit_logger import createSubTask, updateSubTask, insertErrorLog, getMaintaskId

""" Generating FACT tables """


def generateFacttable():
    """ Execute the Fact table queries
    Args:
    No Arguments
    Returns:
    No return variable
    """
    subtaskid = createSubTask(
        "fact table generation and measure calculation", getMaintaskId())
    conn = dbConnection()
    try:
        cur = conn.cursor()
        cur.execute(fact_table_update)
        cur.execute(fact_table_insert)
        cur.execute(fact_table_map_update_date)
        cur.execute(fact_table_map_resolution_date)
        cur.execute(create_guid_mapping_status)
        cur.execute(update_contact_guid_mapping)
        cur.execute(fact_table_update_test_incidents_as_inactive)
        cur.execute(fact_table_update_BenE_incidents_as_inactive)
        cur.execute(update_case_origin_fact)
        conn.commit()
        updateSubTask(subtaskid, "SUCCESS")
    except (Exception, psycopg2.Error) as error:
        insertErrorLog(subtaskid, error)

    finally:
        # closing database connection.
        if(conn):
            cur.close()
            conn.close()
