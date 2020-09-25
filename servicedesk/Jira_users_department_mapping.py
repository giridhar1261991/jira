import psycopg2
from common.data_extractor_dao import dbConnection
import pandas as pd
from servicedesk.db_queries import department_dim_insert, department_mapping_in_user, department_mapping_in_transition, update_department_id_in_fact
from common.audit_logger import getMaintaskId, createSubTask, updateSubTask, insertErrorLog
import io


def user_department_mapping():
    """ Map department ID to user, fact and transition map table
    Args:
    No Arguments
    Returns:
    No return variable
    """
    subtaskid = createSubTask("Map department ID to user, fact and transition map table", getMaintaskId())
    try:
        mapping = pd.read_csv('user_department_mapping.csv')
        conn = dbConnection()
        cur = conn.cursor()
        cur.execute('''
            CREATE TEMP TABLE mapping
            (
            displayName VARCHAR(200),
            SamAccountName VARCHAR(100),
            Department VARCHAR(150)
            )
        ''')
        output = io.StringIO()
        mapping.to_csv(output, sep='|', header=True, index=False)
        output.seek(0)
        copy_query = "COPY mapping FROM STDOUT csv DELIMITER '|' NULL ''  ESCAPE '\\' HEADER "
        # inserting data into the database
        cur.copy_expert(copy_query, output)
        cur.execute(department_dim_insert)
        cur.execute(department_mapping_in_user)
        conn.commit()
        cur.execute(update_department_id_in_fact)
        cur.execute(department_mapping_in_transition)
        conn.commit()
        updateSubTask(subtaskid, "SUCCESS")
    except (Exception, psycopg2.Error) as error:
        insertErrorLog(subtaskid, error)
