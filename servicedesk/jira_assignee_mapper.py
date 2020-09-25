import psycopg2
from servicedesk.jira_api_dao import Jira
from common.credentials import jira_api
from servicedesk.db_queries import upsert_assigne, insert_missing_assignee_from_api
from common.audit_logger import createSubTask, updateSubTask, insertErrorLog, getMaintaskId
from common.data_extractor_dao import dbConnection
import io


def generateAssigneeDimension():
    """
     This method get list of valid jira assignee and upsert them in
     idw.jira_user_dim

    Args:
    No Arguments
    Returns:
    No return variable
    """

    subtaskid = createSubTask(
        "Upsert jira users into idw.jira_user_dim ",
        getMaintaskId())

    engine = dbConnection()
    try:
        MyJira = Jira(**jira_api)
        jira_assignee = MyJira.getJiraUsers()
        jira_assignee = jira_assignee.drop_duplicates(['key'], keep='first')
        cur = engine.cursor()
        cur.execute('''
            CREATE temp TABLE jira_assignee
            (
            user_id VARCHAR(100),
            user_name VARCHAR(100),
            email_id VARCHAR(100),
            active boolean
            )
        ''')
        output = io.StringIO()
        jira_assignee.to_csv(output, sep='\t', header=True, index=False)
        output.seek(0)
        copy_query = "COPY jira_assignee FROM STDOUT csv DELIMITER '\t' NULL ''  ESCAPE '\\' HEADER "
        # inserting data into the database
        cur.copy_expert(copy_query, output)

        cur.execute(upsert_assigne)
        engine.commit()
        cur.execute(insert_missing_assignee_from_api)
        engine.commit()
        updateSubTask(subtaskid, "SUCCESS")
    except (Exception, psycopg2.Error) as error:
        insertErrorLog(subtaskid, error)
    finally:
        # closing database connection.
        if(engine.commit()):
            engine.commit().close()
