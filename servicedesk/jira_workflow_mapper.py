import psycopg2
from common.data_extractor_dao import dbEngine, dbConnection, executeQuery, executeQueryReturnId
from servicedesk.jira_api_dao import Jira
from common.credentials import jira_api
from servicedesk.db_queries import update_status_change_date_dim_id, getValidWorkflowAssignee, workflow_transition_time, workflow_status_dim_insert, insert_jira_workflow_map, getActiveandOpenJiraTickets, getValidWorkflowStatus, upsertTransitionLog, update_is_current_status_flag, update_workflow_transition_time, workflow_assignee_transition_time, update_assignee_transition_time, update_is_current_assignee_flag, update_resolution_date_intransition_map, insert_unchanged_assignee_in_transition_map
from common.audit_logger import createSubTask, updateSubTask, insertErrorLog, getMaintaskId
import pandas as pd
import io

df_status_ids = pd.DataFrame()
df_assignee_ids = pd.DataFrame()


def generateJiraWorkflowMap():
    """
     Execute the database insert to add jira transition map for each workflow status per jira key

     Args:
     No Arguments
     Returns:
     No return variable
    """
    subtaskid = createSubTask("generate workflow mapping for each active jira key", getMaintaskId())
    returnVal = executeQuery(insert_jira_workflow_map)
    updateSubTask(subtaskid, "SUCCESS") if returnVal == 'SUCCESS' else insertErrorLog(subtaskid, returnVal)


def updateWorkflowTransitionMap():
    """
     update worflow transition status for each jira_key-workflow status mapping
     in secondary fact table i.e idw.jira_sd_workflow_transition_map
     This function select only active and open jira tickets in fact table


     Args:
     No Arguments
     Returns:
     No return variable
    """
    subtaskid = createSubTask("update workflow transition log for each active and open jira key", getMaintaskId())
    conn = dbConnection()
    try:
        cur = conn.cursor()
        global df_status_ids, df_assignee_ids
        df_status_ids = pd.read_sql_query(getValidWorkflowStatus, con=dbEngine())
        df_status_ids = df_status_ids.append({'jira_status_dim_id': 0, 'jira_status': 'None', 'jira_api_id': 0}, ignore_index=True)
        df_assignee_ids = pd.read_sql_query(getValidWorkflowAssignee, con=dbEngine())

        df = pd.read_sql_query(getActiveandOpenJiraTickets, con=dbEngine())
        if not df.empty:
            MyJira = Jira(**jira_api)
            df.apply(lambda jira_key: updateTransitionLog(MyJira.getChangeLog(jira_key[0]), cur), axis=1)
            conn.commit()
        updateSubTask(subtaskid, "SUCCESS")
    except (Exception, psycopg2.Error) as error:
        insertErrorLog(subtaskid, error)
    finally:
        # closing database connection.
        if(conn):
            cur.close()
            conn.close()


def updateTransitionLog(df_log, cur):

    """
    This method apply lambda function on data frame received as argument
    and call insert log function for each log entry

    Args:
    pandas datafarme: data frame contain change log for given jira_key
    pandas dataframe: data frame contain all the workflow status applicable to project servicedesk
    database cursor : active instance of database to avoid multiple db connection attempts for each record insert
    Returns:
    No return variable
    """

    try:
        if not df_log.empty:
            df_log['to_status_id'] = df_log.apply(lambda tlog: getStatusID(tlog['to_status'], tlog['to_id']), axis=1)
            df_log['from_status_id'] = df_log.apply(lambda tlog: getStatusID(tlog['from_status'], tlog['from_id']), axis=1)
            df_log['to_assignee_dim_id'] = df_log.apply(lambda tlog: getAssigneeID(tlog['to_user'], tlog['to_user_name']), axis=1)
            df_log['from_assignee_dim_id'] = df_log.apply(lambda tlog: getAssigneeID(tlog['from_user'], tlog['from_user_name']), axis=1)

            df_log.apply(lambda tlog: cur.execute(getJiratransitionlogUpsertQuery(tlog)), axis=1)

    except (Exception, psycopg2.Error) as error:
        raise error


def getJiratransitionlogUpsertQuery(tlog):
    """
    This function frame insert/update transition log for given jira key and to_status entry.

    Args:
    data row: data row containing change log for given jira_key
    Returns:
    String: upsert query is returned for given transition records
    """
    update_query = upsertTransitionLog.format(tlog['jira_key'], tlog['to_status_id'], tlog['from_status_id'], tlog['date'], tlog['issue_create_date'], tlog['to_assignee_dim_id'], tlog['from_assignee_dim_id'])
    return update_query


def getStatusID(status_name, jira_status_id):
    global df_status_ids
    try:
        return df_status_ids[df_status_ids['jira_api_id'] == int(jira_status_id)]['jira_status_dim_id'].values[0]
    except(Exception):
        status_id = insertMissingWorkflowStatus(status_name, jira_status_id)
        df_status_ids = df_status_ids.append({'jira_status_dim_id': status_id, 'jira_status': status_name, 'jira_api_id': int(jira_status_id)}, ignore_index=True)
        return status_id


def getAssigneeID(assignee_id, assignee_name):
    global df_assignee_ids
    assignee_id = 'None' if assignee_id == 0 else assignee_id
    try:
        return df_assignee_ids[df_assignee_ids['jira_user_id'] == assignee_id]['jira_user_dim_id'].values[0]
    except(Exception):
        user_id = insertMissingWorkflowAssignee(assignee_id, assignee_name)
        df_assignee_ids = df_assignee_ids.append({'jira_user_dim_id': user_id, 'jira_user_id': assignee_id, 'jira_user_name': assignee_name}, ignore_index=True)
        return user_id


def insertMissingWorkflowAssignee(assignee_id, assignee_name):
    status_id = executeQueryReturnId('''insert into idw.jira_user_dim (jira_user_dim_id,jira_user_id,jira_user_name,jira_project,email_id,department_dim_id,is_active)
                            select COALESCE(max(jira_user_dim_id),0)+1,'{0}','{1}','SERVICDESK',null,null,true from idw.jira_user_dim returning jira_user_dim_id'''.format(assignee_id, assignee_name))
    return status_id


def insertMissingWorkflowStatus(status_name, jira_status_id):
    status_id = executeQueryReturnId('''insert into idw.jira_status_dim (jira_status_dim_id,jira_status,is_workflow_status,jira_api_id)
                            select COALESCE(max(jira_status_dim_id),0)+1,'{0}',true,{1} from idw.jira_status_dim returning jira_status_dim_id'''.format(status_name, jira_status_id))
    return status_id


def updateIsCurrentFlag():
    """
     Update is current workflow status and assignee flag for each jira key in secondary map

     Args:
     No Arguments
     Returns:
     No return variable
    """
    subtaskid = createSubTask("update is current workflow status and assignee flag for each jira key in secondary map", getMaintaskId())
    returnVal = executeQuery(update_is_current_status_flag + " " + update_is_current_assignee_flag)
    updateSubTask(subtaskid, "SUCCESS") if returnVal == 'SUCCESS' else insertErrorLog(subtaskid, returnVal)


def getworkflowStatus():

    subtaskid = createSubTask("get list of valid workflow status for SERVICDESK project from jira api", getMaintaskId())

    MyJira = Jira(**jira_api)
    df_workflow_status = MyJira.getworkflow()

    df_resolution_status = MyJira.getworkflowResolution()
    df_workflow_status = df_workflow_status.append(df_resolution_status, ignore_index=True)
    engine = dbConnection()
    cur = engine.cursor()
    cur.execute('''
        CREATE temp TABLE workflow_status
        (
        status VARCHAR(80),
        id integer,
        flag BOOLEAN
        )
    ''')
    output = io.StringIO()
    df_workflow_status.to_csv(output, sep='\t', header=True, index=False)
    output.seek(0)
    copy_query = "COPY workflow_status FROM STDOUT csv DELIMITER '\t' NULL ''  ESCAPE '\\' HEADER "
    # inserting data into the database
    cur.copy_expert(copy_query, output)
    try:
        cur.execute(workflow_status_dim_insert)
        engine.commit()
        updateSubTask(subtaskid, "SUCCESS")
    except (Exception, psycopg2.Error) as error:
        insertErrorLog(subtaskid, error)
    finally:
        # closing database connection.
        if(engine.commit()):
            engine.commit().close()


def updateMeasuresInTransitionMap():
    """
     This method initiate multiple update statements to update time_spent measure for status and assignee
     It also insert missing assignee for those jira which  never went trough assignee transition
     It update resolution date for all jira in transition map


     Args:
     No Arguments
     Returns:
     No return variable
    """
    subtaskid = createSubTask("calculate and  update measures in transition map table", getMaintaskId())
    returnVal = executeQuery(update_resolution_date_intransition_map)
    returnVal = executeQuery(insert_unchanged_assignee_in_transition_map) if returnVal == 'SUCCESS' else insertErrorLog(subtaskid, returnVal)
    returnVal = executeQuery(workflow_transition_time + " " + update_workflow_transition_time + " " + workflow_assignee_transition_time + " " + update_assignee_transition_time + " " + update_status_change_date_dim_id) if returnVal == 'SUCCESS' else insertErrorLog(subtaskid, returnVal)
    updateSubTask(subtaskid, "SUCCESS") if returnVal == 'SUCCESS' else insertErrorLog(subtaskid, returnVal)


def initSecondaryFactGenerator():
    generateJiraWorkflowMap()
    updateWorkflowTransitionMap()
    updateIsCurrentFlag()
    updateMeasuresInTransitionMap()
