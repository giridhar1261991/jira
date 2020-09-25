import psycopg2
from common.data_extractor_dao import dbConnection, executeQuery
from servicedesk.jira_api_dao import Jira
import io
from common.variables import jira_project
from common.credentials import jira_api
from servicedesk.contact_dim_generator import generateContactDim
from servicedesk.sd_fact_generator import generateFacttable
from common.audit_logger import getMaintaskId, createMainTask, createSubTask, updateMainTask, updateSubTask, insertErrorLog, getLastsuccessfulExtarctionDate
from servicedesk.request_type_dim_generator import updateRequestTypeDim
from servicedesk.jira_workflow_mapper import getworkflowStatus, initSecondaryFactGenerator
from servicedesk.db_queries import backup_raw_data
from servicedesk.jira_assignee_mapper import generateAssigneeDimension
from servicedesk.load_dim_case_orign import updateDimCaseOrigin
from servicedesk.load_jira_product_dim import upsertJiraProduct, persistJiraProductMap
from servicedesk.Jira_users_department_mapping import user_department_mapping
from servicedesk.sanity_check import sanity_check_counts
from servicedesk.db_data_cleanup_queries import remove_duplicate_transition_log
# Fetching the dimentions tables


def getJiraTickets():
    """ Provides condition for jira API JQL to fetch JIRA issue list
    Conditions that can be used for like : DURING (startOfWeek(-1w), endOfWeek(-1w)) and created >= '2020-01-01' AND status='Open and  AND created >= -28d
    Args:
    No Arguments
    Returns:
    Jsin_Dataframe(dataframe) : Pandas Dataframe with required fields
    """
    subtaskid = createSubTask("pull service desk incidents from JIRA API", getMaintaskId())
    try:
        MyJira = Jira(**jira_api)
        fromDate = getLastsuccessfulExtarctionDate()

        # Condition JIRA API JQL to fetch the required issues
        Condition_string = f"project='" + jira_project + "' "

        if fromDate is not None:
            Condition_string = Condition_string + "AND (created >'" + fromDate.strftime(
                "%Y-%m-%d") + "' OR Updated > '" + fromDate.strftime("%Y-%m-%d") + "')"

        JSON_List = MyJira.getIssues(condition=Condition_string)
        Json_DataFrame = MyJira.create_df(JSON_List)
        # Returning the pandas dataframe with JIRA issues
        updateSubTask(subtaskid, "SUCCESS")
        return Json_DataFrame
    except (Exception, psycopg2.Error) as error:
        insertErrorLog(subtaskid, error)

# Function to insert JIRA data into staging db schema


def stageJiraTickets():
    """ Insert JIRA issue data into staging db schema
    Args:
    No Arguments
    Returns:
    No return variable
    """
    subtaskid = createSubTask("extract JIRA data to staging", getMaintaskId())
    try:
        df = getJiraTickets()
        executeQuery('DELETE FROM birepusr.jira_sd_stage;')
        if not df.empty:
            # Arranging the columns of pandas dataframe to insert into the DB
            jiraData = df[['assignee', 'jira_case', 'components', 'created',
                           'creator', 'crm_account', 'crm_contact',
                           'description', 'fixVersions', 'issuetype', 'key',
                           'priority', 'reporter', 'resolution',
                           'resolution_date', 'status_description',
                           'status_name', 'subtask', 'summary', 'updated',
                           'versions', 'watches', 'Reporter_Email', 'channel',
                           'product_name', 'request_type', 'assignee_name']]
            engine = dbConnection()
            cur = engine.cursor()
            # Truncating the existing staging data
            output = io.StringIO()
            jiraData.to_csv(output, sep='\t', header=True, index=False)
            output.seek(0)
            copy_query = "COPY birepusr.jira_sd_stage FROM STDOUT csv DELIMITER '\t' NULL ''  ESCAPE '\\' HEADER "
            # inserting data into the database
            cur.copy_expert(copy_query, output)
            engine.commit()
        updateSubTask(subtaskid, "SUCCESS")
    except (Exception, psycopg2.Error) as error:
        insertErrorLog(subtaskid, error)


def handler(event, context):
    createMainTask("JIRA data extraction", "JIRA")
    stageJiraTickets()
    executeQuery(backup_raw_data)
    generateContactDim()
    updateRequestTypeDim()
    updateDimCaseOrigin()
    upsertJiraProduct()
    getworkflowStatus()
    generateAssigneeDimension()
    generateFacttable()
    persistJiraProductMap()
    initSecondaryFactGenerator()
    user_department_mapping()
    executeQuery(remove_duplicate_transition_log)
    sanity_check_counts()
    updateMainTask(getMaintaskId(), "SUCCESS", "JIRA data extraction")


if __name__ == "__main__":
    handler(None, None)
