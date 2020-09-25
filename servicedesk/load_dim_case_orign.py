from common.data_extractor_dao import executeQuery
from servicedesk.db_queries import load_dim_case_origin
from common.audit_logger import createSubTask, updateSubTask, insertErrorLog, getMaintaskId


def updateDimCaseOrigin():
    """
     Execute the database insert and update query to populate
     missing case origin identified from source table in mscrm

    Args:
    No Arguments
    Returns:
    No return variable
    """
    subtaskid = createSubTask(
        "add missing case origin from source data into idw.jira_origin_dim ",
        getMaintaskId())
    returnVal = executeQuery(load_dim_case_origin)
    updateSubTask(subtaskid, "SUCCESS") if returnVal == 'SUCCESS' else insertErrorLog(subtaskid, returnVal)
