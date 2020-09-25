from common.data_extractor_dao import executeQuery
from servicedesk.db_queries import request_type_dim_insert
from common.audit_logger import createSubTask, updateSubTask, insertErrorLog, getMaintaskId


def updateRequestTypeDim():
    """
     Execute the database insert and update query to populate
     missing request type identified from staging table

    Args:
    No Arguments
    Returns:
    No return variable
    """
    subtaskid = createSubTask(
        "add missing request type form stage data into idw.request_type_dim ",
        getMaintaskId())
    returnVal = executeQuery(request_type_dim_insert)
    updateSubTask(subtaskid, "SUCCESS") if returnVal == 'SUCCESS' else insertErrorLog(subtaskid, returnVal)
