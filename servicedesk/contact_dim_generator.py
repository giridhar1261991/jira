from common.data_extractor_dao import executeQuery
from servicedesk.db_queries import contact_dim_insert, contact_dim_update
from common.audit_logger import createSubTask, updateSubTask, insertErrorLog, getMaintaskId


def generateContactDim():
    """
     Execute the database insert and update query to populate
     contacts from mscrm.contact_entity table,

    Args:
    No Arguments
    Returns:
    No return variable
    """
    subtaskid = createSubTask("copy data from mscrm.contact_entity to idw.contact_dim", getMaintaskId())
    returnVal = executeQuery(contact_dim_insert + " " + contact_dim_update)
    updateSubTask(subtaskid, "SUCCESS") if returnVal == 'SUCCESS' else insertErrorLog(subtaskid, returnVal)
