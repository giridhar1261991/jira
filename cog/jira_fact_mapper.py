from cog.db_queries import insert_sprint_issues, update_issue_status_on_sprint_closure
from common.data_extractor_dao import executeQuery
from common.audit_logger import createSubTask, getMaintaskId, insertErrorLog, updateSubTask


def generate_mapper():
    """
     This method inserts data into the fact tables and map project and epic dim id

    Args:
    No Arguments
    Returns:
    No return variable
    """
    subtaskid = createSubTask("pull epics worked on by project from JIRA", getMaintaskId())
    try:
        executeQuery(insert_sprint_issues)
        executeQuery(update_issue_status_on_sprint_closure)
        updateSubTask(subtaskid, "SUCCESS")
    except (Exception) as error:
        insertErrorLog(subtaskid, error)
