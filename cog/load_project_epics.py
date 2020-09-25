import requests
import json
import re
from common.credentials import jira_auth, jira_epic_request
from common.data_extractor_dao import executeQuery, executeQueryAndReturnDF
import cog.db_queries as dq
import pandas as pd
from common.audit_logger import createSubTask, getMaintaskId, insertErrorLog, updateSubTask


def getMaxEpicID(project_dim_id):
    """
    getter method for pulling epics from JIRA for given project

    Args:
    String: Project key for which method will pull all epics (only jan'2020 onwards)

    Returns:
    Dataframe: returns pandas dataframe with list of epics for given project
    """
    maxEpicId = executeQueryAndReturnDF(dq.get_max_epic_id.format(project_dim_id))
    return int(maxEpicId['epic_id'])


def getProjectEpic(projectKey, project_dim_id):
    """
    getter method for pulling epics from JIRA for given project

    Args:
    String: Project key for which method will pull all epics (only jan'2020 onwards)

    Returns:
    Dataframe: returns pandas dataframe with list of epics for given project
    """

    col_names = ['epic_name', 'epic_id', 'epic_key', 'aha_score', 'is_closed', 'date_created', 'date_updated', 'date_resolved', 'aha_reference']
    epics = pd.DataFrame(columns=col_names)
    subtaskid = createSubTask("pull epics worked on by project from JIRA", getMaintaskId())
    try:
        max_epic_id = getMaxEpicID(project_dim_id)
        jira_epics_req = jira_epic_request.replace('@@project_name@@', '{0}'.format(projectKey)).replace('@@epic_id@@', '{0}'.format(max_epic_id))
        response = requests.get(jira_epics_req, auth=(jira_auth))
        jira_epics = json.loads(response.text)
        for epic in jira_epics['issues']:

            is_closed = False

            if not epic['fields']['resolutiondate'] is None:
                is_closed = True
            epics = epics.append({'epic_name': epic['fields']['customfield_10416'],
                                 'epic_id': epic['id'], 'epic_key': epic['key'],
                                  'aha_score': epic['fields']['customfield_12741'], 'is_closed': is_closed,
                                  'date_created': epic['fields']['created'], 'date_updated': epic['fields']['updated'],
                                  'date_resolved': epic['fields']['resolutiondate'],
                                  'aha_reference': '{0}'.format(epic['fields']['customfield_12740'])}, ignore_index=True)
        updateSubTask(subtaskid, "SUCCESS")
    except (Exception) as error:
        insertErrorLog(subtaskid, error)
    return epics


def initProjectEpicRequest():
    """
    this method get list of distinct projects worked on by team and
    initiate database insert for list of epics returned by getProjectEpic()

    Args:
    No Arguments

    Returns:
    No Return
    """
    subtaskid = createSubTask("pull epics worked on by project from JIRA and initiate data insert in idw.epic_dim", getMaintaskId())
    try:
        team_projects = executeQueryAndReturnDF(dq.get_team_projects)
        team_projects.apply(lambda project: insertEpics(getProjectEpic(project['project_key'], int(project['project_dim_id'])), int(project['project_dim_id']), int(project['product_dim_id'])), axis=1)
        updateSubTask(subtaskid, "SUCCESS")
    except (Exception) as error:
        insertErrorLog(subtaskid, error)


def insertEpics(df_epics, project_dim_id, product_dim_id):
    """
    this method perform database insert operation in idw.epic_dim,
    it insert epics returned by getProjectEpic() for given project

    Args:
    dataFrame: first argument is pandas dataframe which contain list of epics for given project
    project_dim_id: second argument is project dim id for which epics will be isnerted in idw.epic_dim
    product_dim_id: product dim id to which epics belong

    Returns:
    No Return
    """
    subtaskid = createSubTask("insert epic worked on by project in warehouse table in idw.epic_dim", getMaintaskId())
    try:
        df_epics.apply(lambda epic: executeQuery((dq.insert_epics.format(re.sub('[^A-Za-z0-9 ]+', '', str(epic['epic_name'])), epic['epic_id'], epic['epic_key'], epic['aha_score'], epic['is_closed'], epic['date_created'], None if epic['date_updated'] is None else "'" + epic['date_updated'] + "'", None if epic['date_resolved'] is None else "'" + epic['date_resolved'] + "'", project_dim_id, "'" + epic['aha_reference'] + "'")).replace('None', 'null').replace('nan', 'null')), axis=1)
        updateSubTask(subtaskid, "SUCCESS")
    except (Exception) as error:
        insertErrorLog(subtaskid, error)


def updateEpicDimId():
    """
    this method update epic_dim_id in idw.sprint_issues

    Args:
    No argument

    Returns:
    No Return
    """
    subtaskid = createSubTask("update epic_dim_id in idw.sprint_issues", getMaintaskId())
    try:
        executeQuery(dq.update_epic_dim_id)
        updateSubTask(subtaskid, "SUCCESS")
    except (Exception) as error:
        insertErrorLog(subtaskid, error)
