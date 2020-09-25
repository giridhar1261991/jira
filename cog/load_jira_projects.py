import requests
import json
import pandas as pd
from common.data_extractor_dao import executeQuery, executeQueryAndReturnDF
import cog.db_queries as dq
from common.credentials import jira_project_request, jira_auth
from common.audit_logger import createSubTask, getMaintaskId, insertErrorLog, updateSubTask


def getProjects():
    """
    getter method for jira projects with id and key

    Args:
    No Arguments
    Returns:
    returns pandas dataframe with list of jira projects
    """
    col_names = ['project_id', 'project_key', 'project_name']
    jira_projects = pd.DataFrame(columns=col_names)

    subtaskid = createSubTask(
        "generate feature list for a product", getMaintaskId())
    try:
        response = requests.get(jira_project_request, auth=(jira_auth))
        print(response)
        projects = json.loads(response.text)
        for project in projects:
            jira_projects = jira_projects.append(
                {'project_id': project['id'],
                 'project_key': project['key'],
                 'project_name': project['name']},
                ignore_index=True)

        updateSubTask(subtaskid, "SUCCESS")
    except (Exception) as error:
        insertErrorLog(subtaskid, error)

    return jira_projects.drop_duplicates()


def getdbProjects():
    """
    getter method for existing projects in warehouse table idw.jira_projects_dim

    Args:
    No Arguments
    Returns:
    returns pandas dataframe with list of jira projects in idw.jira_projects_dim
    """
    return executeQueryAndReturnDF(dq.get_db_projects)


def insertProjects():
    """
    db insert method for persisting new projects in warehouse table idw.jira_projects_dim

    Args:
    No Arguments
    Returns:
    No return
    """
    subtaskid = createSubTask(
        "pull projects from JIRA and insert in warehouse table idw.jira_project_dim",
        getMaintaskId())
    try:
        db_projects = getdbProjects()
        jira_projects = getProjects()
        jira_projects["project_id"] = jira_projects["project_id"].astype(int)
        comparison_df = jira_projects.merge(db_projects,
                                            indicator=True,
                                            how='outer', on='project_id')
        comparison_df[comparison_df['_merge'] == 'left_only'].apply(
            lambda
            project:
            executeQuery(
                dq.insert_projects.format(
                    project['project_id'],
                    project['project_key'],
                    project['project_name'])),
            axis=1)
        updateSubTask(subtaskid, "SUCCESS")
    except (Exception) as error:
        insertErrorLog(subtaskid, error)


def updateProductId():
    """
    update product dim id using product name matching with project name

    Args:
    No Arguments
    Returns:
    No return
    """
    subtaskid = createSubTask(
        "update product id for projects in idw.jira_project_dim",
        getMaintaskId())
    try:
        executeQuery(dq.update_product_dim_id_for_project)
        updateSubTask(subtaskid, "SUCCESS")
    except (Exception) as error:
        insertErrorLog(subtaskid, error)
