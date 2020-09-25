import requests
import json
from common.credentials import jira_auth, jira_sprint_issues
from common.data_extractor_dao import executeQuery, executeQueryAndReturnDF
import cog.db_queries as dq
import pandas as pd
from common.audit_logger import createSubTask, getMaintaskId, insertErrorLog, updateSubTask


def getSprintIssues(sprintId, subtaskid):
    """
    getter method for pulling issues from JIRA for given sprint-Id

    Args:
    Integer: sprint-Id for which method will pull issues

    Returns:
    Dataframe: returns pandas dataframe with list of issues for sprint
    """

    col_names = ['jira_ticket_id', 'jira_ticket_key' 'ticket_details']
    issues = pd.DataFrame(columns=col_names)

    try:
        jira_issues_req = jira_sprint_issues.replace('@@sprint_id@@', '{0}'.format(sprintId))
        is_last = False
        startAt = 0
        total_issues = 0
        while(not is_last):
            response = requests.get(jira_issues_req + "&startAt={0}".format(startAt), auth=(jira_auth))
            jira_issues = json.loads(response.text)
            for sprint in jira_issues['issues']:
                startAt = startAt + 1
                total_issues = total_issues + 1
                if total_issues == jira_issues['total']:
                    is_last = True
                issues = issues.append({'jira_ticket_id': sprint['id'], 'jira_ticket_key': sprint['key'], 'ticket_details': json.dumps(sprint)}, ignore_index=True)
    except (Exception) as error:
        insertErrorLog(subtaskid, error)
    return issues


def initSprintIssues():
    """
    this method loop's through all the active sprints for which
    issues are not pulled in datawarehouse,
    call getter method to get sprint issues and insert them in warehouse

    Args:
    No Arguments

    Returns:
    No Return 
    """
    subtaskid = createSubTask("initialize insertion of sprint issues in datawarehouse", getMaintaskId())
    try:
        team_sprints = executeQueryAndReturnDF(dq.get_sprints)
        team_sprints.apply(lambda sprint: insertSprintIssues(getSprintIssues(int(sprint['sprint_number']), subtaskid), int(sprint['sprint_number']), int(sprint['team_dim_id'])), axis=1)
        updateSubTask(subtaskid, "SUCCESS")
    except (Exception) as error:
        insertErrorLog(subtaskid, error)


def insertSprintIssues(df_sprint_issues, sprint_number, team_dim_id):
    """
    this method perform database insert operation in idw.sprint_issues,
    it insert issues completed in a sprint by given team

    Args:
    dataFrame: first argument is pandas dataframe which contain list of issues for given sprint for respective team
    sprint_number: second argument sprint number to map issues in destination table
    team_dim_id: team dim id to which sprint and issues belong

    Returns:
    No Return
    """
    subtaskid = createSubTask("insert sprint issues in datawarehouse staging table birepusr.stage_sprint_issues for sprint {0}".format(sprint_number), getMaintaskId())
    try:
        df_sprint_issues.apply(lambda sprint: executeQuery(dq.insert_issues_in_stage.format(team_dim_id, sprint_number, sprint['jira_ticket_id'], sprint['jira_ticket_key'], str(sprint['ticket_details']).replace("\'", ""))), axis=1)
        executeQuery(dq.update_issue_load_status.format(sprint_number, team_dim_id))
        updateSubTask(subtaskid, "SUCCESS")
    except (Exception) as error:
        insertErrorLog(subtaskid, error)


def truncateStaging():
    executeQuery('delete from birepusr.stage_sprint_issues;')
