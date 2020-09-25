import requests
import json
from common.credentials import jira_sprint_request, jira_auth
from common.data_extractor_dao import executeQuery, executeQueryAndReturnDF
import cog.db_queries as dq
import pandas as pd
from common.audit_logger import createSubTask, getMaintaskId, insertErrorLog, updateSubTask


def getSprints(maxSprintId, rapidBoardId, teamId):
    """
    getter method for jira sprints for given team board

    Args:
    Integer: sprint board identifire to pull respective sprint details using jira api
    Integer: team dim id to add reference in datawarehousetable while inserting sprint summary

    Returns:
    Dataframe: returns pandas dataframe with list of jira projects
    """
    # creaet data frame to hold sprint details
    col_names = ['sprint_serial_id', 'sprint_number', 'sprint_name', 'team_dim_id', 'sprint_start_date', 'sprint_end_date', 'sprint_complete_date']
    sprints = pd.DataFrame(columns=col_names)

    subtaskid = createSubTask("pull all latest sprints for SE teams from JIRA", getMaintaskId())
    try:
        # pull sprints for give rapid board id only
        jira_sprint_req = jira_sprint_request.replace('@@rapid_board_id@@', '{0}'.format(rapidBoardId))
        is_last = False
        startAt = maxSprintId
        sprint_serial_id = maxSprintId
        # iterate through sprints till all the sprints are pulled
        while(not is_last):
            response = requests.get(jira_sprint_req + "&startAt={0}".format(startAt), auth=(jira_auth))
            jira_sprints = json.loads(response.text)
            is_last = jira_sprints['isLast']
            for sprint in jira_sprints['values']:
                startAt = startAt + 1
                sprint_serial_id = sprint_serial_id + 1
                if sprint['originBoardId'] == rapidBoardId:
                    sprints = sprints.append({'sprint_serial_id': sprint_serial_id, 'sprint_number': sprint['id'], 'sprint_name': sprint['name'], 'team_dim_id': teamId, 'sprint_start_date': sprint['startDate'], 'sprint_end_date': sprint['endDate'], 'sprint_complete_date': sprint['completeDate']}, ignore_index=True)
        updateSubTask(subtaskid, "SUCCESS")
    except (Exception) as error:
        insertErrorLog(subtaskid, error)
    return sprints.drop_duplicates()


def putSprintSummary(sprints_for_team):
    """
    This method insert sprint summary for team in idw.sprint_summary table

    Args:
    Dataframe: pandas dataframe with all the latest sprints for team

    Returns:
    No Return
    """
    subtaskid = createSubTask("initialize database insert of sprint summary", getMaintaskId())
    try:
        sprints_for_team.apply(lambda sprint: executeQuery(dq.insert_sprints.format(sprint['sprint_serial_id'], sprint['sprint_number'], sprint['sprint_name'], sprint['team_dim_id'], sprint['sprint_start_date'], sprint['sprint_end_date'], sprint['sprint_complete_date'])), axis=1)
        updateSubTask(subtaskid, "SUCCESS")
    except (Exception) as error:
        insertErrorLog(subtaskid, error)


def initPutSprintSummary():
    """
    this method loop's through all the active teams in datawarehouse and insert sprint summary in idw.sprint_summary table

    Args:
    No Arguments

    Returns:
    No Return
    """ 
    subtaskid = createSubTask("insert sprint summary in datawarehouse table idw.jira_sprint_summary", getMaintaskId())
    try:
        team_rapid_board = executeQueryAndReturnDF(dq.get_max_sprint)
        team_rapid_board.apply(lambda teams: putSprintSummary(getSprints(int(teams['sprint_number']), int(teams['jira_rapid_view_id']), int(teams['team_dim_id']))), axis=1)
        updateSubTask(subtaskid, "SUCCESS")
        return team_rapid_board
    except (Exception) as error:
        insertErrorLog(subtaskid, error)
