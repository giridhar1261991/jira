from common.variables import jira_url, jira_project, datawarehouse_name, user_name, password, datawarehouse_host, jira_api_usr, jira_api_password

""" This class is to create connection starings for
       1. Database connection
       2. jira api connection


 This class is to create connection starings for
    1. Database connection
    2. jira api connection

 These variables are used by other scripts for respective connection.
"""

# Fetch db details from variables
datawarehouse_db_config = {
    'database': '{}'.format(datawarehouse_name),
    'user': '{}'.format(user_name),
    'password': '{}'.format(password),
    'host': '{}'.format(datawarehouse_host)
}


datawarehouse_db_engine = 'postgresql+psycopg2://{0}:{1}@{2}:5432/{3}'.format(
    user_name, password, datawarehouse_host, datawarehouse_name)

# Fetch JIRA API details from variables
jira_api = {
    'username': '{}'.format(jira_api_usr),
    'password': '{}'.format(jira_api_password)
}

jira_workflow_request = '{0}/rest/api/2/project/{1}/statuses/'.format(jira_url, jira_project)
jira_resolution_request = '{0}/rest/api/2/resolution'.format(jira_url)
jira_get_assignee_request = '{0}/rest/api/2/user/assignable/search?project={1}&maxResults=1000'.format(jira_url, jira_project)
jira_get_product_request = '{0}/rest/api/latest/issue/createmeta?projectkey={1}&issuetypeIds=10000&expand=projects.issuetypes.fields'.format(jira_url, jira_project)
jira_auth = '{}'.format(jira_api_usr), '{}'.format(jira_api_password)


# below are request urls for Cost of Goods Sold

jira_project_request = '{0}/rest/api/2/project'.format(jira_url)
jira_sprint_issues = '{0}/rest/agile/1.0/sprint/@@sprint_id@@/issue/?expand=changelog&fields=-customfield_11502,-summary,-customfield_12725,-comment,-parent,-subtasks,-description,-customfield_12600,-customfield_12501,-customfield_12720,-issuelinks'.format(jira_url)
jira_sprint_request = '{0}/rest/agile/1.0/board/@@rapid_board_id@@/sprint/?state=closed'.format(jira_url)
jira_epic_request = '{0}/rest/api/2/search?jql=issuetype=Epic AND project=@@project_name@@ and (created>="2020-01-01" or updated>="2020-01-01")&fields=created,updated,key,customfield_12741,resolutiondate,customfield_10416,customfield_12740'.format(jira_url)
