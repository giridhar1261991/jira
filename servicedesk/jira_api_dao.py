from jira import JIRA
import pandas as pd
import requests
from common.credentials import jira_workflow_request, jira_resolution_request, jira_auth, jira_get_assignee_request, jira_get_product_request
from common.variables import jira_url
import json

requests.packages.urllib3.disable_warnings(
    requests.packages.urllib3.exceptions.InsecureRequestWarning)

"""
This is JIRA API access class.
This class provides getter methods to retrive servie desk tickets in  full extract or incrementally
"""


class JiraException(Exception):
    pass


# Connecting JIRA
class Jira(object):
    __options = {
        'server': jira_url,
        'verify': False
    }
    __client = None

    def __init__(self, **kwargs):
        if len(kwargs) != 2:
            raise JiraException(
                'In order to use this class you need to specify a user and a password as keyword arguments!')
        else:
            if 'username' in kwargs.keys():
                self.__username = kwargs['username']
            else:
                raise JiraException(
                    'You need to specify a username as keyword argument!')
            if 'password' in kwargs.keys():
                self.__password = kwargs['password']
            else:
                raise JiraException(
                    'You need to specify a password as keyword argument!')

            try:
                self.__client = JIRA(
                    self.__options,
                    basic_auth=(self.__username, self.__password))
            except(JiraException):
                raise JiraException(
                    'Could not connect to the API, invalid username or password!')

    # Fetch issues from JIRA API
    def getIssues(self, maxResults=100, raw=False, **kwargs):
        """ Connects to the jira API and fetch the issue list.
        Args:
        condition(str) : condition string for Jira JQL to fetch the required issues
        Returns:
        Issues(list) : list variable with issue entries
        """
        Issues = []
        # search_st=''
        i = 0
        chunk_size = 100
        while True:
            if len(kwargs) < 1:
                raise JiraException('You need to specify a search criteria!')
            else:
                searchstring = kwargs['condition']
            chunk = self.__client.search_issues(
                searchstring, startAt=i, maxResults=chunk_size)
            Issues.extend(chunk)
            if i >= chunk.total:
                break
            i += chunk_size
            print('Number of issues:', len(Issues))
        return Issues

    # Creating Pandas dataframe to insert JIRA data
    def create_df(self, data):
        """ Creates a dataframe with required fields from Jira API
        Args:
        JSON_list(list) : Accepts the list variable generated from Jira API
        Returns:
        JSON_DATA(dataframe) : Pandas Dataframe with required fields
        """
        JSON_DATA = pd.DataFrame()
        for x in data:
            col = {
                'key': x.key,
                'assignee': x.fields.assignee if str(x.fields.assignee) == 'None' else x.fields.assignee.key,
                'creator': x.fields.creator,
                'reporter': x.fields.reporter,
                'created': x.fields.created,
                'components': x.fields.components,
                'description': x.fields.description,
                'summary': x.fields.summary,
                'fixVersions': x.fields.fixVersions,
                'subtask': x.fields.issuetype.subtask,
                'issuetype': x.fields.issuetype.name,
                'priority': x.fields.priority.name,
                'resolution': x.fields.resolution,
                'resolution_date': x.fields.resolutiondate,
                'status_name': x.fields.status.name,
                'status_description': x.fields.status.description,
                'updated': x.fields.updated,
                'versions': x.fields.versions,
                'watches': x.fields.watches.watchCount,
                'jira_case': x.fields.customfield_12409,
                'crm_contact': x.fields.customfield_12410,
                'crm_account': x.fields.customfield_12411,
                'Reporter_Email': x.fields.customfield_11905,
                'channel': x.fields.customfield_11809,
                'product_name': x.fields.customfield_13609,
                'request_type': x.fields.customfield_11813,
                'assignee_name': x.fields.assignee

            }
            JSON_DATA = JSON_DATA.append(col, ignore_index=True)
        return JSON_DATA

    def getChangeLog(self, jiraid):
        issue = self.__client.issue(jiraid, expand='changelog')
        changelog = issue.changelog
        valuesarr = []
        col_names = ['jira_key', 'to_status', 'from_status', 'date', 'to_id', 'from_id', 'issue_create_date', 'to_user', 'from_user']
        df_log = pd.DataFrame(columns=col_names)
        for history in changelog.histories:
            valuesarr = [None, None, None, None, None, None, None, None]
            for item in history.items:
                if item.field == 'status':
                    valuesarr[0] = item.toString
                    valuesarr[1] = item.fromString
                    valuesarr[2] = item.to
                    valuesarr[3] = item.__dict__["from"]
                if item.field == 'assignee':
                    valuesarr[4] = item.to
                    valuesarr[5] = item.__dict__["from"]
                    valuesarr[6] = item.toString
                    valuesarr[7] = item.fromString
            if not valuesarr.count(None) == len(valuesarr):
                df_log = df_log.append({'jira_key': jiraid, 'to_status': valuesarr[0], 'from_status': valuesarr[1], 'date': history.created, 'to_id': valuesarr[2], 'from_id': valuesarr[3], 'issue_create_date': issue.fields.created, 'to_user': valuesarr[4], 'from_user': valuesarr[5], 'to_user_name': valuesarr[6], 'from_user_name': valuesarr[7]}, ignore_index=True)
        df_log.fillna(value=0, inplace=True)
        return df_log

    def getworkflow(self):
        response = requests.get(jira_workflow_request, auth=(jira_auth))
        workflow_status = json.loads(response.text)
        col_names = ['status', 'id', 'flag']
        jira_workflow = pd.DataFrame(columns=col_names)
        for issuetype in workflow_status:
            for item in issuetype['statuses']:
                jira_workflow = jira_workflow.append({'status': item['name'], 'id': item['id'], 'flag': 'true'}, ignore_index=True)

        jira_workflow.drop_duplicates(['id'], keep='first')
        return jira_workflow

    def getworkflowResolution(self):
        response = requests.get(jira_resolution_request, auth=(jira_auth))
        workflow_status = json.loads(response.text)
        col_names = ['status', 'id', 'flag']
        jira_workflow = pd.DataFrame(columns=col_names)
        for issuetype in workflow_status:
            if issuetype['name'] != 'Closed':
                jira_workflow = jira_workflow.append({'status': issuetype['name'], 'id': issuetype['id'], 'flag': 'false'}, ignore_index=True)

        jira_workflow.drop_duplicates(['id'], keep='first')
        return jira_workflow

    def getJiraUsers(self):
        response = requests.get(jira_get_assignee_request, auth=(jira_auth))
        assignees = json.loads(response.text)
        jira_assignees = pd.DataFrame(assignees)
        return jira_assignees[['key', 'displayName', 'emailAddress', 'active']]

    def getServiceDeskProducts(self):
        response = requests.get(jira_get_product_request, auth=(jira_auth))
        products = json.loads(response.text)
        col_names = ['product_name', 'api_id']
        jira_products = pd.DataFrame(columns=col_names)
        for projects in products['projects']:
            for issuetypes in projects['issuetypes']:
                if 'customfield_13609' in issuetypes['fields']:
                    for field in issuetypes['fields']['customfield_13609']['allowedValues']:
                        jira_products = jira_products.append({'product_name': field['value'], 'api_id': field['id']}, ignore_index=True)
        return jira_products.drop_duplicates()
