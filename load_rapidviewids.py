import requests
import json
import re
from common.credentials import jira_auth
from common.data_extractor_dao import executeQuery, executeQueryAndReturnDF
import cog.db_queries as dq
import pandas as pd

def getRapidViewId():
    """
    getter method for pulling rapidviewids from JIRA

    """

    col_names = ['Rapidview_id', 'Rapidview_name', 'Owner_id', 'Owner_name']
    Rapidview = pd.DataFrame(columns=col_names)
    jira_Rapidview_req = 'https://jira.imo-online.com/rest/greenhopper/latest/rapidviews/list'
    response = requests.get(jira_Rapidview_req, auth=(jira_auth))
    jira_Rapidview = json.loads(response.text)
    for Rapidview_id in jira_Rapidview['views']:

        Rapidview = Rapidview.append({'Rapidview_id': Rapidview_id['id'],
                                'Rapidview_name': Rapidview_id['name'],
                                'Owner_id': Rapidview_id['filter']['owner']['userName'],
                                'Owner_name': Rapidview_id['filter']['owner']['displayName']
                                }, ignore_index=True)
    print(Rapidview)
    Rapidview.apply(lambda rapidView: executeQuery(dq.insert_rapidview.format(rapidView['Rapidview_id'], re.sub('[^A-Za-z0-9 ]+', '', str(rapidView['Rapidview_name'])), rapidView['Owner_id'], rapidView['Owner_name'])), axis=1)

if __name__ == "__main__" :
    getRapidViewId()

