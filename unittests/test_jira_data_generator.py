import unittest
from common.data_extractor_dao import dbConnection
from servicedesk.jira_api_dao import Jira
from common.credentials import jira_api
from datetime import datetime, timedelta
from servicedesk.jira_data_generator import handler
from unittests.test_case_origin import TestCaseOrigin


class TestJiraApiDao(unittest.TestCase):
    def test_etl(self):
        print('starting end to end unitest')
        handler(None, None)
        print('End to end unitest Complete')

    def test_jira_api_dao(self):
        s_date = datetime(2020, 4, 8)
        st_date = s_date - timedelta(hours=5, minutes=0)
        st = str(st_date.strftime('%Y-%m-%d %H:%M'))
        en_date = s_date + timedelta(hours=24, minutes=0)
        en = str(en_date.strftime('%Y-%m-%d'))
        MyJira = Jira(**jira_api)

        # Condition JIRA API JQL to fetch the required issues
        Condition_string = "project='SERVICDESK' and created >= '" + st + "' and created < '" + en + "'"
        print(Condition_string)
        List_var = MyJira.getIssues(condition=Condition_string)
        list_DataFrame = MyJira.create_df(List_var)
        print(list_DataFrame)
        print(list_DataFrame.shape[0])
        self.assertEqual(list_DataFrame.shape[0], 13)
        # self.assertEqual(x1,y)

    def test_jira_api_dao_compare(self):
        s_date = datetime(2020, 4, 8)
        jira_date = str(s_date)
        st_date = s_date - timedelta(hours=5, minutes=0)
        st = str(st_date.strftime('%Y-%m-%d %H:%M'))
        en_date = s_date + timedelta(hours=24, minutes=0)
        en = str(en_date.strftime('%Y-%m-%d'))
        MyJira1 = Jira(**jira_api)
        # Condition JIRA API JQL to fetch the required issues
        Condition_string1 = f"project='SERVICDESK' and created >= '" + st + "' and created < '" + en + "'"
        List_var1 = MyJira1.getIssues(condition=Condition_string1)
        list_DataFrame1 = MyJira1.create_df(List_var1)
        conn = dbConnection()
        cur = conn.cursor()
        cur.execute("select count(*) from idw.jira_sd_fact a left outer join idw.date_dimension b on a.date_created_dim_id=b.sk_date where a.is_active='true' and b.dt='" + jira_date + "'")
        rows = cur.fetchall()
        self.assertEqual(list_DataFrame1.shape[0], rows[0][0])

    def test_jira_api_contact_compare(self):
        MyJira1 = Jira(**jira_api)
        # Condition JIRA API JQL to fetch the required issues
        Condition_string1 = f"project='SERVICDESK' and key='SERVICDESK-20310'"
        List_var1 = MyJira1.getIssues(condition=Condition_string1)
        list_DataFrame1 = MyJira1.create_df(List_var1)
        jira_con = (list_DataFrame1['crm_contact'])
        jira_contact = str(jira_con[0])
        conn = dbConnection()
        cur = conn.cursor()
        cur.execute("select b.contactid from idw.jira_sd_fact a inner join idw.contact_dim b on a.contact_dim_id=b.contact_dim_id where a.jira_key='SERVICDESK-20310' and a.is_active=true")
        rows = cur.fetchall()
        fact_contact = str(rows[0][0])
        self.assertEqual(jira_contact, fact_contact)

    def test_data_extractor_dao(self):
        # connect to postgresql (w/ psycopg2)
        conn = dbConnection()
        self.assertIsNotNone(conn)
        conn.close()

    def test_case_origin(self):
        tco = TestCaseOrigin()
        tco.test_case_origin_id_compare()


if __name__ == '__main__':
    print('starting data sanity check')
    unittest.main()
    print('Data sanity check complete')
