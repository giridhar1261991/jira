import unittest
from common.data_extractor_dao import dbConnection
import requests
from common.credentials import jira_project_request, jira_auth
import json


class TestCog(unittest.TestCase):

    def test_api_connection(self):
        # check API connection
        response = requests.get(jira_project_request, auth=(jira_auth))
        projects = json.loads(response.text)
        if (len(projects) > 0):
            val = True
        else:
            val = False
        self.assertEqual(val, True)

    def test_data_extractor_dao(self):
        # connect to postgresql (w/ psycopg2)
        conn = dbConnection()
        self.assertIsNotNone(conn)
        conn.close()

    def test_sprint_completed_points(self):
        # check sprint completed non maintenance points
        conn = dbConnection()
        cur = conn.cursor()
        cur.execute("""select non_maintenance_feature_points
        from idw.jira_sprint_summary where sprint_number=2448""")
        rows = cur.fetchall()
        summary = round(rows[0][0])
        cur.execute("""select sum(points) as completed_points
        from (select sprint_number, team_dim_id, epic_dim_id,
        (ticket_details -> 'fields' ->> 'resolutiondate')::timestamp as resolutionDate,
        (ticket_details -> 'fields' ->> 'customfield_10411')::numeric as points
        from idw.sprint_issues where is_active=true and sprint_number=2448
        and is_maintenance=false) s
        inner join idw.jira_sprint_summary e
        on s.sprint_number=e.sprint_number and s.team_dim_id=e.team_dim_id
        where resolutionDate between sprint_start_date and sprint_end_date""")
        row1 = cur.fetchall()
        issues = round(row1[0][0])
        self.assertEqual(summary, issues)

    def test_number_of_devs(self):
        # check number of developers
        conn = dbConnection()
        cur = conn.cursor()
        cur.execute("select number_of_devs from idw.jira_sprint_summary where sprint_number=2506")
        rows = cur.fetchall()
        summary = round(rows[0][0])
        cur.execute("select count(u.user_dim_id) as no_of_devs  from idw.jira_sprint_summary ss inner join idw.user_team_map u on ss.team_dim_id=u.team_dim_id and u.team_entry_date < ss.sprint_start_date and coalesce(u.team_exit_date, now()) > ss.sprint_end_date where ss.sprint_number=2506 group by u.team_dim_id, ss.sprint_number order by u.team_dim_id, ss.sprint_number")
        row1 = cur.fetchall()
        issues = round(row1[0][0])
        self.assertEqual(summary, issues)


if __name__ == '__main__':
    print('starting unittest')
    unittest.main()
    print('Unittest complete')
