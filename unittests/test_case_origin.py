import unittest
from common.data_extractor_dao import dbConnection


class TestCaseOrigin(unittest.TestCase):

    def test_case_origin_id_compare(self):
        conn = dbConnection()
        cur = conn.cursor()
        cur1 = conn.cursor()
        cur.execute("select jira_origin_dim_id from idw.jira_sd_fact where jira_key = 'SERVICDESK-10021'")
        cur1.execute("select jira_origin_dim_id FROM  mscrm.incident_entity cs LEFT JOIN idw.jira_origin_dim od ON COALESCE(cs.caseorigincodename,'None')= od.jira_origin_name where ticketnumber= 'CAS-07276-J4Q4G0'")
        fact_id_origin = cur.fetchall()[0]
        source_id_origin = cur1.fetchall()[0]
        self.assertEqual(source_id_origin, fact_id_origin)


if __name__ == '__main__':
    unittest.main()
