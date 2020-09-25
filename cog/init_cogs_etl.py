from cog.load_jira_projects import insertProjects, updateProductId
from cog.load_sprint_summary import initPutSprintSummary
from cog.load_project_epics import initProjectEpicRequest, updateEpicDimId
from cog.load_sprint_issues import initSprintIssues, truncateStaging
from common.audit_logger import createMainTask, updateMainTask, getMaintaskId
from cog.backup_staging_history import backupStaging
from cog.jira_fact_mapper import generate_mapper
from cog.static_queries import executeStaticQueries


def init_codgs_etl():
    #createMainTask("Cost of Good sold Data Extraction", 'COGS')
    insertProjects()
    executeStaticQueries()
    updateProductId()
    initPutSprintSummary()
    initSprintIssues()
    generate_mapper()
    initProjectEpicRequest()
    updateEpicDimId()
    backupStaging(getMaintaskId())
    #truncateStaging()
    #updateMainTask(getMaintaskId(), "SUCCESS", "Cost of Good sold Data Extraction")

 
def handler(event, context):
    init_codgs_etl()


if __name__ == "__main__":
    init_codgs_etl()
