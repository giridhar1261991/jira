from common.data_extractor_dao import executeQuery

backupQuery = ('''insert into birepusr.stage_sprint_issues_history
select *,{0} from birepusr.stage_sprint_issues;''')


def backupStaging(maintaskId):
    executeQuery(backupQuery.format(maintaskId))
