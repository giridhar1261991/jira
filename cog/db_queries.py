# this class contain db queries

insert_sprint_issues = ('''
update idw.sprint_issues i
set is_active=false
from birepusr.stage_sprint_issues s
where i.jira_ticket_id=s.jira_ticket_id
and i.sprint_number=s.sprint_number
and i.team_dim_id=s.team_dim_id
and i.is_active=true;

insert into idw.sprint_issues(team_dim_id, sprint_number, jira_ticket_id, jira_ticket_key, ticket_details, project_dim_id, is_active, date_created)
select team_dim_id, sprint_number, jira_ticket_id, jira_ticket_key, issues ticket_details, proj.project_dim_id, true, now()
from  birepusr.stage_sprint_issues issues
left outer join idw.jira_project_dim proj
on split_part(jira_ticket_key,'-',1) = project_key
where Lower(issues -> 'fields' -> 'issuetype' ->> 'name') != 'sub-task';''')

insert_projects = ('''
insert into idw.jira_project_dim (project_dim_id, project_id, project_key, project_name, is_active)
select (select coalesce(max(project_dim_id),0)+1 from idw.jira_project_dim) as project_dim_id , {0} , '{1}', '{2}', true
where not exists (select project_dim_id from idw.jira_project_dim where project_id = {0});''')

get_db_projects = ('''select project_id from idw.jira_project_dim;''')

update_product_dim_id_for_project = ('''update idw.jira_project_dim pj
set product_dim_id=p.jira_product_dim_id
from idw.jira_product_dim p
where lower(pj.project_name) LIKE '%' || lower(replace(p.jira_product_name,' ','%')) || '%'
and pj.product_dim_id is null;''')

get_teams = (
    '''select team_dim_id, team_name, jira_rapid_view_id from idw.team_dim where is_active=true''')

get_max_sprint = (
    '''select coalesce(max(sprint_serial_id),0) as sprint_number,jira_rapid_view_id, t.team_dim_id
from idw.team_dim t left join idw.jira_sprint_summary s
on t.team_dim_id=s.team_dim_id and coalesce(is_issues_loaded,false)=true
where t.is_active=true
group by jira_rapid_view_id,t.team_dim_id;''')

insert_sprints = (
    '''insert into idw.jira_sprint_summary (sprint_serial_id, sprint_number, sprint_name, team_dim_id, sprint_start_date, sprint_end_date, sprint_complete_date)
select {0}, {1}, '{2}', {3}, '{4}', '{5}', '{6}'
where not exists (select 'x' from idw.jira_sprint_summary where sprint_number={1});''')

get_sprints = ('''select sprint_number, team_dim_id from idw.jira_sprint_summary s where sprint_end_date >= '2020-01-01' and coalesce(is_issues_loaded, false)=false;''')

insert_issues_in_stage = (
    '''insert into birepusr.stage_sprint_issues (team_dim_id, sprint_number, jira_ticket_id, jira_ticket_key, issues, date_created)
select {0}, {1}, {2}, '{3}', '{4}', now();''')

get_team_projects = (
    '''select project_dim_id,project_key,coalesce(product_dim_id,0) as product_dim_id from idw.jira_project_dim where project_key in (
select DISTINCT SPLIT_PART(jira_ticket_key,'-',1) from  birepusr.stage_sprint_issues);''')

insert_epics = (
    '''insert into idw.epic_dim (epic_dim_id, epic_name, epic_id, epic_key, aha_score, is_closed, date_created, date_updated, date_resolved, project_dim_id, aha_reference)
select (select coalesce(max(epic_dim_id),0)+1 from idw.epic_dim), '{0}', {1}, '{2}', {3}, {4}, '{5}', {6}, {7}, {8}, {9}
where not exists (select 'x' from idw.epic_dim where epic_key='{2}');''')

insert_rapidview = ('''insert into idw.rapidview_dim (Rapidview_dim_id, Rapidview_id, Rapidview_name, Owner_id, Owner_name, date_created, is_active)
select (select coalesce(max(Rapidview_dim_id),0)+1 from idw.rapidview_dim) , {0}, '{1}', '{2}', '{3}', now(), true
where not exists (select 'x' from idw.rapidview_dim where Rapidview_id={0});''')

get_max_epic_id = (
    '''select coalesce(max(epic_id),0) as epic_id from idw.epic_dim e where project_dim_id={0};''')

update_issue_load_status = (
    '''update idw.jira_sprint_summary set is_issues_loaded = true where sprint_number={0} and team_dim_id={1}''')

update_epic_dim_id = ('''update idw.sprint_issues s
set epic_dim_id=e.epic_dim_id
from idw.epic_dim e
where (ticket_details -> 'fields' -> 'epic' ->> 'id')::numeric = e.epic_id
and s.epic_dim_id is null and s.is_active=true;''')

update_issue_status_on_sprint_closure = ('''update idw.sprint_issues si
set status_on_sprint_closure=coalesce(to_status,'open')
from (
select  jira_ticket_key,t.sprint_number,t.team_dim_id,sprint_start_date,sprint_end_date,
created::timestamp as status_change_date,
t1->>'fromString' as from_status,
t1->>'toString' as to_status,
row_number() over(partition by jira_ticket_key,t.sprint_number,t.team_dim_id order by created::timestamp desc) as row_num
from (select jira_ticket_key,sprint_number,team_dim_id,jsonb_array_elements(ticket_details->'changelog'->'histories')->>'created' as created,
      jsonb_array_elements(jsonb_array_elements(ticket_details->'changelog'->'histories') ->'items') as t1
      from idw.sprint_issues where is_active=true and status_on_sprint_closure is null) as t
      inner join idw.jira_sprint_summary ss on ss.sprint_number=t.sprint_number
where  t1->>'field'='status' and created::timestamp <= sprint_complete_date) a
where row_num=1 and si.jira_ticket_key=a.jira_ticket_key
and si.sprint_number=a.sprint_number
and si.team_dim_id=a.team_dim_id;

update idw.sprint_issues si
set status_on_sprint_closure='Open'
where is_active=true
and status_on_sprint_closure is null;''')
