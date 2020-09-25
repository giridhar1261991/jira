

remove_duplicate_transition_log = ('''create temp table jira_sd_workflow_transition_map_bkp as
SELECT jira_key, to_status_dim_id, from_status_dim_id, status_change_date, is_current_state, time_spent_in_status_in_days
, time_spent_in_status_in_hours, time_spent_in_status_in_minutes, issue_created_date, status_change_date_dim_id, to_assignee_dim_id
, from_assignee_dim_id, to_department_dim_id, from_department_dim_id, time_spent_by_assignee_in_days, time_spent_by_assignee_in_hours
, time_spent_by_assignee_in_minutes, is_current_assignee, issue_resolved_date
 FROM idw.jira_sd_workflow_transition_map
 group by jira_key, to_status_dim_id, from_status_dim_id, status_change_date, is_current_state, time_spent_in_status_in_days
, time_spent_in_status_in_hours, time_spent_in_status_in_minutes, issue_created_date, status_change_date_dim_id, to_assignee_dim_id
, from_assignee_dim_id, to_department_dim_id, from_department_dim_id, time_spent_by_assignee_in_days, time_spent_by_assignee_in_hours
, time_spent_by_assignee_in_minutes, is_current_assignee, issue_resolved_date having count(1)>1;

delete from idw.jira_sd_workflow_transition_map b
using jira_sd_workflow_transition_map_bkp t where
COALESCE(b.jira_key,'0')=COALESCE(t.jira_key,'0') and
COALESCE(b.to_status_dim_id,0)=COALESCE(t.to_status_dim_id,0) and
COALESCE(b.from_status_dim_id,0)=COALESCE(t.from_status_dim_id,0) and
COALESCE(b.status_change_date, now())=COALESCE(t.status_change_date,now()) and
COALESCE(b.is_current_state,'0')=COALESCE(t.is_current_state,'0') and
COALESCE(b.time_spent_in_status_in_days,0)=COALESCE(t.time_spent_in_status_in_days,0) and
COALESCE(b.time_spent_in_status_in_hours,0)=COALESCE(t.time_spent_in_status_in_hours,0) and
COALESCE(b.time_spent_in_status_in_minutes,0)=COALESCE(t.time_spent_in_status_in_minutes,0) and
COALESCE(b.issue_created_date,now())=COALESCE(t.issue_created_date,now()) and
COALESCE(b.status_change_date_dim_id,0)=COALESCE(t.status_change_date_dim_id,0) and
COALESCE(b.to_assignee_dim_id,0)=COALESCE(t.to_assignee_dim_id,0) and
COALESCE(b.from_assignee_dim_id,0)=COALESCE(t.from_assignee_dim_id,0) and
COALESCE(b.to_department_dim_id,0)=COALESCE(t.to_department_dim_id,0) and
COALESCE(b.from_department_dim_id,0)=COALESCE(t.from_department_dim_id,0) and
COALESCE(b.time_spent_by_assignee_in_days,0)=COALESCE(t.time_spent_by_assignee_in_days,0) and
COALESCE(b.time_spent_by_assignee_in_hours,0)=COALESCE(t.time_spent_by_assignee_in_hours,0) and
COALESCE(b.time_spent_by_assignee_in_minutes,0)=COALESCE(t.time_spent_by_assignee_in_minutes,0) and
COALESCE(b.is_current_assignee,'0')=COALESCE(t.is_current_assignee,'0') and
COALESCE(b.issue_resolved_date,now())=COALESCE(t.issue_resolved_date,now());

insert into idw.jira_sd_workflow_transition_map(jira_key, to_status_dim_id, from_status_dim_id, status_change_date, is_current_state, time_spent_in_status_in_days
, time_spent_in_status_in_hours, time_spent_in_status_in_minutes, issue_created_date, status_change_date_dim_id, to_assignee_dim_id
, from_assignee_dim_id, to_department_dim_id, from_department_dim_id, time_spent_by_assignee_in_days, time_spent_by_assignee_in_hours
, time_spent_by_assignee_in_minutes, is_current_assignee, issue_resolved_date)
select jira_key, to_status_dim_id, from_status_dim_id, status_change_date, is_current_state, time_spent_in_status_in_days
, time_spent_in_status_in_hours, time_spent_in_status_in_minutes, issue_created_date, status_change_date_dim_id, to_assignee_dim_id
, from_assignee_dim_id, to_department_dim_id, from_department_dim_id, time_spent_by_assignee_in_days, time_spent_by_assignee_in_hours
, time_spent_by_assignee_in_minutes, is_current_assignee, issue_resolved_date from jira_sd_workflow_transition_map_bkp;''')
