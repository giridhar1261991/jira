# example queries, will be different across different db platform

backup_raw_data = ('''
insert into birepusr.jira_sd_stage_raw
select *,now() from birepusr.jira_sd_stage;''')

contact_dim_update = ('''
    UPDATE idw.contact_dim cd
    SET
        salutation=ce.salutation,
        firstname=ce.firstname,
        lastname=ce.lastname,
        fullname=ce.fullname,
        jobtitle=ce.jobtitle,
        emailaddress=ce.emailaddress1
    FROM mscrm.contact_entity ce
    WHERE trim(cd.contactid)=trim(ce.contactid);
''')

contact_dim_insert = ('''
    INSERT INTO idw.contact_dim(contact_dim_id,contactid,salutation,firstname,lastname,fullname,jobtitle,emailaddress)
    SELECT (select COALESCE(max(contact_dim_id),0)+t.num from idw.contact_dim),
    t.contactid,t.salutation,t.firstname,t.lastname,
    t.fullname,t.jobtitle,t.emailaddress1 FROM
    (SELECT row_number() over () as num,ce.contactid,ce.salutation,ce.firstname,ce.lastname,
    ce.fullname,ce.jobtitle,ce.emailaddress1
FROM mscrm.contact_entity ce
    LEFT OUTER JOIN idw.contact_dim cd USING (contactid)
    WHERE cd.contactid is NULL
    ) as t;''')

department_dim_insert = ('''
    INSERT INTO idw.department_dim(department_dim_id,department_name)
    select (select COALESCE(max(department_dim_id),0)+t.num from idw.department_dim), department_name
    from (SELECT row_number() over () as num,COALESCE(sd.department,'None') as department_name
        FROM (select distinct department from mapping) sd
        where sd.Department not in (select department_name from idw.department_dim)
        ) as t;
''')

department_mapping_in_user = ('''
    update idw.jira_user_dim u
set department_dim_id=d.department_dim_id
from mapping m
inner join idw.department_dim d
on COALESCE(d.department_name, 'None') = COALESCE(trim(m.department), 'None')
where COALESCE(u.department_dim_id,0)=0 and lower(u.jira_user_id)=lower(trim(m.samaccountname));

update idw.jira_user_dim u
set department_dim_id=d.department_dim_id
from mapping m
inner join idw.department_dim d
on COALESCE(d.department_name, 'None') = COALESCE(trim(m.department), 'None')
where COALESCE(u.department_dim_id,0)=0 and lower(u.jira_user_name)=lower(trim(m.displayName));

update idw.jira_user_dim u
set department_dim_id=0
where department_dim_id is null;
''')

department_mapping_in_transition = ('''
    update idw.jira_sd_workflow_transition_map tr
    set to_department_dim_id = u.department_dim_id
    from idw.jira_user_dim u
    where to_department_dim_id is null and tr.to_assignee_dim_id=u.jira_user_dim_id;

    update idw.jira_sd_workflow_transition_map tr
    set from_department_dim_id = u.department_dim_id
    from idw.jira_user_dim u
    where from_department_dim_id is null and tr.from_assignee_dim_id=u.jira_user_dim_id;
''')

request_type_dim_insert = ('''
    INSERT INTO idw.jira_request_type_dim(jira_request_type_dim_id,jira_request_type_name)
    SELECT (select COALESCE(max(jira_request_type_dim_id),0)+t.num from idw.jira_request_type_dim),jira_request_type_name
    FROM
    (
        SELECT row_number() over () as num, COALESCE(stage.request_type,'None') as jira_request_type_name
        FROM birepusr.jira_sd_stage stage
        LEFT OUTER JOIN idw.jira_request_type_dim rt
        ON COALESCE(stage.request_type,'None')=rt.jira_request_type_name
        WHERE rt.jira_request_type_dim_id is NULL
        GROUP BY COALESCE(stage.request_type,'None')
    ) as t
''')

fact_table_update = ('''UPDATE idw.jira_sd_fact fact
SET is_active=false,
    date_updated = now()
FROM birepusr.jira_sd_stage stage
WHERE stage.key=fact.jira_key''')

fact_table_insert = ('''INSERT INTO idw.jira_sd_fact(jira_key,account_dim_id,
channel_dim_id,contact_dim_id,status_dim_id,resolution_stauts_dim_id,priority_dim_id,
date_created_dim_id,date_created,is_active,jira_contact_guid_map_status_id,
time_to_close_in_days,time_to_close_in_hours,time_to_close_in_minutes,pending_since_in_days,
jira_request_type_dim_id,assignee_dim_id,department_dim_id)
SELECT
stage.key,cust.customer_dim_id,channel.channel_dim_id,contact.contact_dim_id,stat.jira_status_dim_id,resstat.jira_status_dim_id,
priority.jira_priority_dim_id,createdt.sk_date,now(),true,
CASE WHEN stage.crm_contact IS NULL THEN 1 ELSE NULL END,
DATE_PART('day',stage.resolution_date::timestamp - stage.created::timestamp),
DATE_PART('day',stage.resolution_date::timestamp - stage.created::timestamp) *24 +
DATE_PART('hour',stage.resolution_date::timestamp - stage.created::timestamp) +
CASE WHEN DATE_PART('minute',stage.resolution_date::timestamp - stage.created::timestamp) >=30 then 1 else 0 END,
(DATE_PART('day',stage.resolution_date::timestamp - stage.created::timestamp) *24 +
DATE_PART('hour',stage.resolution_date::timestamp - stage.created::timestamp)) * 60 +
DATE_PART('minute',stage.resolution_date::timestamp - stage.created::timestamp),
CASE WHEN resolution_date IS NULL THEN
DATE_PART('day',now()::timestamp - stage.created::timestamp) ELSE NULL END,
reqtype.jira_request_type_dim_id,
usr.jira_user_dim_id,
usr.department_dim_id
FROM birepusr.jira_sd_stage stage
LEFT JOIN idw.customer_dim cust ON trim(stage.crm_account)=trim(cust.accountid)
LEFT JOIN idw.channel_dim channel ON COALESCE(trim(stage.channel),'None')=trim(channel.imo_name)
LEFT JOIN idw.contact_dim contact ON trim(stage.crm_contact)=trim(contact.contactid)
LEFT JOIN idw.jira_status_dim stat ON COALESCE(trim(stage.status_name),'None')=trim(stat.jira_status)
LEFT JOIN idw.jira_status_dim resstat ON COALESCE(trim(stage.resolution),'None')=trim(resstat.jira_status)
LEFT JOIN idw.jira_priority_dim priority ON COALESCE(trim(stage.priority),'None')=trim(priority.jira_priority)
LEFT JOIN idw.date_dimension createdt ON stage.created::date=createdt.dt
LEFT JOIN idw.jira_request_type_dim reqtype ON COALESCE(trim(stage.request_type),'None')=trim(reqtype.jira_request_type_name)
LEFT JOIN idw.jira_user_dim usr ON COALESCE(trim(stage.assignee),'None')=trim(usr.jira_user_id);''')

fact_table_map_update_date = ('''UPDATE idw.jira_sd_fact fact
SET date_updated_dim_id = updatedt.sk_date,
    date_updated = now()
FROM birepusr.jira_sd_stage stage
LEFT JOIN idw.date_dimension updatedt ON stage.updated::date=updatedt.dt
WHERE stage.key=fact.jira_key AND is_active=true''')

fact_table_map_resolution_date = ('''UPDATE idw.jira_sd_fact fact
SET date_resolved_dim_id = resoldt.sk_date,
    date_updated = now()
FROM birepusr.jira_sd_stage stage
LEFT JOIN idw.date_dimension resoldt ON stage.resolution_date::date=resoldt.dt
WHERE stage.key=fact.jira_key AND is_active=true''')

fact_table_update_test_incidents_as_inactive = ('''
update idw.jira_sd_fact f
set is_active=false
from idw.customer_dim c
WHERE f.account_dim_id=c.customer_dim_id
AND c.name='IMO Test Hospital' and f.is_active=true
''')

fact_table_update_BenE_incidents_as_inactive = ('''
update idw.jira_sd_fact f
set is_active=false
from idw.contact_dim c
WHERE f.contact_dim_id=c.contact_dim_id
AND trim(c.firstname) ='Ben' and trim(c.lastname)='Eckler' and f.is_active=true;
''')

workflow_status_dim_insert = ('''
update idw.jira_status_dim st
set is_workflow_status=sd.flag,
jira_api_id=sd.id
from workflow_status sd
where COALESCE(trim(sd.status),'None')=trim(st.jira_status);
INSERT INTO idw.jira_status_dim(jira_status_dim_id,jira_status,is_workflow_status,jira_api_id)
SELECT (select COALESCE(max(jira_status_dim_id),0)+t.num from idw.jira_status_dim), status_name,flag,id
FROM (
    SELECT row_number() over () as num,COALESCE(sd.status,'None') as status_name,sd.flag,COALESCE(sd.id,0) as id
    FROM workflow_status sd
    LEFT OUTER JOIN idw.jira_status_dim st ON COALESCE(trim(sd.status),'None')=trim(st.jira_status)
    WHERE st.jira_status_dim_id is NULL
) as t;''')


# Create guid mapping temporary table
create_guid_mapping_status = ('''create temp table jira_contact_guid_mapping as
select key_stage
,case
    when Valid_Guid = 'Y' then 1
    when No_Guid = 'Y' and No_email = 'Y' then 2
    when Invalid_Guid='Y' and No_email='Y' then 3
    when No_Guid='Y' and Invalid_Email='Y' then 4
    when Invalid_Guid='Y' and Invalid_Email='Y' then 5
    when No_Guid='Y' and Valid_Email='Y' then 6
    when Invalid_Guid = 'Y' and Valid_Email='Y' then 7
    else 0 end as contact_guid_map_status_id
from
(
SELECT X.*
,case when crm_contact_stage is null or trim(crm_contact_stage) = '' then 'Y' else 'N' end No_Guid
,case when trim(crm_contact_stage) = trim(contactid) then 'Y' else 'N' end Valid_Guid
,case when crm_contact_stage is not null and ((trim(crm_contact_stage) <> trim(contactid)) or (contactid is null or trim(contactid)='')) then 'Y' else 'N' end Invalid_Guid
,case when reporter_email_stage is null or trim(reporter_email_stage) ='' then 'Y' else 'N' end No_email
,case when reporter_email_stage is not null and ((trim(reporter_email_stage) <> trim(emailaddress)) or (emailaddress is null or trim(emailaddress)='')) then 'Y' else 'N' end Invalid_Email
,case when reporter_email_stage is not null and trim(reporter_email_stage) = trim(emailaddress) then 'Y' else 'N' end Valid_Email
FROM
(
select S.key key_stage
,S.reporter_email reporter_email_stage
,S.crm_contact crm_contact_stage
,C.contact_dim_id
,C.contactid
,C.emailaddress
from birepusr.jira_sd_stage S
left outer join idw.contact_dim C
on trim(S.crm_contact)=trim(C.contactid) or trim(S.reporter_email)=trim(C.emailaddress)
) X
) Y''')

# updating contact guid mapping status id
update_contact_guid_mapping = ('''UPDATE idw.jira_sd_fact as F
set jira_contact_guid_map_status_id=T.contact_guid_map_status_id
from jira_contact_guid_mapping T
where trim(F.jira_key)=trim(T.key_stage) and F.is_active=true''')

workflow_transition_time = ('''UPDATE idw.jira_sd_workflow_transition_map as F
set time_spent_in_status_in_days=date_part('day',F.status_change_date-lag_time),
    time_spent_in_status_in_hours=days_diff * 24 + DATE_PART('hour', F.status_change_date - lag_time )+round ,
    time_spent_in_status_in_minutes=days_diff*24*60 + hours_diff * 60 + DATE_PART('minute', F.status_change_date - lag_time )
from
    (select *,lag(status_change_date) over (PARTITION BY jira_key order by status_change_date) as lag_time,
     date_part('day',status_change_date-lag(status_change_date) over (PARTITION BY jira_key order by status_change_date)) as days_diff,
     date_part('hour',status_change_date-lag(status_change_date) over (PARTITION BY jira_key order by status_change_date)) as hours_diff,
     case when date_part('minute',status_change_date-lag(status_change_date)
                         over (PARTITION BY jira_key order by status_change_date))>=30 then 1 else 0 end as round
    from idw.jira_sd_workflow_transition_map where from_status_dim_id is not null and from_status_dim_id > 0)  T
where trim(F.jira_key)=trim(T.jira_key) and F.status_change_date=T.status_change_date
and exists (select 1 from birepusr.jira_sd_stage s where s.key=f.jira_key);''')

workflow_assignee_transition_time = ('''UPDATE idw.jira_sd_workflow_transition_map as f
set time_spent_by_assignee_in_days=date_part('day',F.status_change_date-lag_time),
    time_spent_by_assignee_in_hours=days_diff * 24 + DATE_PART('hour', F.status_change_date - lag_time )+round ,
    time_spent_by_assignee_in_minutes=days_diff*24*60 + hours_diff * 60 + DATE_PART('minute', F.status_change_date - lag_time )
from
    (select *,lag(status_change_date) over (PARTITION BY jira_key order by status_change_date) as lag_time,
     date_part('day',status_change_date-lag(status_change_date) over (PARTITION BY jira_key order by status_change_date)) as days_diff,
     date_part('hour',status_change_date-lag(status_change_date) over (PARTITION BY jira_key order by status_change_date)) as hours_diff,
     case when date_part('minute',status_change_date-lag(status_change_date)
                         over (PARTITION BY jira_key order by status_change_date))>=30 then 1 else 0 end as round
    from idw.jira_sd_workflow_transition_map where from_assignee_dim_id is not null)  T
where trim(F.jira_key)=trim(T.jira_key) and F.status_change_date=T.status_change_date
and exists (select 1 from birepusr.jira_sd_stage s where s.key=f.jira_key);''')


update_assignee_transition_time = ('''
UPDATE idw.jira_sd_workflow_transition_map as f
set    time_spent_by_assignee_in_days=date_part('day', status_change_date -issue_created_date),
    time_spent_by_assignee_in_hours=date_part('day',status_change_date - issue_created_date)*24
                                +date_part('hour',status_change_date - issue_created_date)
                                +case when date_part('minute',status_change_date - issue_created_date)>=30 then 1 else 0 end,
    time_spent_by_assignee_in_minutes=date_part('day',status_change_date - issue_created_date)*24*60
                                +date_part('hour',status_change_date - issue_created_date)*60
                                +date_part('minute',status_change_date - issue_created_date)

where   from_assignee_dim_id is not null
        and time_spent_by_assignee_in_days is null
        and time_spent_by_assignee_in_hours is null
        and time_spent_by_assignee_in_minutes is null
        and exists (select 1 from birepusr.jira_sd_stage s where s.key=f.jira_key);''')

update_workflow_transition_time = ('''
UPDATE idw.jira_sd_workflow_transition_map as f
set    time_spent_in_status_in_days=date_part('day', status_change_date -issue_created_date),
    time_spent_in_status_in_hours=date_part('day',status_change_date - issue_created_date)*24
                                +date_part('hour',status_change_date - issue_created_date)
                                +case when date_part('minute',status_change_date - issue_created_date)>=30 then 1 else 0 end,
    time_spent_in_status_in_minutes=date_part('day',status_change_date - issue_created_date)*24*60
                                +date_part('hour',status_change_date - issue_created_date)*60
                                +date_part('minute',status_change_date - issue_created_date)

where   from_status_dim_id is not null
        and time_spent_in_status_in_days is null
        and time_spent_in_status_in_hours is null
        and time_spent_in_status_in_minutes is null
        and exists (select 1 from birepusr.jira_sd_stage s where s.key=f.jira_key);''')

update_status_change_date_dim_id = ('''UPDATE idw.jira_sd_workflow_transition_map as f
set status_change_date_dim_id=sk_date
from  idw.date_dimension createdt
where status_change_date::date=dt and status_change_date is not null
and exists (select 1 from birepusr.jira_sd_stage s where s.key=f.jira_key);''')

contact_dim_select = ('''SELECT * FROM idw.contact_dim''')
channel_dim_select = ('''SELECT * FROM idw.contact_dim''')
account_dim_select = ('''SELECT * FROM idw.customer_dim''')
status_dim_select = ('''SELECT * FROM idw.status_dim''')
priority_dim_select = ('''SELECT * FROM idw.priority_dim''')
date_dim_select = ('''SELECT * FROM idw.date_dimension''')

load_dim_case_origin = ('''INSERT INTO idw.jira_origin_dim(jira_origin_dim_id,jira_origin_name,source_case_origin_id)
SELECT (select COALESCE(max(jira_origin_dim_id),0)+t.num from idw.jira_origin_dim),jira_origin_name,source_case_origin_id
    FROM
    (
        SELECT row_number() over () as num, COALESCE(cs.caseorigincodename,'None') as jira_origin_name,
        COALESCE(caseorigincode,0) as source_case_origin_id
        FROM mscrm.incident_entity  cs
        LEFT OUTER JOIN idw.jira_origin_dim od
        ON COALESCE(cs.caseorigincodename,'None')=od.jira_origin_name
        WHERE od.jira_origin_name is NULL
        GROUP BY COALESCE(cs.caseorigincodename,'None'),COALESCE(caseorigincode,0)
    ) as t''')

update_case_origin_fact = ('''UPDATE idw.jira_sd_fact fact
SET jira_origin_dim_id = od.jira_origin_dim_id,
    date_updated = now()
FROM birepusr.jira_sd_stage cs
left join mscrm.incident_entity ie on cs.jira_case=ie.ticketnumber
inner join idw.jira_origin_dim od ON COALESCE(ie.caseorigincodename,'None')= od.jira_origin_name
WHERE fact.jira_origin_dim_id is null and cs.key=fact.jira_key AND fact.is_active=true;''')

insert_jira_workflow_map = ('''insert into idw.jira_sd_workflow_transition_map(jira_key,to_status_dim_id)
select jira_key,jira_status_dim_id
from idw.jira_sd_fact f inner join idw.jira_status_dim
on is_active=true and is_workflow_status=true
where not exists (select jira_key from idw.jira_sd_workflow_transition_map map where map.jira_key=f.jira_key and to_status_dim_id=jira_status_dim_id);''')

getActiveandOpenJiraTickets = ('''select distinct key from birepusr.jira_sd_stage;''')

getValidWorkflowStatus = ('''select jira_status_dim_id,jira_status,jira_api_id from idw.jira_status_dim
                            where is_workflow_status=true;''')

getValidWorkflowAssignee = ('''select jira_user_dim_id,jira_user_id,jira_user_name from idw.jira_user_dim;''')

upsertTransitionLog = ('''update idw.jira_sd_workflow_transition_map
                        set from_status_dim_id=NULLIF({2},0),
                            status_change_date='{3}',
                            issue_created_date='{4}',
                            to_assignee_dim_id=NULLIF({5},0),
                            from_assignee_dim_id=NULLIF({6},0)
                        where jira_key='{0}' and COALESCE(to_status_dim_id,0)={1}
                        and (from_status_dim_id is null or COALESCE(from_status_dim_id,0) = {2})
                        and to_assignee_dim_id is null;
                        insert into idw.jira_sd_workflow_transition_map
                        (jira_key,to_status_dim_id,from_status_dim_id,status_change_date,issue_created_date,to_assignee_dim_id,from_assignee_dim_id)
                        select '{0}', NULLIF({1},0),NULLIF({2},0),'{3}','{4}',NULLIF({5},0),NULLIF({6},0)
                        where not exists (select  1 from idw.jira_sd_workflow_transition_map
                        where jira_key= '{0}'
                        and COALESCE(to_status_dim_id,0)={1} and COALESCE(from_status_dim_id,0)={2} and status_change_date='{3}' and COALESCE(to_assignee_dim_id,0) = {5} and COALESCE(from_assignee_dim_id,0) ={6});
                        ''')

update_is_current_status_flag = ('''update idw.jira_sd_workflow_transition_map m
set is_current_state= CASE WHEN maxdate=status_change_date THEN true ELSE false END
from (
select distinct jira_key , max(status_change_date) over (partition by jira_key)  as maxdate
from idw.jira_sd_workflow_transition_map
where from_status_dim_id is not null) a
where a.jira_key=m.jira_key;''')

update_is_current_assignee_flag = ('''update idw.jira_sd_workflow_transition_map m
set is_current_assignee= CASE WHEN maxdate=status_change_date THEN true ELSE false END
from (
select distinct jira_key , max(status_change_date) over (partition by jira_key)  as maxdate
from idw.jira_sd_workflow_transition_map
where from_assignee_dim_id is not null and to_assignee_dim_id is not null) a
where a.jira_key=m.jira_key;''')

upsert_assigne = ('''update idw.jira_user_dim ud
set jira_user_name=a.user_name,
jira_project='SERVICDESK',
email_id=a.email_id,
is_active=a.active
from jira_assignee a
where COALESCE(trim(a.user_id),'None')=trim(ud.jira_user_id) and
(jira_user_name!=a.user_name or ud.email_id != a.email_id or ud.is_active !=a.active);
INSERT INTO idw.jira_user_dim(jira_user_dim_id,jira_user_id,jira_user_name,jira_project,email_id,is_active)
SELECT (select COALESCE(max(jira_user_dim_id),0)+t.num from idw.jira_user_dim), user_id,user_name,'SERVICDESK',email_id,active
FROM (
    SELECT row_number() over () as num,COALESCE(u.user_id,'None') as user_id,u.user_name,u.email_id,u.active
    FROM jira_assignee u
    LEFT OUTER JOIN idw.jira_user_dim us ON COALESCE(trim(u.user_id),'None')=trim(us.jira_user_id)
    WHERE us.jira_user_dim_id is NULL
) as t;''')

insert_missing_assignee_from_api = ('''INSERT INTO idw.jira_user_dim(jira_user_dim_id,jira_user_id,jira_user_name,jira_project,email_id,is_active)
SELECT (select
        COALESCE(max(jira_user_dim_id),0)+t.num from idw.jira_user_dim), user_id,user_name,
        'SERVICDESK','None',is_active
FROM (
    SELECT row_number() over () as num, user_id,trim(replace(user_name,'[X]','')) as user_name,
    case when user_name like '%[X]' then false else true end as is_active from (
    select distinct COALESCE(stage.assignee,'None') as user_id,
     stage.assignee_name as user_name
    from birepusr.jira_sd_stage stage
    left outer join idw.jira_user_dim u
    on COALESCE(trim(stage.assignee),'None')=trim(u.jira_user_id)
    where u.jira_user_dim_id is null)a
) as t;''')

update_resolution_date_intransition_map = ('''
update idw.jira_sd_workflow_transition_map m
set issue_resolved_date = s.resolution_date::timestamp
from birepusr.jira_sd_stage s where s.key=m.jira_key;''')

update_department_id_in_fact = ('''
    update idw.jira_sd_fact fact
    set department_dim_id = u.department_dim_id
    from idw.jira_user_dim u
    where fact.department_dim_id is null and fact.assignee_dim_id=u.jira_user_dim_id;
''')

insert_unchanged_assignee_in_transition_map = ('''insert into idw.jira_sd_workflow_transition_map(jira_key,to_assignee_dim_id,status_change_date,issue_created_date,issue_resolved_date,is_current_assignee)
select jira_key,assignee_dim_id,created::timestamp,created::timestamp,resolution_date::timestamp,true from idw.jira_sd_fact f
inner join birepusr.jira_sd_stage s on f.is_active=true and s.key=f.jira_key
where jira_key not in (
select jira_key from idw.jira_sd_workflow_transition_map where status_change_date is not null and is_current_assignee=true);''')

insert_new_product = ('''INSERT INTO idw.jira_product_dim(jira_product_dim_id, jira_product_name, jira_product_api_id)
    SELECT (select COALESCE(max(jira_product_dim_id),0)+1 from idw.jira_product_dim),'{0}','{1}';''')

update_product_name = ('''update idw.jira_product_dim set jira_product_name='{0}' where jira_product_api_id='{1}';''')

insert_jira_product_map = ('''insert into idw.jira_product_map(jira_key, jira_product_dim_id)
select t.key, p.jira_product_dim_id
from   birepusr.jira_sd_stage  t
join   jsonb_array_elements(
    replace(replace(replace(replace(replace(replace(t.product_name,'<JIRA CustomFieldOption:','{'),'>','}'),'value','"value"'),'id','"id"'),'=',':'),'\''','"')::jsonb) obj(val) ON '1'='1'
                                       Left join idw.jira_product_dim p on obj.val->>'id' = p.jira_product_api_id
                                       where not exists (select 1 from idw.jira_product_map m where m.jira_key=t.key
                                       and m.jira_product_dim_id=p.jira_product_dim_id)order by 1;''')

update_time_to_close_in_jira_product_map = ('''update idw.jira_product_map m
set time_to_close_in_days=f.time_to_close_in_days,
time_to_close_in_hours=f.time_to_close_in_hours,
time_to_close_in_minutes=f.time_to_close_in_minutes
from birepusr.jira_sd_stage s inner join
idw.jira_sd_fact f on s.key=f.jira_key
where s.key=m.jira_key and f.is_active=true;''')


class SqlQuery:
    def __init__(self, dimension_name, select_query):
        self.dimension_name = dimension_name
        self.select_query = select_query


account_dim = SqlQuery('account', account_dim_select)
contact_dim = SqlQuery('contact', contact_dim_select)
channel_dim = SqlQuery('channel', channel_dim_select)
status_dim = SqlQuery('status', status_dim_select)
priority_dim = SqlQuery('priority', priority_dim_select)
date_dim = SqlQuery('date', date_dim_select)

dimension_select_queries = [account_dim, contact_dim,
                            channel_dim, status_dim, priority_dim, date_dim]
