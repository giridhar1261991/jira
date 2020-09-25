from common.data_extractor_dao import executeQuery

queries = ('''
insert into idw.jira_product_dim (jira_product_dim_id, jira_product_name,jira_product_api_id )
select (select max(jira_product_dim_id)+1 from idw.jira_product_dim), 'Release Manager', 0
where not exists (select 'x' from idw.jira_product_dim where jira_product_name='Release Manager');

insert into idw.jira_product_dim (jira_product_dim_id, jira_product_name,jira_product_api_id )
select (select max(jira_product_dim_id)+1 from idw.jira_product_dim), 'ECW', 0
where not exists (select 'x' from idw.jira_product_dim where jira_product_name='ECW');
''')


def executeStaticQueries():
    executeQuery(queries)
