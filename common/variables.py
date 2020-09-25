
import os
import boto3
import json

IDW_CONFIG_PREFIX = os.getenv('IDW_CONFIG_PREFIX')
if not IDW_CONFIG_PREFIX:
    raise Exception('The IDW_CONFIG_PREFIX environment variable is not set. Set this value to continue.')


if not IDW_CONFIG_PREFIX == "idwjira/tfs":
    ssm = boto3.client('ssm')

    # Note: run this code here rather than within my_handler so that the config
    # settings are only gotten on startup and not with every execution
    app_config_parameter_name = f'{IDW_CONFIG_PREFIX}/config'
    response = ssm.get_parameter(Name=app_config_parameter_name, WithDecryption=True)
    app_config_json = response['Parameter']['Value']
else:
    app_config_json = os.getenv('IDW_CONFIG')

app_config = json.loads(app_config_json)

datawarehouse_name = app_config['datawarehouse_name']
password = app_config['dbuser_password']
jira_api_usr = app_config['jira_username']
jira_api_password = app_config['jira_password']
datawarehouse_host = app_config['db_host']

user_name = 'svc_etl_user'
datawarehouse_schema_name = 'idw'
staging_schema_name = 'birepusr'
jira_url = 'https://jira.imo-online.com'
jira_project = 'SERVICDESK'
