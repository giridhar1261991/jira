from common.data_extractor_dao import executeQuery, executeQueryAndReturnDF
from servicedesk.db_queries import insert_new_product, update_product_name, insert_jira_product_map
from common.audit_logger import createSubTask, updateSubTask, insertErrorLog, getMaintaskId
from common.credentials import jira_api
from servicedesk.jira_api_dao import Jira


def upsertJiraProduct():
    """
    Execute the database insert and update query to insert
    new product or update existing ones based on response form jira rest api

    Args:
    No Arguments
    Returns:
    No return variable
    """
    subtaskid = createSubTask("upsert service desk products in jira_product map table", getMaintaskId())
    try:
        MyJira = Jira(**jira_api)
        df_products = MyJira.getServiceDeskProducts()
        df_product_dim = executeQueryAndReturnDF("select jira_product_name as product_name, jira_product_api_id as api_id from idw.jira_product_dim")
        comparison_df = df_products.merge(df_product_dim, indicator=True, on=['api_id'], how='outer')
        comparison_df[comparison_df['_merge'] == 'left_only'].apply(lambda product: executeQuery(insert_new_product.format(product['product_name_x'], product['api_id'])), axis=1)
        comparison_df = comparison_df[comparison_df['_merge'] == 'both'].query('product_name_x != product_name_y')
        comparison_df.apply(lambda product: executeQuery(update_product_name.format(product['product_name_x'], product['api_id'])), axis=1)
        updateSubTask(subtaskid, "SUCCESS")
    except (Exception) as error:
        insertErrorLog(subtaskid, error)


def persistJiraProductMap():
    """
    get list of products tagged to a jira key and persist in jira_product map table
    so that user can generate time_to_close metric for each product

    Args:
    No Arguments
    Returns:
    No return variable
    """
    subtaskid = createSubTask("calculate and  update time to close measures in jira_product map table", getMaintaskId())
    returnVal = executeQuery(insert_jira_product_map)
    updateSubTask(subtaskid, "SUCCESS") if returnVal == 'SUCCESS' else insertErrorLog(subtaskid, returnVal)
