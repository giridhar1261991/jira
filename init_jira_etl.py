import cog.init_cogs_etl as cog
#import servicedesk.jira_data_generator as sd


def handler(event, context):
    #sd.handler(event, context)
    cog.handler(event, context)
    #cog.init_codgs_etl()


if __name__ == "__main__":
    handler(None, None)
