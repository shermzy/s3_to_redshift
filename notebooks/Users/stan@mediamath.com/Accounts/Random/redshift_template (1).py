# Databricks notebook source
import pandas_redshift as pr
import pandas as pd

# COMMAND ----------

pr.connect_to_redshift(dbname = 'pso',
                        host = '',
                        port =5439,
                        user = '',
                        password = '')

# COMMAND ----------



# COMMAND ----------

 sql="""copy {}.{} from '{}'\
        credentials \
        'aws_access_key_id={};aws_secret_access_key={}' \
         timeformat 'YYYY-MM-DD HH:MI:SS'
        DELIMITER '\t' lzop;commit;"""\
        .format('public', 'mm_impressions_sh_test', 's3://mm-prod-platform-impressions/data/organization_id=100977/impression_date=2018-07-25/', '{{access_key}}', '{{secret}}')

# COMMAND ----------

from sqlalchemy import create_engine
conn = create_engine('postgresql://user:pw@host:5439/pso')

try:
  conn.execute(sql)
  print("Copy Command executed successfully")
except Exception as e:
    print("type error: " + str(e))
# conn.close() 

# COMMAND ----------

