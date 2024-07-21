from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    @apply_defaults
    def __init__(self,
                  redshift_conn_id="",
                  target_table="",
                  sql_query="",
                 *args, **kwargs):
          
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.target_table = target_table
        self.insert_sql_query = 'INSERT INTO {} {}'.format(target_table, sql_query)

    def execute(self, context):

      self.log.info('Running LoadFactOperator with query {}'.format(self.insert_sql_query))
      redshift_hook = PostgresHook(self.redshift_conn_id)
      redshift_hook.run("""
          BEGIN;
          TRUNCATE TABLE {};
          {};
          COMMIT;
        """.format(self.target_table, self.insert_sql_query))
