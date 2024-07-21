from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 target_table="",
                 sql_query = "",
                 append_key = "",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.target_table = target_table
        self.insert_sql_query = 'INSERT INTO {} {}'.format(target_table, sql_query)
        self.append_key = append_key

    def execute(self, context):
        self.log.info('Running LoadDimensionOperator with query {}'.format(self.insert_sql_query))

        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # If append_key is not specified, then it is an insert   
        if len(self.append_key) == 0:
          redshift_hook.run("""
            BEGIN;
            TRUNCATE TABLE {};
            {};
            COMMIT;
          """.format(self.target_table, self.insert_sql_query))

         # If append_key is specified, then it is an update. Use the append_key (the primary key) in the table to determine
         # if the record already exists.
        else:
          if self.target_table == 'users':
            query_prefix = 'AND'
          else:
            query_prefix = 'WHERE'
          redshift_hook.run("""
            {} {} NOT EXISTS (SELECT {} FROM {})
        """.format(self.insert_sql_query, query_prefix, self.append_key, self.target_table))
          
