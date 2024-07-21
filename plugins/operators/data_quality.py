from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging

class DataQualityOperator(BaseOperator):
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tests="",
                 *args, **kwargs):
        super (DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tests = tests

    def execute(self, context):
        self.log.info("Running DataQualityOperator")
        redshift_hook = PostgresHook(self.redshift_conn_id)
        for test in self.tests:
          records = redshift_hook.get_records(test[0])
          if not test[1](len(records)) or not test[1](len(records[0])):   
              raise ValueError(f"Data quality check failed. {test[0]} returned no results")
          num_records = records[0][0]
          if not test[1](num_records):
              raise ValueError(f"Data quality check failed. {test[0]} returned 0 rows")
          logging.info(f"Data quality query {test[0]} succeeded")