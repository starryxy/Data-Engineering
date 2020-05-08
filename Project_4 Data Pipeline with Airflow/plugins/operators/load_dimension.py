from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 # Define operators params (with defaults)
                 redshift_conn_id="",
                 table="",
                 sql="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from dim table")
        # DELETE FROM {} is slower than TRUNCATE
        # TRUNCATE can ignore delete triggers, can perform an immediate commit, can keep storage allocated for the table
        redshift_hook.run("TRUNCATE TABLE {}".format(self.table))

        self.log.info("Loading data into dim table")
        redshift_hook.run(self.sql)
