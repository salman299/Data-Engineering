from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql_statement="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_statement = sql_statement
        self.table = table

    def execute(self, context):
        """
        Insert data in a fact table
        Parameters:
            redshift_conn_id: (str) redshift credentials to connect to a redshift database
            table: (str) name of a table
            sql_statement: (str) create sql query
        """
        self.log.info(f"Loading data into the {self.table} table")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        sql = f"INSERT INTO {self.table} " + self.sql_statement
        redshift.run(sql)
        records = redshift.get_records(f"select count(*) from {self.table};")
        self.log.info(f"Found {len(records)} in {self.table}")
        self.log.info(f"Successfully loaded data into {self.table}")
