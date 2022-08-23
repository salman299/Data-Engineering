from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table = "",
                 sql_statement = "",
                 truncate_insert = True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.sql_statement = sql_statement
        self.table=table
        self.truncate_insert=truncate_insert

    def execute(self, context):
        """
        Insert data in a dimension table
        Parameters:
            redshift_conn_id: (str) redshift credentials to connect to a redshift database
            table: (str) name of a table
            sql_statement: (str) create sql query
            truncate_insert: (bool) If true, delete entries in table and than insert data
        """
        self.log.info(f"Loading data into the {self.table} table")
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.truncate_insert:
            self.log.info(f"Deleting rows in the {self.table} table")
            redshift.run(f"DELETE FROM {self.table}")
        
        sql = f"INSERT INTO {self.table} " + self.sql_statement
        redshift.run(sql)
        records = redshift.get_records(f"select count(*) from {self.table};")
        self.log.info(f"Found {len(records)} in {self.table}")
        self.log.info(f"Successfully loaded data into {self.table}")
