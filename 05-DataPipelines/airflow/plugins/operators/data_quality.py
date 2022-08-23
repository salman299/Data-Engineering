from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'
    CUSTOM_VALIDATION = """
    Custom Validation - {}
    SQL:
        {}
    EXPECTED VALUE: 
        {}
    ACTUAL VALUE:
        {}
    """
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables = {},
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables


    def execute(self, context):
        """
        Checks the validations for each table.
        Parameter:
            redshift_conn_id: (str) redshift credentials to connect to a redshift database
            tables: (dict)
                Example:
                    {
                        'table1': {basic_validation=True, validations={'sql': 'expected_value'}},
                        'table2': {basic_validation=False, validations={}},
                    }
                On basic_validation=True, Table should have rows to pass basic validation
                Add other validations in validations field
                    Format <SQL_STATEMENT>: <EXPECTED VALUE>
                    Example:
                        validations = {"SELECT COUNT(*) FROM table1": 14896}

        """
        redshif = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        for table in self.tables:
            self.log.info(f"Start checking validations for': {table}")
            
            #check basic validations
            if 'basic_validations' in self.tables[table] and self.tables[table]['basic_validations']:
                self.log.info(f"Checking Basic validations for: {table}")
                records = redshif.get_records(f"SELECT COUNT(*) FROM {table};")
                if len(records) < 1 or len(records[0]) < 1 or records[0][0] < 1:
                    raise ValueError(f"Basic Validation (Table contain Rows) - Failed : {table}")
                self.log.info(f"Basic Validation (Table Contain Rows) - Passed : {table}")
            
            #check extra validations
            if 'validations' in self.tables[table] and self.tables[table]['validations']:
                self.log.info(f"Checking extra validations for: {table}")
                validations = self.tables[table]['validations']
                for sql, expected_value in validations.items():
                    records = redshif.get_records(sql)
                    if records[0][0] != expected_value:
                        raise ValueError(
                            DataQualityOperator.CUSTOM_VALIDATION.format(
                                'FAILD', sql, expected_value, records[0][0]
                            )
                        )
                    self.log.info(
                        DataQualityOperator.CUSTOM_VALIDATION.format(
                            'PASSED', sql, expected_value, records[0][0]
                        )
                    )
        
        self.log.info("Data Quality Validations are passed on all tables.")