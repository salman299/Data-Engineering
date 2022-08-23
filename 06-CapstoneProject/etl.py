import os
import datetime as dt

from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import udf, monotonically_increasing_id
from pyspark.sql import functions as F
from pyspark.sql.window import Window as W


CLEAN_TABLE_STATS = """
Table: {}
Entries before processing: {}
Entries after processing: {}
Dropped Columns: {}
"""

def cast_type(df, cols):
    """
    Cast the columns in given format
    Arguments:
        df: (Dataframe) spark dataframe object
        cols: (Dict) key as a column name and value as a cast type
    """
    for col, data_type in cols.items():
        if col in df.columns:
            df = df.withColumn(col, df[col].cast(data_type))
    return df
    

def clean_immigration_dataset(df):
    """
    Used to clean the immigration dataset.
        - Drops columns with 90% null values
        - Drops columns that are not used in the analysis
        - Cast columns in valid format
    Arguments:
        df: (Spark DataFrame) Immigration Dataset
    Returns:
        df: (Spark DataFrame) Clean Immigration Dataset
    """
    initial_count = df.count()
    initial_cols = df.columns
    
    # Drop columns with high null values
    high_null_col = ["visapost", "occup", "entdepu", "insnum"]
    unuseful_cols = ["i94yr", "i94mon", "fltno", "i94bir", "airline", "fltno",
                     "count", "entdepa", "entdepd", "matflag", "dtaddto", "admnum", "dtadfile"]

    df = df.drop(*high_null_col)
    df = df.drop(*unuseful_cols)
    
    # Drop all rows with null values
    df = df.dropna(how='all')
    
    int_cols = ['cicid', 'i94cit', 'i94res','i94mode', 'i94visa', 'biryear', 'depdate', 'arrdate']
    
    df = cast_type(df, dict(zip(int_cols, len(int_cols)*[IntegerType()])))
    
    get_datetime = udf(lambda x: (dt.datetime(1960, 1, 1).date() + dt.timedelta(x)).isoformat() if x else None)
    
    date_cols = ['arrdate', 'depdate']
    for col in date_cols:
        df = df.withColumn(col, get_datetime(df[col]))
    
    final_count = df.count()
    final_cols = df.columns
    droped_cols = set(initial_cols) - set(final_cols)
    
    print(CLEAN_TABLE_STATS.format('Immigration', initial_count, final_count, droped_cols))

    return df


def clean_demographics_dataset(df):
    """
    Used to clean the demographics datasets
        - Drop duplicates rows 
        - Drop entries with null values
        - Change City to uppercase
    Arguments:
        df: (Spark DataFrame) Demographic Dataset
    Returns:
        df: (Spark DataFrame) Clean Demographic Dataset
    """
    
    initial_count = df.count()
    
    df = df.dropDuplicates(subset=['State Code', 'City','Race'])
    
    df = df.dropna(how='all')
    
    strip_value = udf(lambda x: x.strip().upper())
    df = df.withColumn("City", strip_value(df.City))
    
    final_count = df.count()
    
    print(CLEAN_TABLE_STATS.format('Demographics', initial_count, final_count, []))
    
    return df
    

def clean_temperature_dataset(df):
    """
    Used to clean the immigration dataset.
        - Drop values with AverageTemperature is null
        - Update values of Country to uppercase
    """
    initial_count = df.count()
    
    # Remove the rows with AverageTemperature = None
    df = df.dropna(subset=['AverageTemperature'])
    
    df = df.drop_duplicates(subset=['dt', 'City', 'Country'])
    
    country_value = udf(lambda x: x.strip().upper())
    df = df.withColumn('Country', country_value(df.Country))
    
    final_count = df.count()
    
    print(CLEAN_TABLE_STATS.format('Temperature', initial_count, final_count, []))
    
    return df


def process_demographics_dataset(df, output_path=None, filename='demographics_dim'):
    """
    Process the Demographics data and store the dataset in spark format
    Arguments:
        df: (DataFrame) demographic dataset
        output_path: (str) location to store results. Don't save the results if value is None
        filename: (str) name of the output file
    Returns:
        df: (DataFrame) demographics dimension table
    """
    agg = {
        "Median Age": "first",
        "Male Population": "sum",
        "Female Population": "sum",
        "Total Population": "sum",
        "Number of Veterans": "sum",
        "Foreign-born": "sum",
        "Average Household Size": "avg"
    }

    agg_df = df.groupby(["City", "State", "State Code"]).agg(agg)

    # Pivot Table to transform values of the column Race to different columns
    piv_df = df.groupBy(["City", "State", "State Code"]).pivot("Race").sum("Count")

    df = agg_df.join(other=piv_df, on=["City", "State", "State Code"], how="inner")\
        .withColumnRenamed('State Code', 'StateCode')\
        .withColumnRenamed('sum(Total Population)', 'TotalPopulation')\
        .withColumnRenamed('sum(Female Population)', 'FemalePopulation')\
        .withColumnRenamed('sum(Male Population)', 'MalePopulation')\
        .withColumnRenamed('first(Median Age)', 'MedianAge')\
        .withColumnRenamed('sum(Number of Veterans)', 'NumberVeterans')\
        .withColumnRenamed('sum(Foreign-born)', 'ForeignBorn')\
        .withColumnRenamed('avg(Average Household Size)', 'AverageHouseholdSize')\
        .withColumnRenamed('Hispanic or Latino', 'HispanicOrLatino')\
        .withColumnRenamed('Black or African-American', 'BlackOrAfricanAmerican')\
        .withColumnRenamed('American Indian and Alaska Native', 'AmericanIndianAndAlaskaNative')
    
    df = df.withColumn('id', monotonically_increasing_id())
    df = df.withColumn("id", F.row_number().over(W.orderBy("id")))
    
    if output_path:
        df.write.parquet(os.path.join(output_path, filename), mode="overwrite")
    
    return df
    

def process_temperature_dataset(df, output_path=None, filename='temperature'):
    """
    Process the temperature dataset and cluster the data based on the Country
    Arguments:
        df: (DataFrame) temperature dataset
        output_path: (str) location to store results. Don't save the results if value is None
        filename: (str) name of the output file
    Returns:
        df: (DataFrame) updated temperature dataset
    """
    agg = {
        "AverageTemperature": "avg",
        "Latitude": "first",
        "Longitude": "first"
    }
    
    df = df.groupby(["Country"]).agg(agg)\
    .withColumnRenamed('avg(AverageTemperature)', 'Temperature')\
    .withColumnRenamed('first(Latitude)', 'Latitude')\
    .withColumnRenamed('first(Longitude)', 'Longitude')
    
    
    if output_path:
        df.write.parquet(os.path.join(output_path, filename), mode="overwrite")
    
    return df

def process_visa_table(df, output_path=None, filename='visatype_dim'):
    """
    Create a visa dimension table from immigration dataset
    Arguments:
        df: (DataFrame) immigration dataset
    Returns:
        df_visa: (DataFrame) visa dimension table
    """
    df_visa = df.select(['visatype']).distinct()
    df_visa = df_visa.withColumn('id', monotonically_increasing_id())
    df_visa = df_visa.withColumn("id", F.row_number().over(W.orderBy("id")))
    
    if output_path:
        df_visa.write.parquet(os.path.join(output_path, filename), mode="overwrite")
    
    return df_visa

def process_date_table(df, output_path=None, filename='date_dim'):
    """
    Create a date dimension table from immigration dataset
    Arguments:
        df: (DataFrame) immigration dataset
    Returns:
        dates: (DataFrame) date dimension table
    """
    arr_dates = df.select(['arrdate']).distinct().withColumnRenamed('arrdate','date')
    dep_dates = df.select(['depdate']).distinct().withColumnRenamed('depdate','date')
    dates = arr_dates.union(dep_dates).distinct().dropna(how="all")
    
    dates = dates.withColumn('day', F.dayofmonth('date'))\
        .withColumn('week', F.weekofyear('date'))\
        .withColumn('month', F.month('date'))\
        .withColumn('year', F.year('date'))\
        .withColumn('weekday', F.dayofweek('date'))
    
    if output_path:
        dates.write.parquet(os.path.join(output_path, filename), mode="overwrite")
    
    return dates

def process_country_dataset(df_countries, df_temperature, output_path=None, filename='country_dim'):
    """
    Combine the country dataset with temperature dataset
    Arguments:
        df_countries: (DataFrame) country dataset
        df_temperature: (DataFrame) temperature dataset
    Returns:
        countries: (DataFrame) country dimension table
    """
    df_temperature = df_temperature.withColumnRenamed('Country', 'temp_country')
    countries = df_countries.join(df_temperature, df_countries.country == df_temperature.temp_country, how="left")
    countries = countries.drop('temp_country')
    
    if output_path:
        countries.write.parquet(os.path.join(output_path, filename), mode="overwrite")
    
    return countries

def process_immigration_dataset(df, df_visa, output_path=None, filename='immigration_fact'):
    """
    Creates a immigration fact table
    Arguments:
        df: (DataFrame) immigration dataset
        df_visa: (DataFrame) visatype dataset
    Returns:
        df: (DataFrame) immigration fact table
    """
    df = df.withColumnRenamed('cicid', 'id') \
        .withColumnRenamed('i94cit', 'country_birth_code') \
        .withColumnRenamed('i94res', 'country_residence_code') \
        .withColumnRenamed('i94port', 'arrival_port_code') \
        .withColumnRenamed('i94addr', 'state_code')
    
    df.createOrReplaceTempView("immigration_view")
    df_visa.createOrReplaceTempView("visa_view")
    
    df = spark.sql(
        """
        SELECT 
            immigration_view.*, 
            visa_view.id as visatype_id
        FROM immigration_view
        LEFT JOIN visa_view ON visa_view.visatype=immigration_view.visatype
        """
    )
    df = df.drop('visatype')
    
    if output_path:
        df.write.parquet(os.path.join(output_path, filename), mode="overwrite")
    
    return df

def get_spark_session():
    """
    Returns Spark Session Object
    """
    spark = SparkSession.builder.\
    config("spark.jars.repositories", "https://repos.spark-packages.org/").\
    config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11").\
    enableHiveSupport().getOrCreate()
    return spark

def data_quality_checks(df, table_name, unique_cols=[]):
    """
    Apply data quality checks on tables
        1. Non Zero Rows Data Quality Check
        2. Data should not contain null rows
        3. Unique key should not be duplicated in data
    """
    df_count = df.count()
    
    if df_count:
        print(f'{table_name} : Non Zero Rows - Data Quality Check passed with {df_count} rows')
    else:
        print(f'{table_name} : Non Zero Rows - Data Quality Check failed with {df_count} rows')
        return -1
    
    new_rows = df.dropna(how='all').count()
    null_rows = df_count - new_rows
    
    if not null_rows:
        print(f'{table_name} : No Null Rows - Data Quality Check Passed with {null_rows} null rows')
    else:
        print(f'{table_name} : No Null Rows - Data Quality Check failed with {null_rows} null rows')
        return -1
    
    if unique_cols:
        unique_rows_count = df.dropna(subset=unique_cols).count()
        if unique_rows_count==df_count:
            print(f'{table_name} : Unique Col - Data Quality Check Passed with {unique_cols}')
        else:
            print(f'{table_name} : Unique Col - Data Quality Check Failed with {unique_cols}')
            return -1
    
    return 1

if __name__ == "__main__":
    
    output_directory = 'output'

    # Create directory if it not exsists
    if not os.path.exists(output_directory):
        os.mkdir(output_directory)
    
    # Read Datasets
    spark = get_spark_session()
    df_immigration = spark.read.format('com.github.saurfang.sas.spark').load('../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat')
    df_demographics = spark.read.csv('us-cities-demographics.csv', inferSchema=True, header=True, sep=';')
    df_temperature = spark.read.csv('../../data2/GlobalLandTemperaturesByCity.csv', inferSchema=True, header=True)
    df_countries = spark.read.csv('i94CountryCodes.csv', inferSchema=True, header=True)

    # Clean Datasets
    df_immigration_clean = clean_immigration_dataset(df_immigration)
    df_demographics_clean = clean_demographics_dataset(df_demographics)
    df_temperature_clean = clean_temperature_dataset(df_temperature)

    # Process Datasets
    df_demographics_processed = process_demographics_dataset(df_demographics_clean, output_path=output_directory)
    df_temperature_processed = process_temperature_dataset(df_temperature_clean, output_path=output_directory)

    # Process Dependent Datasets
    df_country_processed = process_country_dataset(df_countries, df_temperature_processed, output_path=output_directory)
    df_date_processed = process_date_table(df_immigration_clean, output_path=output_directory)
    df_visa_processed = process_visa_table(df_immigration_clean, output_path=output_directory)
    df_immigration_processed = process_immigration_dataset(df_immigration_clean, df_visa_processed, output_path=output_directory)

    # Data Quality Checks on fact and dimension tables
    data_quality_checks(df_immigration_processed, 'immigration', unique_cols=['id'])
    data_quality_checks(df_demographics_processed, 'demographics', unique_cols=['StateCode', 'City'])
    data_quality_checks(df_country_processed, 'countries', unique_cols=['code'])
    data_quality_checks(df_visa_processed, 'visatype', unique_cols=['id'])
    data_quality_checks(df_date_processed, 'date', unique_cols=['date'])
