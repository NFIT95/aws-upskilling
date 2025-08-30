import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def process_glue_data(input_db, input_table, output_db, output_table, columns_to_select):
    """
    Reads from a Glue table, selects columns, and writes to a new Glue table.

    :param input_db: The Glue database of the source table.
    :param input_table: The Glue table name for the source data.
    :param output_db: The Glue database for the destination table.
    :param output_table: The Glue table name for the output data.
    :param columns_to_select: A list of column names to select.
    """
    try:
        # Initialize SparkSession with Hive support enabled to interact with Glue Catalog
        spark = SparkSession.builder \
            .appName("GlueDataProcessing") \
            .enableHiveSupport() \
            .getOrCreate()

        print("SparkSession created with Hive support.")

        # Construct the full table names
        source_table_name = f"{input_db}.{input_table}"
        destination_table_name = f"{output_db}.{output_table}"

        print(f"Reading data from Glue table: {source_table_name}")

        # Read data from the Glue table
        df = spark.table(source_table_name)

        print("Data read successfully. Source schema:")
        df.printSchema()

        print(f"Selecting columns: {', '.join(columns_to_select)}")
        
        # Select the desired columns
        transformed_df = df.select([col(c) for c in columns_to_select])
        
        print("Columns selected. Schema of transformed data:")
        transformed_df.printSchema()

        print(f"Writing transformed data to Glue table: {destination_table_name}")

        # Write the transformed DataFrame back to S3, managed by Glue Catalog
        # This will create a new table in Glue and write the data in Parquet format by default.
        transformed_df.write.mode("overwrite").saveAsTable(destination_table_name)

        print("Data written successfully as a new Glue table.")

        # Stop the SparkSession
        spark.stop()
        print("SparkSession stopped.")

    except Exception as e:
        print(f"An error occurred: {e}")
        sys.exit(1)


if __name__ == '__main__':
    if len(sys.argv) != 6:
        print("Usage: spark-submit process_glue_data.py <input_db> <input_table> <output_db> <output_table> <columns>")
        sys.exit(-1)

    # Command-line arguments for Glue integration
    input_database = sys.argv[1]
    input_table_name = sys.argv[2]
    output_database = sys.argv[3]
    output_table_name = sys.argv[4]
    columns_str = sys.argv[5]
    columns_to_keep = [c.strip() for c in columns_str.split(',')]

    # Call the main processing function
    process_glue_data(input_database, input_table_name, output_database, output_table_name, columns_to_keep)