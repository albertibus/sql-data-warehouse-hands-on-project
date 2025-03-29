import sys
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
from utils.psql_commands import TABLES, DB_URL, run_psql_command as psql
from utils.logs import logging, configure_logger

logger = logging.getLogger("SILVER")


def truncate_tables(table_file_pairs: list[tuple[str, str]]):
    """
    Truncates specified tables in the silver layer.

    Iterates through the provided list of table names and truncates each table in the
    silver schema. If any truncation operation fails, the process is stopped and
    the script exits with a non-zero status.

    Args:
        table_file_pairs (list): List of tuples containing table names and file names
                                 (file names are ignored in this function).

    Returns:
        None
    """
    for table, _ in table_file_pairs:
        logger.info(f"Truncating table: silver.{table}")
        success = psql(command_str=f"TRUNCATE TABLE silver.{table}")
        if not success:
            sys.exit(1)


def sales_and_price_to_business_rule(row):
    """
    Applies the business rules for sales and price calculations.

    This function calculates 'sls_price' and 'sls_sales' based on specific business rules:
    - If 'sls_price' is null, it is calculated as 'sls_sales' / 'sls_quantity'.
    - If 'sls_price' is negative, its absolute value is taken, and 'sls_sales' is recalculated as sls_price * sls_quantity.
    - If the calculated 'sls_sales' does not match the product of 'sls_quantity' and 'sls_price', it is recalculated as sls_quantity * sls_price.

    **Data Assumptions**:
    - 'sls_price' and 'sls_sales' are not both null in the same row.
    - 'sls_quantity' is never null.

    Args:
        row (pd.Series): A row of the DataFrame containing the sales data. It includes 'sls_price', 'sls_sales', and 'sls_quantity' columns.

    Returns:
        pd.Series: The row with updated 'sls_price' and 'sls_sales' based on the business rules.
    """
    if pd.isnull(row["sls_price"]):
        row["sls_price"] = row["sls_sales"] / row["sls_quantity"]

    if row["sls_price"] < 0:
        row["sls_price"] = abs(row["sls_price"])
        row["sls_sales"] = row["sls_price"] * row["sls_quantity"]

    if row["sls_sales"] != row["sls_price"] * row["sls_quantity"]:
        row["sls_sales"] = row["sls_quantity"] * row["sls_price"]
    return row


def map_prd_line_category(x):
    """
    Maps a product line code to a descriptive product category name.

    Converts the input to an uppercase string and matches it against known product line codes:
    "M" for Mountain, "R" for Road, "S" for Other Sales, and "T" for Touring. If the input is None
    or does not match any known code, returns "N/A".

    Args:
        x (str | None): A single-character code representing a product line.

    Returns:
        str: The corresponding product category name, or "N/A" if input is invalid or unrecognized.
    """
    if x is None:
        return "N/A"

    x = str(x).strip().upper()
    match x:
        case "M":
            return "Mountain"
        case "R":
            return "Road"
        case "S":
            return "Other Sales"
        case "T":
            return "Touring"
        case _:
            return "N/A"


def map_crm_sales_details_date_columns(x):
    """
    Converts an integer date in YYYYMMDD format to a datetime.date object.

    Validates that the input is a valid 8-digit integer representing a date between January 1, 1900,
    and January 1, 2050. If the input is invalid (e.g., not 8 digits, less than or equal to 0, or outside
    the valid date range), returns None. Otherwise, parses and converts the integer to a date.

    Args:
        x (int): An 8-digit integer representing a date in YYYYMMDD format.

    Returns:
        datetime.date | None: A datetime.date object if valid, otherwise None.
    """
    if x <= 0 or len(str(x)) != 8 or x > 20500101 or x < 19000101:
        return None
    return datetime.strptime(str(x), "%Y%m%d").date()


def extract_data(engine, table_file_pairs: list[tuple[str, str]]) -> dict:
    """
    Extracts data from bronze layer tables and returns them as a dictionary of DataFrames.

    For each table in the provided list, executes a SELECT * query from the corresponding bronze schema table.
    Logs extraction times and stores the result in a dictionary keyed by table name. If an error occurs,
    raises an exception.

    Args:
        engine (SQLAlchemy Engine): SQLAlchemy engine used for database connection.
        table_file_pairs (list): List of tuples containing table names and file names (file names are ignored).

    Returns:
        dict: Dictionary with table names as keys and corresponding extracted DataFrames as values.
    """
    data_frames = {}
    extraction_start_time = datetime.now()

    try:
        for table, _ in table_file_pairs:
            start_time_table = datetime.now()
            sql_query = f"SELECT * FROM bronze.{table}"
            logger.info(f"Extracting data from table: {table}")
            df = pd.read_sql(sql_query, engine)

            table_extract_duration = (datetime.now() - start_time_table).total_seconds()
            logger.info(
                f"Table {table} extraction duration: {table_extract_duration:.2f} seconds"
            )

            data_frames[table] = df

        logger.info(
            f"Total bronze layer extraction time: {(datetime.now() - extraction_start_time).total_seconds():.2f} seconds"
        )

    except Exception as e:
        logger.error(f"Error extracting data: {str(e)}")
        raise Exception(f"Error extracting data: {str(e)}")

    return data_frames


def clean_and_load_crm_customer_info(engine, df: pd.DataFrame):
    """
    Cleans and loads CRM customer information into the silver layer table.

    Applies type casting, removes invalid or duplicate records based on the customer ID,
    trims unwanted spaces in string fields, and standardizes values for gender and
    marital status. The cleaned and enriched data is then loaded into the
    `silver.crm_customer_info` table using the provided SQLAlchemy engine.

    Args:
        engine (SQLAlchemy Engine): Database connection engine.
        df (pd.DataFrame): Raw customer data to be cleaned and loaded.

    Returns:
        None
    """
    try:
        logger.info("Processing crm_customer_info_table")
        start_time = datetime.now()

        # Cast columns to the appropriate type
        df["cst_id"] = df["cst_id"].astype("Int64")
        df["cst_create_date"] = pd.to_datetime(df["cst_create_date"])

        str_columns = [
            "cst_firstname",
            "cst_lastname",
            "cst_material_status",
            "cst_gnder",
        ]
        df[str_columns] = df[str_columns].astype("string")

        # Check for null or duplicates in the primary key, if there are duplicates keep the latest date
        df = df.dropna(subset=["cst_id"])
        df = df.sort_values(by=["cst_id", "cst_create_date"], ascending=[True, False])
        df = df.drop_duplicates(subset=["cst_id"], keep="first")

        # Remove unwanted spaces for string columns
        df[str_columns] = df[str_columns].apply(
            lambda x: x.str.strip() if x.dtype == "string" else x
        )

        # Data standardization and consistency
        df["cst_gnder"] = df["cst_gnder"].apply(
            lambda x: (
                "Male"
                if pd.notna(x) and x.upper() == "M"
                else ("Female" if pd.notna(x) and x.upper() == "F" else "N/A")
            )
        )

        df["cst_material_status"] = df["cst_material_status"].apply(
            lambda x: (
                "Married"
                if pd.notna(x) and x.upper() == "M"
                else ("Single" if pd.notna(x) and x.upper() == "S" else "N/A")
            )
        )

        # Load data into the database
        df.to_sql(
            "crm_customer_info",
            schema="silver",
            con=engine,
            if_exists="append",
            index=False,
        )
        total_duration = (datetime.now() - start_time).total_seconds()
        logger.info(
            f"Table crm_customer_info processing time: {total_duration:.2f} seconds"
        )
    except Exception as e:
        logger.error(f"Error cleaning crm_customer_info: {str(e)}")
        raise Exception(f"Error cleaning crm_customer_info: {str(e)}")


def clean_and_load_crm_prd_info(engine, df: pd.DataFrame):
    """
    Cleans and loads CRM product information into the silver layer table.

    Performs type casting, handles null and invalid values, enriches the data by calculating
    end dates, derives new columns such as category ID, and standardizes product line
    classifications. Ensures data consistency and completeness before loading the result
    into the `silver.crm_prd_info` table using the provided SQLAlchemy engine.

    Args:
        engine (SQLAlchemy Engine): Database connection engine.
        df (pd.DataFrame): Raw product data to be cleaned and loaded.

    Returns:
        None
    """
    try:
        logger.info("Processing crm_prd_info_table")
        start_time = datetime.now()

        # Cast columns to the appropriate type
        df["prd_id"] = df["prd_id"].astype("Int64")
        df["prd_cost"] = pd.to_numeric(df["prd_cost"], errors="coerce")
        df["prd_start_dt"] = pd.to_datetime(df["prd_start_dt"], errors="coerce")

        # Set NaT values in prd_start_dt to None for consistency
        df["prd_start_dt"] = df["prd_start_dt"].fillna(pd.NaT)

        # Calculate the end date by shifting the start date for each prd_key and subtracting one day
        # Data enrichment
        df["prd_end_dt"] = df.groupby("prd_key")["prd_start_dt"].shift(
            -1
        ) - pd.Timedelta(days=1)

        # Convert string columns to 'string' type for consistency
        str_columns = ["prd_key", "prd_nm", "prd_line"]
        df[str_columns] = df[str_columns].astype("string")

        # Derive 'cat_id' with the first 5 characters of 'prd_key' and replace '-' with '_'
        df["cat_id"] = df["prd_key"].str[:5].str.replace("-", "_")

        # Update 'prd_key' with the remaining characters (after the first 5)
        df["prd_key"] = df["prd_key"].str[5:]

        # Handle null or negative values in the 'prd_cost' column and replace with the mean
        prd_cost_mean = df["prd_cost"].mean()
        df["prd_cost"] = df["prd_cost"].apply(
            lambda x: prd_cost_mean if (x < 0 or pd.isnull(x)) else x
        )

        # Data standardization and consistency for 'prd_line'
        df["prd_line"] = df["prd_line"].apply(lambda x: map_prd_line_category(x))

        # Load data into the database
        df.to_sql(
            "crm_prd_info",
            schema="silver",
            con=engine,
            if_exists="append",
            index=False,
        )
        total_duration = (datetime.now() - start_time).total_seconds()
        logger.info(f"Table crm_prd_info processing time: {total_duration:.2f} seconds")
    except Exception as e:
        logger.error(f"Error cleaning crm_prd_info: {str(e)}")
        raise Exception(f"Error cleaning crm_prd_info: {str(e)}")


def clean_and_load_crm_sales_details(engine, df: pd.DataFrame):
    """
    Cleans and loads CRM sales details data into the silver layer.

    Applies date formatting to order, shipment, and due dates using mapping logic. Ensures
    the business rule `sales = quantity * price` is satisfied, correcting values when needed.
    After validation and transformation, loads the cleaned data into the
    `silver.crm_sales_details` table using the provided SQLAlchemy engine.

    Args:
        engine (SQLAlchemy Engine): Database connection engine.
        df (pd.DataFrame): DataFrame containing raw sales data.

    Returns:
        None
    """
    try:
        logger.info("Processing crm_sales_details_table")
        start_time = datetime.now()

        # Dates do not overlap, however formatting needs to be done
        df["sls_order_dt"] = df["sls_order_dt"].apply(
            lambda x: map_crm_sales_details_date_columns(x)
        )
        df["sls_ship_dt"] = df["sls_ship_dt"].apply(
            lambda x: map_crm_sales_details_date_columns(x)
        )
        df["sls_due_dt"] = df["sls_due_dt"].apply(
            lambda x: map_crm_sales_details_date_columns(x)
        )

        # Business rule -> sales = quantity * price
        # Iterate over columns to check whether the business rule is met;
        # if not, transform the values accordingly
        df = df.apply(lambda row: sales_and_price_to_business_rule(row), axis=1)

        # Load data into the database
        df.to_sql(
            "crm_sales_details",
            schema="silver",
            con=engine,
            if_exists="append",
            index=False,
        )
        total_duration = (datetime.now() - start_time).total_seconds()
        logger.info(
            f"Table crm_sales_details processing time: {total_duration:.2f} seconds"
        )
    except Exception as e:
        logger.error(f"Error cleaning crm_sales_details: {str(e)}")
        raise Exception(f"Error cleaning crm_sales_details: {str(e)}")


def run_silver_layer():
    try:
        engine = create_engine(DB_URL)
        tables_files = [item for sublist in TABLES.values() for item in sublist]

        ## Bronze
        # Data extraction
        dfs = extract_data(engine, tables_files)

        ## Silver
        # Truncate silver tables before performing the data transfomation and load
        truncate_tables(tables_files)

        # Transform and load data
        logger.info(f"Transforming and loading CRM tables")
        start_time = datetime.now()
        clean_and_load_crm_customer_info(engine=engine, df=dfs["crm_customer_info"])
        clean_and_load_crm_prd_info(engine=engine, df=dfs["crm_prd_info"])
        clean_and_load_crm_sales_details(engine=engine, df=dfs["crm_sales_details"])
        logger.info(
            f"Total CRM tables transform and load duration: {(datetime.now() - start_time).total_seconds():.2f} seconds"
        )
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    configure_logger()
    run_silver_layer()
