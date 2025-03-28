import sys
import logging
from utils.psql_commands import DB_NAME, run_psql_script as psql

logger = logging.getLogger("INIT_DB")
IGNORABLE_ERRORS = ["is being accessed by other users"]

SQL_SCRIPTS = [
    ("Setting up the database", "./scripts/sql/drop_and_create.sql", "postgres"),
    (
        "Instantiating database schemas",
        "./scripts/sql/set_up_datawarehouse.sql",
        DB_NAME,
    ),
    (
        "Instantiating bronze layer tables",
        "./scripts/sql/bronze/ddl_bronze.sql",
        DB_NAME,
    ),
    (
        "Instantiating silver layer tables",
        "./scripts/sql/silver/ddl_silver.sql",
        DB_NAME,
    ),
]


def set_up_data_warehouse():
    """
    Sets up the data warehouse by executing a series of SQL scripts to create and
    configure the necessary database schemas and tables.

    This function runs a list of predefined SQL scripts, logs each operation, and
    checks for any errors during execution. If any error occurs (except for those
    listed in `IGNORABLE_ERRORS`), the process will stop, and further scripts will not be executed.

    Args:
        None: This function does not take any arguments. It uses predefined SQL scripts and
              settings specified within the function.

    Returns:
        None: This function does not return anything. It only logs the status of each SQL script execution.
    """
    for message, script, dbname in SQL_SCRIPTS:
        logger.info(f"Starting: {message}")

        try:
            success = psql(
                sql_script=script,
                dbname=dbname if dbname else None,
                ignorable_errors=IGNORABLE_ERRORS,
            )
            if not success:
                sys.exit(1)
            logger.info(f"SUCCESS: {message}")
        except Exception as e:
            logger.error(f"An unexpected error occurred while executing {message}.")
            logger.error(f"Error details: {str(e)}")
            sys.exit(1)


if __name__ == "__main__":
    set_up_data_warehouse()
