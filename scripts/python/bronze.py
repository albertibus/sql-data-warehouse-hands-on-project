import sys
from datetime import datetime
from utils.logs import logging
from utils.psql_commands import TABLES, run_psql_command as psql


logger = logging.getLogger("BRONZE")


def load_tables(source: str, table_file_pairs: list[tuple[str, str]]) -> float:
    """
    Loads data into bronze layer tables for a given source (e.g., CRM or ERP).

    Truncates each table and inserts data from corresponding CSV files using the `COPY` command. Logs operation times
    and returns the total loading time. Returns -1 if any operation fails.

    Args:
        source (str): Source name (e.g., 'crm', 'erp').
        table_file_pairs (list): List of tuples with table names and CSV filenames.

    Returns:
        float: Total loading time in seconds, or -1 if an error occurs.
    """
    logger.info(f"Loading {source} tables")
    total_time = 0

    for table, filename in table_file_pairs:
        logger.info(f"Truncating table: bronze.{table}")
        if not psql(command_str=f"TRUNCATE TABLE bronze.{table}"):
            return -1

        start_time = datetime.now()
        file_path = f"./datasets/source_{source.lower()}/{filename}"
        copy_command = (
            rf"\COPY bronze.{table} FROM {file_path} DELIMITER ',' CSV HEADER;"
        )
        logger.info(f"Inserting data into: bronze.{table}")

        if not psql(command_str=copy_command):
            return -1

        duration = (datetime.now() - start_time).total_seconds()
        total_time += duration
        logger.info(f"Load duration: {duration:.2f} seconds")

    return total_time


def load_bronze_layer():
    """
    Orchestrates the loading of CRM and ERP data into the bronze layer.

    Loads data from CRM and ERP sources, truncating tables and inserting data. Logs the loading times for each source
    and the overall process. Stops if any step fails.

    Args:
        None: Uses predefined configuration for sources and tables.

    Returns:
        None: Logs the loading times and stops on failure.
    """
    logger.info("Loading bronze layer")

    crm_time = load_tables("CRM", TABLES["crm"])
    if crm_time == -1:
        sys.exit(1)

    erp_time = load_tables("ERP", TABLES["erp"])
    if erp_time == -1:
        sys.exit(1)

    total = crm_time + erp_time

    logger.info(f"Total CRM loading time: {crm_time:.2f} seconds")
    logger.info(f"Total ERP loading time: {erp_time:.2f} seconds")
    logger.info(f"Total bronze layer loading time: {total:.2f} seconds")


if __name__ == "__main__":
    load_bronze_layer()
