import os
import subprocess
from dotenv import load_dotenv
import logging

load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "datawarehouse")
DB_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Logger configuration
logger = logging.getLogger("PSQL")


def run_psql_command(
    command_str,
    db_user=DB_USER,
    db_password=DB_PASSWORD,
    db_host=DB_HOST,
    db_port=DB_PORT,
    db_name=DB_NAME,
    extra_env=None,
):
    """
    Executes a psql command with authentication and optional custom environment variables.

    Args:
        command_str (str): The SQL or psql command to execute (e.g., COPY ...).
        db_user (str, optional): Database username (default is loaded from environment variable `DB_USER`).
        db_password (str, optional): Password for the database user (default is loaded from environment variable `DB_PASSWORD`).
        db_host (str, optional): Database host (default is "localhost").
        db_port (str, optional): Database port (default is "5432").
        db_name (str, optional): Database name (default is loaded from environment variable `DB_NAME`).
        extra_env (dict, optional): Additional environment variables to include (default is None).

    Returns:
        bool: True if the command executed successfully, False otherwise.
    """
    command = [
        "psql",
        "-U",
        db_user,
        "-h",
        db_host,
        "-p",
        db_port,
        "-d",
        db_name,
        "-c",
        command_str,
    ]

    env = os.environ.copy()
    env["PGPASSWORD"] = db_password
    if extra_env:
        env.update(extra_env)

    try:
        result = subprocess.run(command, env=env, capture_output=True, text=True)
        result.check_returncode()

        logger.info(f"Command executed successfully: {command_str}")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Error executing command: {command_str}")
        logger.error(f"Subprocess error: {e.stderr}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error while executing command: {command_str}")
        logger.error(f"Error details: {str(e)}")
        return False


def run_psql_script(sql_script, dbname=DB_NAME, ignorable_errors=list()):
    """
    Executes a SQL script file using psql with authentication and optional error filtering.

    Args:
        sql_script (str): Path to the SQL script file to be executed.
        dbname (str): Target database name. Defaults to the main data warehouse DB.
        ignorable_errors (list): List of substrings representing error messages that can be ignored.

    Returns:
        bool: True if the script executed successfully or only raised ignorable errors, False otherwise.
    """
    command = [
        "psql",
        "-v",
        "ON_ERROR_STOP=1",
        "-U",
        DB_USER,
        "-h",
        DB_HOST,
        "-p",
        DB_PORT,
        "-d",
        dbname,
        "-f",
        sql_script,
    ]

    env = os.environ.copy()
    env["PGPASSWORD"] = DB_PASSWORD

    try:
        result = subprocess.run(command, env=env, capture_output=True, text=True)
        result.check_returncode()  # This raises a CalledProcessError for non-zero exit codes

        logger.info(f"SQL script executed successfully: {sql_script}")
        return True
    except subprocess.CalledProcessError as e:
        if any(err in e.stderr for err in ignorable_errors):
            logger.warning(f"Ignoring error in script: {e.stderr}")
            return True
        logger.error(f"Error executing SQL script: {sql_script}")
        logger.error(f"Subprocess error: {e.stderr}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error while executing SQL script: {sql_script}")
        logger.error(f"Error details: {str(e)}")
        return False


def run_psql_script(sql_script, dbname=DB_NAME, ignorable_errors=list()):
    """
    Executes a SQL script file using psql with authentication and optional error filtering.

    Args:
        sql_script (str): Path to the SQL script file to be executed.
        dbname (str): Target database name. Defaults to the main data warehouse DB.
        ignorable_errors (list): List of substrings representing error messages that can be ignored.

    Returns:
        bool: True if the script executed successfully or only raised ignorable errors, False otherwise.
    """
    command = [
        "psql",
        "-v",
        "ON_ERROR_STOP=1",
        "-U",
        DB_USER,
        "-h",
        DB_HOST,
        "-p",
        DB_PORT,
        "-d",
        dbname,
        "-f",
        sql_script,
    ]

    env = os.environ.copy()
    env["PGPASSWORD"] = DB_PASSWORD

    result = subprocess.run(command, env=env, capture_output=True, text=True)

    if result.returncode != 0:
        if any(err in result.stderr for err in ignorable_errors):
            return True
        logger.error(f"{result.stderr}")
        return False
    else:
        return True
