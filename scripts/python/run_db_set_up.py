import os
import subprocess
from dotenv import load_dotenv


load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "datawarehouse")


BASE_DIR = os.path.dirname(__file__)


DROP_CREATE_SQL = os.path.join(BASE_DIR, "..", "sql", "drop_and_create.sql")
SETUP_SCHEMA_SQL = os.path.join(BASE_DIR, "..", "sql", "set_up_datawarehouse.sql")


def run_psql_command(dbname, sql_file):
    command = [
        "psql",
        "-U",
        DB_USER,
        "-h",
        DB_HOST,
        "-p",
        DB_PORT,
        "-d",
        dbname,
        "-f",
        sql_file,
    ]

    env = os.environ.copy()
    env["PGPASSWORD"] = DB_PASSWORD

    result = subprocess.run(command, env=env, capture_output=True, text=True)

    if result.returncode != 0:
        print("Error:")
        print(result.stderr)
        return False
    else:
        return True


def main():
    print("Dropping and creating the database...")
    if not run_psql_command("postgres", DROP_CREATE_SQL):
        print("Failed during DROP/CREATE")
        return

    print("Setting up schema and initial data...")
    if not run_psql_command(DB_NAME, SETUP_SCHEMA_SQL):
        print("Failed during schema setup")
        return

    print("Process completed successfully!")


if __name__ == "__main__":
    main()
