from datetime import datetime
from utils.logs import configure_logger, logging
from init_db import set_up_data_warehouse
from bronze import load_bronze_layer
from silver import run_silver_layer

configure_logger(log_to_file=True)

logger = logging.getLogger("MAIN")

if __name__ == "__main__":
    start_time = datetime.now()
    set_up_data_warehouse()
    load_bronze_layer()
    run_silver_layer()
    logger.info(
        f"Total ETL executiion time: {(datetime.now() - start_time).total_seconds():.2f} seconds"
    )
    logger.info("ETL process completed successfully")
