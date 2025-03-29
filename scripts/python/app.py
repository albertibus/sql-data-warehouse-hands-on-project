from utils.logs import configure_logger
from init_db import set_up_data_warehouse
from bronze import load_bronze_layer
from silver import run_silver_layer

configure_logger(log_to_file=True)

if __name__ == "__main__":
    set_up_data_warehouse()
    load_bronze_layer()
    run_silver_layer()
