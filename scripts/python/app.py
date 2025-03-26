from utils.logs import configure_logger
from initdb import set_up_data_warehouse
from loadbronze import load_bronze_layer


if __name__ == "__main__":
    configure_logger(log_to_file=True)

    set_up_data_warehouse()
    load_bronze_layer()
