import logging

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('conversion.log'),
            logging.StreamHandler()
        ]
    )

def log_error(context, error):
    logging.error(f"{context}: {error}")

def log_warning(context, warning):
    logging.warning(f"{context}: {warning}")

def log_info(context, info):
    logging.info(f"{context}: {info}")