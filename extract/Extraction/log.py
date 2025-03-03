import os
import logging

def setup_logging(log_dir):
    os.makedirs(log_dir, exist_ok=True)  # Ensure log directory exists

    # Create a logger for success logs
    success_logger = logging.getLogger(f"{log_dir}_success")
    success_logger.setLevel(logging.INFO)
    success_handler = logging.FileHandler(f"{log_dir}/zendesk_api_success.log")
    success_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    success_logger.addHandler(success_handler)

    # Create a logger for error logs
    error_logger = logging.getLogger(f"{log_dir}_error")
    error_logger.setLevel(logging.ERROR)
    error_handler = logging.FileHandler(f"{log_dir}/zendesk_api_errors.log")
    error_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    error_logger.addHandler(error_handler)

    # Create a console logger for both success and error messages
    console_logger = logging.getLogger(f"{log_dir}_console")
    console_logger.setLevel(logging.INFO)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    console_logger.addHandler(console_handler)

    return success_logger, error_logger, console_logger
