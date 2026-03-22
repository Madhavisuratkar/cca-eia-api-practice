import logging
import os
from typing import Optional
from concurrent_log_handler import ConcurrentRotatingFileHandler
from fastapi import Request
from app.middleware.request_context import get_request  # your contextvar getter

# Create log directory if it doesn't exist
log_folder = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'Logs')
os.makedirs(log_folder, exist_ok=True)

class CustomFormatter(logging.Formatter):
    def format(self, record):
        record.email = getattr(record, 'email', 'unknown user')
        record.ipaddr = getattr(record, 'ipaddr', 'unknown ip')
        return super().format(record)

def setup_logger(name: str, filename: str) -> logging.Logger:
    """
    Creates (or retrieves) a logger bound to a specific file.
    """
    log_file = os.path.join(log_folder, filename)
    logger = logging.getLogger(name)

    # Avoid duplicate handlers
    if logger.hasHandlers():
        return logger

    logger.setLevel(logging.INFO)

    formatter = CustomFormatter('%(asctime)s - %(levelname)s - %(message)s')

    # File handler with concurrency-safe rotation
    file_handler = ConcurrentRotatingFileHandler(
        log_file, maxBytes=50 * 1024 * 1024, backupCount=20
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Stream (console) handler
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    return logger


# 👇 Define multiple independent loggers
app_logger = setup_logger("app_logger", "logger.log")
etl_logger = setup_logger("etl_logger", "etl.log")


def log_message(
    level: str,
    message: str,
    *,
    request: Optional[Request] = None,
    data: dict = None,
    ErrorCode: int = 1,
    log_type: str = "app",  # <-- NEW parameter to switch logs
    portfolio_id: str = None
):
    """
    Logs a message with context info.
    Default goes into app.log, but log_type="etl" sends to etl.log.
    """
    try:
        if request:
            ip = getattr(request.client, "host", "unknown")
            email = getattr(request.state, "user_email", "unknown")
            app_name = getattr(request.state, "app_name", "unknown")
            portfolio_id = getattr(request.state, "portfolio_id")
            endpoint = request.url.path
        else:
            req = get_request()
            ip = getattr(req.state, "client_ip", "unknown") if req else "unknown"
            email = getattr(req.state, "user_email", "unknown") if req else "unknown"
            endpoint = getattr(req.state, "endpoint", "unknown") if req else "unknown"
            app_name = getattr(req.state, "app_name", "unknown") if req else "unknown"
    except Exception:
        ip = email = endpoint = app_name = "unknown"

    log_data = (
        f"AppName:{app_name}, Endpoint: {endpoint}, "
        f"Email: {email}, Remote IP Addr: {ip}, ErrorCode: {ErrorCode}"
    )

    # Add portfolio_id if provided
    if portfolio_id:
        log_data += f", Portfolio ID: {portfolio_id}"
        
    log_entry = f"[{log_data}], {message}"
    if data:
        log_entry += f" | Data: {data}"

    # Select which logger to use
    logger = etl_logger if log_type == "etl" else app_logger

    log_func = getattr(logger, level.lower(), logger.info)
    log_func(log_entry)
