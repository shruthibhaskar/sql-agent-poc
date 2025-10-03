import os
import json
import logging


# Get environment variables
ENVIRONMENT = os.getenv("ENVIRONMENT", "dev")

logger = logging.getLogger()
logger.setLevel("INFO") if ENVIRONMENT == "dev" else logger.setLevel("ERROR")


def log_event(
    request_type,
    log_levels,
    session_id,
    user_id,
    user_query,
    correlation_id,
    services,
    additional_services,
    log_message,
    log_integer=-1,
):
    """
    Logs an event with the specified details.

    Args:
    - env (str): The environment where the event occurred ("dev" or "prod").
    - request_type (str): Type of request (name of lambda function).
    - log_levels (str): Level of the log (e.g., error, warning, info).
    - session_id (str): Unique identifier for the session.
    - user_id (str): Unique identifier for the user.
    - user_query (str): The query made by the user.
    - correlation_id (str): Correlation ID for tracing the request.
    - services (str): List of services involved in the request (python file name).
    - additional_services (str): Additional services involved (function name).
    - log_message (str): The log message to be recorded.

    Returns:
    - (None)
    """
    log_body = {
        "environment": ENVIRONMENT,
        "request_type": request_type,
        "log_levels": log_levels,
        "session_id": session_id,
        "user_id": user_id,
        "user_query": user_query,
        "correlation_id": correlation_id,
        "services": services,
        "additional_services": additional_services,
        "log_message": log_message,
    }

    if log_integer > -1:
        log_body["log_integer"] = log_integer
    log_msg = json.dumps(log_body)

    if log_levels == "error":
        logging.error(log_msg)
    elif log_levels == "warning":
        logging.warning(log_msg)
    elif log_levels == "info":
        logging.info(log_msg)
    else:
        logging.log(logging.INFO, log_msg)
