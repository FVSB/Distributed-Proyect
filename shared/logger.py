import logging
import json
import time
import os
from datetime import datetime
import inspect
import traceback

logs_dir = "app/logs"
logs_json = {}

_initialized = False


def setup_logging(log_dir: str = "app/logs") -> None:
    global logs_dir, _initialized
    if _initialized:
        return
    logs_dir = log_dir
    os.makedirs(logs_dir, exist_ok=True)

    log_file_name = f"my_logs_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.txt"
    log_file_path = os.path.join(logs_dir, log_file_name)

    formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(filename)s - %(lineno)d - %(custom_funcName)s - %(message)s"
    )
    file_handler = logging.FileHandler(log_file_path)
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    logging.basicConfig(handlers=[file_handler, console_handler], level=logging.DEBUG)
    _initialized = True


def serialize_logs(filename: str = "logs_container.json") -> None:
    full_path = os.path.join(logs_dir, filename)
    with open(full_path, "w") as f:
        json.dump(logs_json, f, indent=4)


def log_message(message: str, level: str = "INFO", extra_data: dict = {}, func=None) -> None:
    if not _initialized:
        setup_logging()

    caller_frame = inspect.currentframe().f_back
    caller_line = caller_frame.f_lineno

    if func is None:
        caller_method = caller_frame.f_code.co_name
    else:
        caller_method = func.__name__ if callable(func) else str(func)

    log_entry = {
        "timestamp": datetime.now().isoformat(),
        "level": level,
        "message": f"{message} Error: {traceback.format_exc()}",
        "extra_data": extra_data,
        "method": caller_method,
        "line": caller_line,
    }
    logs_json[time.time()] = log_entry

    extra_data["custom_funcName"] = caller_method
    extra_data["line"] = caller_line

    logger = logging.getLogger(__name__)
    logger.log(logging.getLevelName(level), message, extra=extra_data)


if __name__ == "__main__":
    log_message("Test desde shared/logger.py")
    while True:
        serialize_logs()
        time.sleep(30)
