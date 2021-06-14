import json
import os
from datetime import datetime
from pathlib import Path
from logging import config, getLogger, Logger


def get_logger(logger_name: str) -> Logger:
    datetime_now = datetime.now().strftime('%Y%m%d%H%M%S')
    log_file_name = f'{datetime_now}.log'
    parent_path = Path(__file__).parent
    log_conf_name = 'logging.json'
    log_dir_path = f'{parent_path}/log'
    os.makedirs(log_dir_path, exist_ok=True)
    with open(f'{parent_path}/{log_conf_name}', 'r') as f:
        cnf = json.load(f)
    cnf['handlers']['logFileHandler']['filename'] = f'{log_dir_path}/{log_file_name}'
    config.dictConfig(cnf)
    return getLogger(logger_name)


if __name__ == '__main__':
    print(get_logger('test'))
