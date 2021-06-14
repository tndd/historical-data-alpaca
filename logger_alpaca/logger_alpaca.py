import json
from pathlib import Path
from logging import config, getLogger, Logger


def get_logger(logger_name: str) -> Logger:
    log_file_name = 'app.log'
    parent_path = Path(__file__).parent
    log_conf_name = 'logging.json'
    with open(f'{parent_path}/{log_conf_name}', 'r') as f:
        cnf = json.load(f)
    cnf['handlers']['logFileHandler']['filename'] = f'{parent_path}/{log_file_name}'
    config.dictConfig(cnf)
    return getLogger(logger_name)


if __name__ == '__main__':
    print(get_logger('test'))
