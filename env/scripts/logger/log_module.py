import logging
import logging.handlers
import os
import sys
from logging import StreamHandler
from logging.handlers import RotatingFileHandler
from scripts.config import log

if not os.path.exists(log.LOG_BASE_PATH):
    os.makedirs(log.LOG_BASE_PATH)

logging.trace = logging.DEBUG - 5
logging.addLevelName(logging.DEBUG - 5, 'TRACE')


class FTDMAutomationLogger(logging.getLoggerClass()):
    def __init__(self, name):
        super().__init__(name)

    def trace(self, msg, *args, **kwargs):
        if self.isEnabledFor(logging.trace):
            self._log(logging.trace, msg, args, **kwargs)


def get_logger():
    """sets logger mechanism"""
    _logger = logging.getLogger("FTDM_automation_script_log")
    _logger.setLevel(log.LOG_LEVEL)

    if log.LOG_LEVEL == 'DEBUG' or log.LOG_LEVEL == 'TRACE':
        _formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(module)s - '
                                       '%(lineno)d - %(message)s')
    else:
        _formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    if 'file' in log.LOG_HANDLERS:
        _file_handler = logging.FileHandler(log.FILE_NAME)
        _file_handler.setFormatter(_formatter)
        _logger.addHandler(_file_handler)

    if 'rotating' in log.LOG_HANDLERS:
        _rotating_file_handler = RotatingFileHandler(filename=log.FILE_NAME,
                                                     maxBytes=int(log.FILE_BACKUP_SIZE),
                                                     backupCount=int(log.FILE_BACKUP_COUNT))
        _rotating_file_handler.setFormatter(_formatter)
        _logger.addHandler(_rotating_file_handler)

    if 'console' in log.LOG_HANDLERS:
        _console_handler = StreamHandler(sys.stdout)
        _console_handler.setFormatter(_formatter)
        _logger.addHandler(_console_handler)

    return _logger


logger = get_logger()
