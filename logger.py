import logging
from datetime import datetime
from pathlib import Path

import emailing.mailtemplate as mailtemplate

class logger():
    """
    Class made to siplify the logging handling during the process
    """
    def __init__(self, mail=False):
        self.mail = mail
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)
        self.filepath = createlogfilepath()

        if not self.logger.handlers:
            formatter = logging.Formatter(
                "[%(levelname)s] %(asctime)s : %(message)s",
                datefmt="%m/%d/%Y %I:%M:%S %p"
            )

            # for writing in file
            file_handler = logging.FileHandler(filename=self.filepath, encoding="utf-8")
            file_handler.setLevel(logging.INFO)
            file_handler.setFormatter(formatter)

            # for console printing
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.DEBUG)
            console_handler.setFormatter(formatter)

            self.logger.addHandler(file_handler)
            self.logger.addHandler(console_handler)
            
    def debug(self, msg, *args, **kwargs):
        self.logger.debug(msg, *args, **kwargs)
        
    def log(self, msg, *args, **kwargs):
        self.logger.info(msg, *args, **kwargs)
        
    def warn(self, msg, *args, **kwargs):
        self.logger.warning(msg, *args, **kwargs)
        
    def error(self, msg, *args, **kwargs):
        self.logger.error(msg, *args, **kwargs)
        if self.mail:
            mailtemplate.errormail(msg)
        
def createlogfilepath():
    """Return a timestamped log file path under the local `logs` directory."""
    logs_dir = Path(__file__).resolve().parent / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    today = datetime.now().strftime("%Y-%m-%d_%H-%M_LOG")
    return logs_dir / f"{today}.log"
