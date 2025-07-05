import os
import logging
from logging.handlers import RotatingFileHandler

from dotenv import load_dotenv
from .config import BasicConfig

# Load envs
load_dotenv()

# Set up config
config = BasicConfig()

# 確保 logs 資料夾存在
root = os.path.dirname(__file__)
log_dir = os.path.join(root, '..', 'logs')
os.makedirs(log_dir, exist_ok=True)

# 檔案 Handler：滾動式，單檔最大 10 MB，保留 5 個備份
file_handler = RotatingFileHandler(
    filename=os.path.join(log_dir, 'app.log'),
    maxBytes=10 * 1024 * 1024,
    backupCount=5,
    encoding='utf-8'
)

# 終端 Handler：輸出到 stdout
stream_handler = logging.StreamHandler()

# 統一格式
formatter = logging.Formatter(
    "[%(asctime)s] [%(process)d] [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S %z"
)
file_handler.setFormatter(formatter)
stream_handler.setFormatter(formatter)

# 設定 root logger
logging.basicConfig(
    handlers=[file_handler, stream_handler],
    level=logging.INFO
)

logger = logging.getLogger()
