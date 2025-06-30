from .routes import init_routes
from config import config
from flask import Flask
import logging

# 全域設定 LOGGING
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)5s [%(name)s:%(lineno)d] %(message)s"
)


def create_app():
    """
    建立並回傳 Flask 應用
    - 讀取設定
    - 初始化路由
    """
    app = Flask(__name__)
    # 載入 config 物件中的所有設定
    app.config.from_object(config)
    # 註冊路由
    init_routes(app)
    return app
