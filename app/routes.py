import gc
import psutil
import datetime
import threading
import logging

from flask import request, Response
from linebot.exceptions import InvalidSignatureError
from .views import update_and_broadcast

logger = logging.getLogger(__name__)


def init_routes(app):
    @app.route("/", methods=["GET"])
    def home():
        """
        Default route for health checks.
        """
        return Response(status=200)

    @app.route("/callback", methods=["POST"])
    def callback():
        """
        Handle incoming webhook events from LINE Bot.
        """
        signature = request.headers.get("X-Line-Signature", "")
        body = request.get_data(as_text=True)
        logger.info(f"Request body: {body}")
        try:
            handler = app.config["WEBHOOK_HANDLER"]
            handler.handle(body, signature)
        except InvalidSignatureError:
            logger.warning("Invalid signature in callback")
            return Response("Invalid signature", status=400)
        return Response(status=200)

    @app.route("/wakeup", methods=["GET"])
    def wakeup():
        """
        Wake up the service and perform garbage collection.
        """
        gc.collect()
        try:
            memory_usage = psutil.Process().memory_info().rss / 1024**2
            logger.debug(f"💾 Memory usage after GC: {memory_usage:.2f} MB")
        except Exception:
            logger.debug("psutil not available or error getting memory usage")
        return Response(status=200)

    @app.route("/update", methods=["GET"])
    def update():
        """
        Update data and optionally broadcast stock recommendations.
        """
        # Prevent overlapping updates
        if app.config.get("is_updating", False):
            logger.warning("🚧 Update already in progress")
            return Response("Update already in progress", status=429)

        # Validate API-Access-Token header
        token = request.headers.get("API-Access-Token")
        if not token:
            logger.warning("Missing API-Access-Token header")
            return Response("Missing API-Access-Token", status=401)
        if token != app.config.get("API_ACCESS_TOKEN"):
            logger.warning("Invalid API-Access-Token header")
            return Response("Invalid API-Access-Token", status=401)

        # Parse target date
        date_str = request.headers.get("Target-Date")
        if date_str:
            try:
                target_date = datetime.datetime.strptime(
                    date_str, "%Y-%m-%d").date()
            except ValueError:
                logger.warning(f"Invalid Target-Date format: {date_str}")
                return Response("Invalid Target-Date format", status=400)
        else:
            target_date = datetime.date.today()

        # Determine broadcast flag (header or query param, default True)
        raw_broadcast = request.headers.get(
            "Need-Broadcast") or request.args.get("need_broadcast")
        if raw_broadcast is None:
            need_broadcast = True
        else:
            need_broadcast = str(raw_broadcast).lower() == "true"

        # Set update flag and spawn background thread
        app.config["is_updating"] = True

        def task():
            with app.app_context():
                try:
                    update_and_broadcast(app, target_date, need_broadcast)
                except Exception:
                    logger.exception("‼️ Update task crashed")
                finally:
                    app.config["is_updating"] = False
                    logger.info("🔄 Update flag reset, ready for next request")

        threading.Thread(target=task, daemon=True).start()
        logger.info(
            f"🚀 Spawned update task date={target_date} broadcast={need_broadcast}")
        return Response(status=200)
