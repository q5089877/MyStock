import os
from app import create_app

import logging

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)5s [%(name)s:%(lineno)d] %(message)s"
)

app = create_app()

if __name__ == "__main__":
    port = int(os.getenv("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
