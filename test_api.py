from argparse import ArgumentParser
from datetime import datetime
import requests
import sys
import time
try:
    from colorama import init, Fore, Style
    init()
    GREEN = Fore.GREEN
    RED = Fore.RED
    RESET = Style.RESET_ALL
except ImportError:
    GREEN = RED = RESET = ""

# === CONFIG ===
BASE_URL = "https://mystock-oovw.onrender.com"
API_TOKEN = "qazwsxedcrfvtgbyhnj"
TIMEOUT = 30
MAX_RETRY = 3
# ==============


def pretty(status_code):
    color = GREEN if 200 <= status_code < 300 else RED
    return f"{color}{status_code}{RESET}"


def call_api(method, path, headers=None):
    url = f"{BASE_URL}{path}"
    for attempt in range(1, MAX_RETRY + 1):
        try:
            resp = requests.request(
                method, url, headers=headers, timeout=TIMEOUT)
            return resp
        except requests.exceptions.RequestException as e:
            if attempt == MAX_RETRY:
                raise
            print(f"  ⚠️  {e} → retry {attempt}/{MAX_RETRY}")
            time.sleep(1)


def main():
    ap = ArgumentParser()
    ap.add_argument("--date", help="Target date YYYY-MM-DD")
    ap.add_argument("--broadcast", action="store_true",
                    help="Really push to LINE")
    args = ap.parse_args()

    print("=== Wakeup ===")
    resp = call_api("GET", "/wakeup")       # 不帶任何 header
    print("Status:", pretty(resp.status_code), resp.text or "<empty>")

    print("\n=== Update ===")
    headers = {
        "API-Access-Token": API_TOKEN,
        "Need-Broadcast": str(args.broadcast)
    }
    if args.date:
        headers["Target-Date"] = args.date
    resp = call_api("GET", "/update", headers=headers)
    print("Status:", pretty(resp.status_code))
    try:
        print(resp.json())
    except ValueError:
        print(resp.text or "<empty>")


if __name__ == "__main__":
    main()
