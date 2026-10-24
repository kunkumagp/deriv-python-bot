"""Configuration for the Deriv even/odd digit bot."""

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

DERIV_API_TOKEN = os.getenv("DERIV_API_TOKEN", "").strip()
APP_ID = os.getenv("APP_ID", "").strip()
ENVIRONMENT = os.getenv("ENVIRONMENT", "demo").strip().lower()
_dry_run_value = os.getenv("DRY_RUN", "true").strip().lower()
if _dry_run_value not in {"1", "true", "yes", "on", "0", "false", "no", "off"}:
    raise ValueError("DRY_RUN must be a boolean value.")
DRY_RUN = _dry_run_value in {"1", "true", "yes", "on"}

STARTING_CAPITAL = 100.00
STAKE = 1.00
DAILY_TAKE_PROFIT = 4.00
DAILY_STOP_LOSS = -5.00
MAX_DAILY_TRADES = 15
LOSS_STREAK_LIMIT = 3
LOSS_COOLDOWN_SECONDS = 30 * 60
POST_TRADE_TICK_COOLDOWN = 5

SYMBOL = "1HZ100V"
CURRENCY = "USD"
CONTRACT_DURATION = 1
CONTRACT_DURATION_UNIT = "t"
SIGNAL_WINDOW_SIZE = 100
SIGNAL_THRESHOLD = 0.62

# Dry-run wins pay back 1.95x stake (0.95x net profit); this is only a
# configurable mock assumption, not a quote or promise of live contract payout.
SIMULATED_GROSS_PAYOUT = 1.95

WEBSOCKET_BASE_URL = "wss://ws.derivws.com/websockets/v3"
REQUEST_TIMEOUT_SECONDS = 15
CONTRACT_TIMEOUT_SECONDS = 120
INITIAL_RECONNECT_DELAY_SECONDS = 1
MAX_RECONNECT_DELAY_SECONDS = 60
TRADE_HISTORY_FILE = Path(__file__).resolve().with_name("trade_history.json")


def validate_config() -> None:
    """Fail early for invalid settings and unsafe live-mode configuration."""
    if not APP_ID:
        raise ValueError("APP_ID must be set in the .env file.")
    if ENVIRONMENT not in {"demo", "live"}:
        raise ValueError("ENVIRONMENT must be either 'demo' or 'live'.")
    if not DRY_RUN:
        if not DERIV_API_TOKEN:
            raise ValueError("DERIV_API_TOKEN must be set when DRY_RUN=false.")
