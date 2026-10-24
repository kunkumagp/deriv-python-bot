"""Async Deriv tick-stream bot for Even/Odd digit contracts."""

import asyncio
import json
import logging
import random
import time
from collections import deque
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import websockets
from websockets.exceptions import WebSocketException

import config


class DerivAPIError(Exception):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(f"{code}: {message}")
        self.code = code
        self.retryable = code.lower() in {
            "ratelimit",
            "toomanyrequests",
            "timeout",
            "internalservererror",
            "serviceunavailable",
        }


class TradeExecutionUncertain(RuntimeError):
    """Raised when a live order may exist but its final result is unknown."""


def make_logger() -> logging.Logger:
    logger = logging.getLogger("deriv_even_odd")
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        handler = logging.StreamHandler()
        color_enabled = handler.stream.isatty()

        class ColorFormatter(logging.Formatter):
            COLORS = {
                logging.INFO: "\033[36m",
                logging.WARNING: "\033[33m",
                logging.ERROR: "\033[31m",
                logging.CRITICAL: "\033[1;31m",
            }

            def format(self, record: logging.LogRecord) -> str:
                message = super().format(record)
                color = self.COLORS.get(record.levelno)
                if color_enabled and color:
                    return f"{color}{message}\033[0m"
                return message

        handler.setFormatter(
            ColorFormatter("%(asctime)s | %(levelname)s | %(message)s")
        )
        logger.addHandler(handler)
    return logger


LOGGER = make_logger()


class DerivClient:
    """Owns one WebSocket connection and correlates API replies by req_id."""

    def __init__(self, app_id: str) -> None:
        self.uri = f"{config.WEBSOCKET_BASE_URL}?{urlencode({'app_id': app_id})}"
        self.websocket: Any = None
        self.reader_task: asyncio.Task[None] | None = None
        self.pending: dict[int, asyncio.Future[dict[str, Any]]] = {}
        self.request_id = 0
        self.send_lock = asyncio.Lock()
        self.tick_queue: asyncio.Queue[dict[str, Any] | None] = asyncio.Queue()
        self.contract_queue: asyncio.Queue[dict[str, Any] | None] = asyncio.Queue()

    async def __aenter__(self) -> "DerivClient":
        self.websocket = await websockets.connect(
            self.uri,
            ping_interval=20,
            ping_timeout=20,
            close_timeout=10,
        )
        self.reader_task = asyncio.create_task(self._read_messages())
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        if self.websocket is not None:
            await self.websocket.close()
        if self.reader_task is not None:
            if not self.reader_task.done():
                self.reader_task.cancel()
            try:
                await self.reader_task
            except asyncio.CancelledError:
                pass

    async def request(
        self,
        payload: dict[str, Any],
        timeout: float = config.REQUEST_TIMEOUT_SECONDS,
    ) -> dict[str, Any]:
        if self.websocket is None or self.reader_task is None:
            raise ConnectionError("WebSocket is not connected.")
        if self.reader_task.done():
            raise ConnectionError("WebSocket message reader has stopped.")

        self.request_id += 1
        req_id = self.request_id
        future: asyncio.Future[dict[str, Any]] = (
            asyncio.get_running_loop().create_future()
        )
        self.pending[req_id] = future
        request = dict(payload)
        request["req_id"] = req_id
        try:
            async with self.send_lock:
                await self.websocket.send(json.dumps(request))
            return await asyncio.wait_for(future, timeout=timeout)
        finally:
            self.pending.pop(req_id, None)

    async def _read_messages(self) -> None:
        failure: BaseException = ConnectionError("WebSocket connection closed.")
        try:
            async for raw_message in self.websocket:
                try:
                    message = json.loads(raw_message)
                except json.JSONDecodeError as exc:
                    raise ConnectionError("Deriv sent invalid JSON.") from exc

                req_id = message.get("req_id")
                future = self.pending.get(req_id)
                if future is not None and not future.done():
                    error = message.get("error")
                    if error:
                        future.set_exception(
                            DerivAPIError(
                                str(error.get("code", "APIError")),
                                str(error.get("message", "Unknown API error")),
                            )
                        )
                    else:
                        future.set_result(message)

                msg_type = message.get("msg_type")
                if msg_type == "tick":
                    await self.tick_queue.put(message)
                elif msg_type == "proposal_open_contract":
                    await self.contract_queue.put(message)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            failure = exc
            raise
        finally:
            for future in self.pending.values():
                if not future.done():
                    future.set_exception(failure)
            await self.tick_queue.put(None)
            await self.contract_queue.put(None)


class DailyLedger:
    """Persists one UTC-day session while preserving previous daily summaries."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.today = datetime.now(timezone.utc).date().isoformat()
        self.mode = "dry_run" if config.DRY_RUN else config.ENVIRONMENT
        self.data: dict[str, Any] = {
            "trades": [],
            "daily_stats": {},
            "unresolved_order": None,
        }
        self.stats: dict[str, Any] = {
            "trades": 0,
            "pnl": 0.0,
            "wins": 0,
            "losses": 0,
            "loss_streak": 0,
            "balance": config.STARTING_CAPITAL,
        }
        self._load()

    def _load(self) -> None:
        if not self.path.exists():
            self._save()
            return
        try:
            stored = json.loads(self.path.read_text(encoding="utf-8"))
            if not isinstance(stored, dict):
                raise ValueError("trade history root must be a JSON object")
            trades = stored.get("trades", [])
            daily_stats = stored.get("daily_stats", {})
            if not isinstance(trades, list) or not isinstance(daily_stats, dict):
                raise ValueError("trade history has an invalid structure")
            unresolved_order = stored.get("unresolved_order")
            if unresolved_order is not None and not isinstance(unresolved_order, dict):
                raise ValueError("unresolved order marker is invalid")
            self.data = {
                "trades": trades,
                "daily_stats": daily_stats,
                "unresolved_order": unresolved_order,
            }
            day_stats = daily_stats.get(self.today, {})
            if not isinstance(day_stats, dict):
                raise ValueError("today's daily statistics are invalid")
            today_stats = day_stats.get(self.mode)
            if today_stats is not None:
                required = {
                    "trades",
                    "pnl",
                    "wins",
                    "losses",
                    "loss_streak",
                    "balance",
                }
                if not isinstance(today_stats, dict) or not required.issubset(
                    today_stats
                ):
                    raise ValueError("today's trade statistics are incomplete")
                self.stats.update(today_stats)
        except (OSError, json.JSONDecodeError, ValueError, TypeError) as exc:
            raise RuntimeError(
                f"Could not safely load trade history at {self.path}: {exc}"
            ) from exc
        self._save()

    def record(self, trade: dict[str, Any]) -> None:
        self.rollover_if_needed()
        pnl = float(trade["pnl"])
        self.stats["trades"] += 1
        self.stats["pnl"] = round(float(self.stats["pnl"]) + pnl, 2)
        self.stats["balance"] = round(float(self.stats["balance"]) + pnl, 2)
        if pnl > 0:
            self.stats["wins"] += 1
            self.stats["loss_streak"] = 0
        elif pnl < 0:
            self.stats["losses"] += 1
            self.stats["loss_streak"] += 1
        else:
            self.stats["loss_streak"] = 0

        trade["balance"] = self.stats["balance"]
        trade["session_pnl"] = self.stats["pnl"]
        self.data["trades"].append(trade)
        self.data["unresolved_order"] = None
        self._save()

    def mark_order_pending(
        self, contract_type: str, contract_id: str | None = None
    ) -> None:
        self.data["unresolved_order"] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "mode": self.mode,
            "contract_type": contract_type,
            "stake": config.STAKE,
            "contract_id": contract_id,
        }
        self._save()

    def clear_pending_order(self) -> None:
        self.data["unresolved_order"] = None
        self._save()

    def set_live_balance(self, balance: float) -> None:
        self.stats["balance"] = round(balance, 2)
        self._save()

    def rollover_if_needed(self) -> None:
        today = datetime.now(timezone.utc).date().isoformat()
        if today == self.today:
            return
        self.today = today
        self.stats = {
            "trades": 0,
            "pnl": 0.0,
            "wins": 0,
            "losses": 0,
            "loss_streak": 0,
            "balance": self.stats["balance"],
        }
        self._save()

    def clear_loss_streak(self) -> None:
        self.stats["loss_streak"] = 0
        self._save()

    def _save(self) -> None:
        day_stats = self.data["daily_stats"].setdefault(self.today, {})
        if not isinstance(day_stats, dict):
            raise RuntimeError(f"Invalid daily statistics for {self.today}.")
        day_stats[self.mode] = dict(self.stats)
        temporary_path = self.path.with_name(self.path.name + ".tmp")
        try:
            temporary_path.write_text(
                json.dumps(self.data, indent=2, ensure_ascii=True) + "\n",
                encoding="utf-8",
            )
            temporary_path.replace(self.path)
        except OSError as exc:
            raise RuntimeError(f"Could not persist trade history: {exc}") from exc


def extract_last_digit(tick: dict[str, Any]) -> int:
    quote = Decimal(str(tick["tick"]["quote"]))
    pip_size = tick["tick"].get("pip_size")
    decimal_places = (
        max(0, -quote.as_tuple().exponent) if pip_size is None else int(pip_size)
    )
    if decimal_places < 0 or decimal_places > 10:
        raise ValueError(f"Invalid pip_size: {decimal_places}")
    formatted = f"{quote:.{decimal_places}f}"
    digits = formatted.replace(".", "").replace("-", "")
    if not digits:
        raise ValueError(f"Cannot extract a digit from quote {quote}.")
    return int(digits[-1])


class TradingBot:
    def __init__(self) -> None:
        config.validate_config()
        self.ledger = DailyLedger(config.TRADE_HISTORY_FILE)
        self.digits: deque[int] = deque(maxlen=config.SIGNAL_WINDOW_SIZE)
        self.ticks_to_skip = 0
        self.cooldown_until = 0.0
        if self.ledger.stats["loss_streak"] >= config.LOSS_STREAK_LIMIT:
            self.cooldown_until = time.monotonic() + config.LOSS_COOLDOWN_SECONDS
        self.stop_reason: str | None = None

    def _check_stop_conditions(self) -> str | None:
        pnl = float(self.ledger.stats["pnl"])
        trades = int(self.ledger.stats["trades"])
        if pnl >= config.DAILY_TAKE_PROFIT:
            return "daily take-profit reached"
        if pnl <= config.DAILY_STOP_LOSS:
            return "daily stop-loss reached"
        if trades >= config.MAX_DAILY_TRADES:
            return "daily trade limit reached"
        return None

    def _process_tick(self, message: dict[str, Any], log: bool = True) -> None:
        digit = extract_last_digit(message)
        self.digits.append(digit)
        if not log:
            return
        even_count = sum(value % 2 == 0 for value in self.digits)
        count = len(self.digits)
        even_pct = 100 * even_count / count
        odd_pct = 100 - even_pct
        stream = "".join(str(value) for value in self.digits)
        LOGGER.info(
            "Tick digit=%d | recent digits=%s | EVEN %.1f%% / ODD %.1f%% (%d/%d)",
            digit,
            stream,
            even_pct,
            odd_pct,
            count,
            config.SIGNAL_WINDOW_SIZE,
        )

    def _signal(self) -> tuple[str, float] | None:
        if len(self.digits) < config.SIGNAL_WINDOW_SIZE:
            return None
        even_pct = sum(value % 2 == 0 for value in self.digits) / len(self.digits)
        odd_pct = 1 - even_pct
        if even_pct >= config.SIGNAL_THRESHOLD:
            return "DIGITODD", even_pct
        if odd_pct >= config.SIGNAL_THRESHOLD:
            return "DIGITEVEN", odd_pct
        return None

    async def _authorize_and_read_balance(self, client: DerivClient) -> None:
        authorized = await client.request({"authorize": config.DERIV_API_TOKEN})
        account = authorized.get("authorize", {})
        account_currency = account.get("currency")
        if account_currency != config.CURRENCY:
            raise ValueError(
                f"Account currency must be {config.CURRENCY}; got {account_currency!r}."
            )
        is_virtual = account.get("is_virtual")
        if is_virtual is None:
            raise ValueError("Authorize response did not identify the account mode.")
        actual_environment = "demo" if bool(int(is_virtual)) else "live"
        if actual_environment != config.ENVIRONMENT:
            raise ValueError(
                f"Configured ENVIRONMENT={config.ENVIRONMENT!r} does not match "
                f"the authorized {actual_environment!r} account."
            )
        balance_response = await client.request({"balance": 1})
        balance = float(balance_response["balance"]["balance"])
        self.ledger.set_live_balance(balance)
        LOGGER.info(
            "Authorized %s account; balance=%.2f USD",
            config.ENVIRONMENT,
            balance,
        )

    async def _dry_run_trade(self, contract_type: str) -> float:
        await asyncio.sleep(0)
        selected_parity = 1 if contract_type == "DIGITODD" else 0
        winning_digit = random.randrange(10)
        if winning_digit % 2 == selected_parity:
            return round(config.STAKE * (config.SIMULATED_GROSS_PAYOUT - 1), 2)
        return -config.STAKE

    async def _live_trade(
        self, client: DerivClient, contract_type: str
    ) -> tuple[float, str]:
        proposal_response = await client.request(
            {
                "proposal": 1,
                "amount": config.STAKE,
                "basis": "stake",
                "contract_type": contract_type,
                "currency": config.CURRENCY,
                "duration": config.CONTRACT_DURATION,
                "duration_unit": config.CONTRACT_DURATION_UNIT,
                "symbol": config.SYMBOL,
            }
        )
        proposal = proposal_response.get("proposal", {})
        proposal_id = proposal.get("id")
        ask_price = float(proposal.get("ask_price", 0))
        if not proposal_id or ask_price <= 0 or ask_price > config.STAKE:
            raise ValueError(
                f"Invalid proposal response or price outside fixed stake: {proposal!r}"
            )

        self.ledger.mark_order_pending(contract_type)
        try:
            buy_response = await client.request(
                {"buy": proposal_id, "price": ask_price}
            )
        except DerivAPIError:
            self.ledger.clear_pending_order()
            raise
        except (TimeoutError, ConnectionError, OSError, WebSocketException) as exc:
            raise TradeExecutionUncertain(
                "Buy request outcome is unknown; stopping to prevent a duplicate order."
            ) from exc
        contract_id = buy_response.get("buy", {}).get("contract_id")
        if not contract_id:
            raise TradeExecutionUncertain(
                f"Buy response did not identify the order: {buy_response!r}"
            )
        self.ledger.mark_order_pending(contract_type, str(contract_id))

        subscription_id = None
        try:
            initial = await client.request(
                {
                    "proposal_open_contract": 1,
                    "contract_id": contract_id,
                    "subscribe": 1,
                }
            )
            subscription = initial.get("subscription")
            if isinstance(subscription, dict):
                subscription_id = subscription.get("id")
            contract = initial.get("proposal_open_contract", {})
            while not self._is_settled(contract):
                update = await asyncio.wait_for(
                    client.contract_queue.get(),
                    timeout=config.CONTRACT_TIMEOUT_SECONDS,
                )
                if update is None:
                    raise ConnectionError("Connection dropped before contract settlement.")
                candidate = update.get("proposal_open_contract", {})
                if str(candidate.get("contract_id")) == str(contract_id):
                    contract = candidate
            pnl = float(contract["profit"])
            return round(pnl, 2), str(contract_id)
        except (
            TimeoutError,
            ConnectionError,
            WebSocketException,
            DerivAPIError,
            KeyError,
            ValueError,
        ) as exc:
            raise TradeExecutionUncertain(
                f"Contract {contract_id} was bought but its result is unknown; "
                "stopping to prevent another order."
            ) from exc
        finally:
            await self._forget(client, subscription_id)

    @staticmethod
    def _is_settled(contract: dict[str, Any]) -> bool:
        status = str(contract.get("status", "")).lower()
        return bool(contract.get("is_sold")) or status in {"won", "lost", "sold"}

    async def _forget(self, client: DerivClient, subscription_id: Any) -> None:
        if subscription_id is None:
            return
        try:
            await client.request({"forget": subscription_id})
        except (ConnectionError, TimeoutError, DerivAPIError, WebSocketException) as exc:
            LOGGER.warning("Could not cancel subscription %s: %s", subscription_id, exc)

    async def _execute_trade(
        self,
        client: DerivClient,
        contract_type: str,
        signal_percentage: float,
    ) -> None:
        side = "ODD" if contract_type == "DIGITODD" else "EVEN"
        LOGGER.info(
            "Placing %s trade | symbol=%s | duration=1 tick | stake=%.2f USD | "
            "trigger ratio=%.1f%% | dry_run=%s",
            side,
            config.SYMBOL,
            config.STAKE,
            signal_percentage * 100,
            config.DRY_RUN,
        )
        contract_id = "simulated"
        if config.DRY_RUN:
            pnl = await self._dry_run_trade(contract_type)
        else:
            pnl, contract_id = await self._live_trade(client, contract_type)

        if pnl > 0:
            outcome = "WIN"
        elif pnl < 0:
            outcome = "LOSS"
        else:
            outcome = "BREAKEVEN"
        trade = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "mode": "dry_run" if config.DRY_RUN else config.ENVIRONMENT,
            "symbol": config.SYMBOL,
            "contract_id": contract_id,
            "contract_type": contract_type,
            "stake": config.STAKE,
            "pnl": pnl,
            "outcome": outcome,
        }
        self.ledger.record(trade)
        LOGGER.info(
            "%s | trade PnL=%+.2f USD | session PnL=%+.2f USD | "
            "balance=%.2f USD | wins=%d losses=%d | trades=%d/%d",
            outcome,
            pnl,
            self.ledger.stats["pnl"],
            self.ledger.stats["balance"],
            self.ledger.stats["wins"],
            self.ledger.stats["losses"],
            self.ledger.stats["trades"],
            config.MAX_DAILY_TRADES,
        )

        self.stop_reason = self._check_stop_conditions()
        if self.stop_reason:
            if self.stop_reason == "daily take-profit reached":
                LOGGER.warning(
                    "Victory: daily take-profit reached; cancelling subscriptions and exiting."
                )
            else:
                LOGGER.warning("Risk breaker triggered: %s.", self.stop_reason)
            return

        if self.ledger.stats["loss_streak"] >= config.LOSS_STREAK_LIMIT:
            self.cooldown_until = time.monotonic() + config.LOSS_COOLDOWN_SECONDS
            LOGGER.warning(
                "%d consecutive losses: mandatory 30-minute cooldown begins.",
                config.LOSS_STREAK_LIMIT,
            )

        self.ticks_to_skip = config.POST_TRADE_TICK_COOLDOWN
        while True:
            try:
                queued_tick = client.tick_queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            if queued_tick is None:
                raise ConnectionError("Connection dropped after trade settlement.")
            self._process_tick(queued_tick, log=False)
        LOGGER.info(
            "Trade execution cooldown: waiting for %d fresh ticks before signal checks.",
            config.POST_TRADE_TICK_COOLDOWN,
        )

    async def _wait_loss_cooldown(self, client: DerivClient) -> None:
        while True:
            remaining = self.cooldown_until - time.monotonic()
            if remaining <= 0:
                self.cooldown_until = 0.0
                self.ledger.clear_loss_streak()
                self.ticks_to_skip = max(
                    self.ticks_to_skip, config.POST_TRADE_TICK_COOLDOWN
                )
                LOGGER.info(
                    "Loss cooldown complete; waiting for %d fresh ticks before "
                    "evaluating signals.",
                    self.ticks_to_skip,
                )
                return
            LOGGER.warning(
                "Loss cooldown: %s remaining.",
                str(timedelta(seconds=max(1, int(remaining)))),
            )
            await asyncio.sleep(min(60, remaining))
            while True:
                try:
                    queued_tick = client.tick_queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                if queued_tick is None:
                    raise ConnectionError("Connection dropped during loss cooldown.")
                self._process_tick(queued_tick, log=False)

    async def _run_connection(self) -> None:
        async with DerivClient(config.APP_ID) as client:
            if not config.DRY_RUN:
                await self._authorize_and_read_balance(client)
            tick_response = await client.request(
                {"ticks": config.SYMBOL, "subscribe": 1}
            )
            subscription = tick_response.get("subscription", {})
            tick_subscription_id = subscription.get("id")
            LOGGER.info(
                "Streaming %s | mode=%s | dry_run=%s",
                config.SYMBOL,
                config.ENVIRONMENT,
                config.DRY_RUN,
            )
            try:
                while not self.stop_reason:
                    self.ledger.rollover_if_needed()
                    if self.cooldown_until > time.monotonic():
                        await self._wait_loss_cooldown(client)
                        continue
                    message = await client.tick_queue.get()
                    if message is None:
                        raise ConnectionError("Tick stream ended.")
                    self._process_tick(message)
                    if self.ticks_to_skip:
                        self.ticks_to_skip -= 1
                        if self.ticks_to_skip == 0:
                            LOGGER.info(
                                "Fresh-tick cooldown complete; signal checks resumed."
                            )
                        continue
                    signal = self._signal()
                    if signal is not None:
                        await self._execute_trade(client, signal[0], signal[1])
                LOGGER.info("Stopping bot: %s.", self.stop_reason)
            finally:
                await self._forget(client, tick_subscription_id)

    async def run(self) -> None:
        unresolved_order = self.ledger.data.get("unresolved_order")
        if unresolved_order is not None:
            raise TradeExecutionUncertain(
                "Trade history contains an unresolved live order "
                f"({unresolved_order!r}). Reconcile it with Deriv before restarting."
            )
        self.stop_reason = self._check_stop_conditions()
        if self.stop_reason:
            LOGGER.warning("Session already at a hard limit: %s.", self.stop_reason)
            return

        delay = config.INITIAL_RECONNECT_DELAY_SECONDS
        while not self.stop_reason:
            try:
                await self._run_connection()
                delay = config.INITIAL_RECONNECT_DELAY_SECONDS
            except TradeExecutionUncertain:
                raise
            except DerivAPIError as exc:
                if not exc.retryable:
                    raise
                LOGGER.warning("Retryable Deriv API error: %s", exc)
            except (ConnectionError, OSError, TimeoutError, WebSocketException) as exc:
                LOGGER.warning("WebSocket connection problem: %s", exc)

            if not self.stop_reason:
                jitter = random.uniform(0, delay * 0.25)
                wait_seconds = min(
                    delay + jitter, config.MAX_RECONNECT_DELAY_SECONDS
                )
                LOGGER.warning("Reconnecting in %.1f seconds.", wait_seconds)
                await asyncio.sleep(wait_seconds)
                delay = min(delay * 2, config.MAX_RECONNECT_DELAY_SECONDS)


async def async_main() -> None:
    bot = TradingBot()
    await bot.run()


if __name__ == "__main__":
    try:
        asyncio.run(async_main())
    except KeyboardInterrupt:
        LOGGER.info("Bot stopped by user.")
    except TradeExecutionUncertain as exc:
        LOGGER.critical("%s", exc)
        raise SystemExit(2) from exc
    except Exception:
        LOGGER.exception("Bot stopped because of a fatal error.")
        raise SystemExit(1)
