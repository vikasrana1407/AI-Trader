"""
FastAPI Bot Broker Adapter for Kotak Neo Trade API & Dry-Run Simulator.
"""

import logging
import threading
import time
import uuid
from typing import Dict, Any, List, Optional, Tuple
from .config import settings
from .models import OrderSide, OrderIntent, CancelOutcome

logger = logging.getLogger("FastAPIBot.Broker")

try:
    # Installed via: pip install kotakneoapi
    # The package installs as the 'neo_api_client' module internally
    from neo_api_client import NeoAPI
    SDK_AVAILABLE = True
except ImportError:
    try:
        from kotakneoapi import NeoAPI  # alternate import path in some versions
        SDK_AVAILABLE = True
    except ImportError:
        SDK_AVAILABLE = False


from .notifier import TelegramAlerts


def _extract_st_code(payload: Any) -> Optional[Any]:
    """Safely extracts stCode from broker response without assuming data is a dictionary."""
    if not isinstance(payload, dict):
        return None
    code = payload.get("stCode")
    if code is not None:
        return code
    data = payload.get("data")
    if isinstance(data, dict):
        return data.get("stCode")
    return None


class BrokerClient:
    def __init__(self, dry_run: bool = settings.DRY_RUN):
        self.dry_run = dry_run
        self.sdk = None
        self._auth_lock = threading.RLock()
        self._order_report_lock = threading.RLock()
        self._is_logged_in = False
        self._mock_orders: Dict[str, Dict[str, Any]] = {}
        self._mock_scrip_master: Dict[str, int] = {
            "NIFTY24OCT25000CE": 25,
            "NIFTY24OCT25000PE": 25,
        }
        self._mock_positions: List[Dict[str, Any]] = []
        self._scrip_lot_sizes: Dict[str, int] = {}
        self._scrip_master_date: Optional[str] = None
        self._scrip_logged: bool = False
        self._trade_path_warmed: bool = False

    @property
    def is_logged_in(self) -> bool:
        return self._is_logged_in or self.dry_run

    def login(self) -> bool:
        """Establishes or refreshes session with Kotak Neo Trade API (thread-safe)."""
        with self._auth_lock:
            return self.initialize()

    def _scrip_cache_path(self) -> str:
        import os
        base = os.path.dirname(getattr(settings, "PERSISTENCE_FILE", "") or "") or "data"
        return os.path.join(base, "scrip_lot_sizes.json")

    def _load_scrip_cache_from_disk(self, today_str: str) -> bool:
        """Load today's lot-size cache from disk (avoids ~30-60s search_scrip on every login)."""
        import json
        import os
        path = self._scrip_cache_path()
        if not os.path.isfile(path):
            return False
        try:
            with open(path, "r", encoding="utf-8") as f:
                payload = json.load(f)
            if not isinstance(payload, dict) or payload.get("date") != today_str:
                return False
            lots = payload.get("lots") or {}
            if not isinstance(lots, dict) or not lots:
                return False
            self._scrip_lot_sizes = {str(k).upper(): int(v) for k, v in lots.items()}
            self._scrip_master_date = today_str
            logger.info(
                f"SCRIP_MASTER_DISK_CACHE Loaded {len(self._scrip_lot_sizes)} lot sizes "
                f"from {path} (skipping live search_scrip)."
            )
            return True
        except Exception as e:
            logger.warning(f"Failed to load scrip disk cache: {e}")
            return False

    def _save_scrip_cache_to_disk(self, today_str: str) -> None:
        import json
        import os
        path = self._scrip_cache_path()
        try:
            os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
            with open(path, "w", encoding="utf-8") as f:
                json.dump({"date": today_str, "lots": self._scrip_lot_sizes}, f)
            logger.info(f"SCRIP_MASTER_DISK_CACHE Saved {len(self._scrip_lot_sizes)} lot sizes to {path}")
        except Exception as e:
            logger.warning(f"Failed to save scrip disk cache: {e}")

    def _ensure_scrip_master(self):
        """Loads and caches scrip master lot sizes for nse_fo daily."""
        import datetime
        today_str = datetime.date.today().isoformat()
        if self._scrip_master_date == today_str and self._scrip_lot_sizes:
            return

        # Prefer disk cache so protect path / re-login is not blocked ~15-60s
        if self._load_scrip_cache_from_disk(today_str):
            return

        if not self.sdk or not hasattr(self.sdk, "search_scrip"):
            return

        try:
            t0 = time.time()
            results = self.sdk.search_scrip(exchange_segment=settings.EXCHANGE_SEGMENT, symbol="nifty")
            if isinstance(results, list) and results:
                if not self._scrip_logged:
                    sample = results[0]
                    logger.info(
                        f"SCRIP_MASTER_LOADED Matched {len(results)} rows in "
                        f"{time.time() - t0:.2f}s. "
                        f"Columns: {list(sample.keys())} | Sample row: {sample}"
                    )
                    self._scrip_logged = True

                for row in results:
                    sym = (
                        row.get("pTrdSymbol") or row.get("trading_symbol") or
                        row.get("trdSym") or row.get("symbol") or row.get("pSymbolName")
                    )
                    lot = (
                        row.get("lLotSize") or row.get("lotSz") or
                        row.get("pLotSize") or row.get("lot_size")
                    )
                    if sym and lot:
                        try:
                            self._scrip_lot_sizes[str(sym).strip().upper()] = int(lot)
                        except (ValueError, TypeError):
                            pass

                self._scrip_master_date = today_str
                logger.info(f"SCRIP_MASTER_CACHED Cached lot sizes for {len(self._scrip_lot_sizes)} symbols.")
                self._save_scrip_cache_to_disk(today_str)
        except Exception as e:
            logger.warning(f"Failed to load scrip master via search_scrip: {e}")

    def get_lot_size(self, symbol: str) -> Optional[int]:
        """
        Returns lot size for symbol from broker scrip master (or mock).
        Returns None if unknown/unavailable. NEVER falls back to 25.
        """
        if self.dry_run:
            return self._mock_scrip_master.get(symbol)

        sym_upper = symbol.strip().upper()
        self._ensure_scrip_master()

        # 1. Exact match from cache
        if sym_upper in self._scrip_lot_sizes:
            return self._scrip_lot_sizes[sym_upper]

        # 2. Dynamic query if missing from bulk cache
        try:
            if self.sdk and hasattr(self.sdk, "search_scrip"):
                res = self.sdk.search_scrip(exchange_segment=settings.EXCHANGE_SEGMENT, symbol=sym_upper)
                if isinstance(res, list) and res:
                    for row in res:
                        r_sym = (row.get("pTrdSymbol") or row.get("trading_symbol") or row.get("trdSym") or "")
                        lot = row.get("lLotSize") or row.get("lotSz") or row.get("pLotSize") or row.get("lot_size")
                        if lot and (str(r_sym).strip().upper() == sym_upper or not r_sym):
                            val = int(lot)
                            self._scrip_lot_sizes[sym_upper] = val
                            return val
        except Exception as e:
            logger.debug(f"Direct search_scrip for {symbol} failed: {e}")

        return None

    def set_mock_lot_size(self, symbol: str, lot_size: int):
        """Configures mock scrip master lot size for testing."""
        self._mock_scrip_master[symbol] = lot_size

    def set_mock_positions(self, positions: Optional[List[Dict[str, Any]]]):
        """Sets mock positions for testing. Pass None to simulate broker positions API failure."""
        self._mock_positions = positions

    def get_positions(self) -> Optional[List[Dict[str, Any]]]:
        """Returns live positions from broker. In dry-run, returns mock positions.
        Returns None if broker call fails, distinguishing API errors from an empty list of positions."""
        if self.dry_run:
            if self._mock_positions is None:
                return None
            return list(self._mock_positions)
        try:
            if self.sdk and hasattr(self.sdk, "positions"):
                resp = self.sdk.positions()
                if isinstance(resp, dict):
                    st_code = _extract_st_code(resp)
                    if str(st_code) == "100008":
                        logger.warning("positions unauthorized (stCode 100008). Attempting re-authentication...")
                        try:
                            self.login()
                            resp = self.sdk.positions()
                            st_code = _extract_st_code(resp)
                        except Exception as reauth_err:
                            logger.error(f"Re-auth failed during positions query: {reauth_err}")
                            return None

                    if st_code is not None:
                        try:
                            if int(st_code) not in (0, 200):
                                logger.error(f"Broker returned error in positions(): stCode={st_code}, resp={resp}")
                                return None
                        except (ValueError, TypeError):
                            pass
                    if "data" in resp:
                        data = resp.get("data")
                        if isinstance(data, list):
                            return data
                        elif data is None:
                            return []
                    return resp.get("data", []) or []
                elif isinstance(resp, list):
                    return resp
        except Exception as e:
            logger.error(f"Failed to fetch positions from broker: {e}")
            return None
        return None

    def initialize(self) -> bool:
        if self.dry_run:
            logger.info("🧪 [FASTAPI DRY_RUN] Broker Client initialized in simulation mode.")
            self._is_logged_in = True
            return True

        if not SDK_AVAILABLE:
            err = "neo_api_client SDK not installed. Run 'pip install neo-api-client'"
            TelegramAlerts.critical_error("Broker Authentication", err)
            raise RuntimeError(err)

        with self._auth_lock:
            try:
                logger.info("Connecting to Kotak Neo Trade API (session authentication)...")
                neo_kwargs = {
                    "consumer_key": settings.KOTAK_CONSUMER_KEY,
                    "environment": settings.KOTAK_ENVIRONMENT
                }
                if getattr(settings, "KOTAK_CONSUMER_SECRET", None):
                    neo_kwargs["consumer_secret"] = settings.KOTAK_CONSUMER_SECRET

                self.sdk = NeoAPI(**neo_kwargs)

                if hasattr(self.sdk, "totp_login"):
                    totp_val = ""
                    if settings.KOTAK_TOTP_SECRET:
                        try:
                            import pyotp
                            totp_val = pyotp.TOTP(settings.KOTAK_TOTP_SECRET).now()
                        except Exception as ex:
                            logger.warning(f"Failed to generate TOTP from secret: {ex}")
                    ucc = getattr(settings, "KOTAK_UCC", "") or settings.KOTAK_MOBILE_NUMBER
                    res_login = self.sdk.totp_login(mobile_number=settings.KOTAK_MOBILE_NUMBER, ucc=ucc, totp=totp_val)
                    if isinstance(res_login, dict) and "error" in res_login:
                        err_txt = f"totp_login error: {res_login['error']}"
                        logger.error(err_txt)
                        TelegramAlerts.auth_failure("totp_login", err_txt)
                        raise RuntimeError(err_txt)

                    if settings.KOTAK_MPIN:
                        res_val = self.sdk.totp_validate(mpin=settings.KOTAK_MPIN)
                        if isinstance(res_val, dict) and "error" in res_val:
                            err_txt = f"totp_validate error: {res_val['error']}"
                            logger.error(err_txt)
                            TelegramAlerts.auth_failure("totp_validate", err_txt)
                            raise RuntimeError(err_txt)
                elif hasattr(self.sdk, "login"):
                    self.sdk.login(mobilenumber=settings.KOTAK_MOBILE_NUMBER, password=settings.KOTAK_PASSWORD)
                    if settings.KOTAK_MPIN and hasattr(self.sdk, "session_2fa"):
                        self.sdk.session_2fa(OTP=settings.KOTAK_MPIN)

                self._is_logged_in = True
                self._ensure_scrip_master()
                logger.info("✅ Live Kotak Neo session established.")
                # Warm once per process — not on every re-login during place_order.
                if not self._trade_path_warmed:
                    try:
                        self.warmup_trade_path()
                        self._trade_path_warmed = True
                    except Exception as warm_ex:
                        logger.warning(f"Trade-path warmup skipped/failed (non-fatal): {warm_ex}")
                return True
            except Exception as e:
                self._is_logged_in = False
                err_msg = f"Kotak Neo login/session failure: {e}"
                logger.error(err_msg, exc_info=True)
                TelegramAlerts.auth_failure("session_init", err_msg)
                raise

    def warmup_trade_path(self) -> None:
        """
        Prime Kotak HTTPS connections used by protect flow:
        order_report + a rejected place_order (qty=0).
        Keeps first real SL near subsequent ~1s latency.
        """
        if self.dry_run or not self.sdk:
            return
        t0 = time.time()
        try:
            _ = self.get_order_report()
        except Exception as e:
            logger.debug(f"warmup order_report: {e}")
        try:
            resp = self.sdk.place_order(
                exchange_segment=settings.EXCHANGE_SEGMENT,
                product=getattr(settings, "PRODUCT_TYPE", None) or "MIS",
                price="1.00",
                order_type="L",
                quantity="0",
                validity="DAY",
                trading_symbol="NIFTY",
                transaction_type="S",
                amo="NO",
                disclosed_quantity="0",
                trigger_price="0",
                tag="BOT_WARMUP",
            )
            logger.info(
                f"TRADE_PATH_WARMUP place_probe done in {time.time() - t0:.2f}s "
                f"resp={str(resp)[:160]}"
            )
        except Exception as e:
            # Exception still warms TLS/session; log and continue
            logger.info(f"TRADE_PATH_WARMUP place_probe exception after {time.time() - t0:.2f}s: {e}")

    def find_order_by_tag(self, tag: str) -> Optional[str]:
        """Queries broker order report to locate an existing order with the specified tag."""
        if not tag:
            return None
        orders = self.get_order_report()
        if not orders:
            return None
        tag_clean = tag.strip().upper()
        for o in orders:
            o_tag = str(o.get("tag") or o.get("GuiOrdId") or o.get("ordModNo") or o.get("ordTag") or "").strip().upper()
            if o_tag == tag_clean:
                ord_id = str(o.get("nOrdNo") or o.get("order_id") or "").strip()
                if ord_id and ord_id not in ("", "0", "None"):
                    return ord_id
        return None

    def find_matching_order(
        self,
        symbol: str,
        side: Any,
        qty: int,
        ord_type: Optional[str] = None,
        price: Optional[float] = None,
        trigger_price: Optional[float] = None,
        tag: Optional[str] = None,
        prod: Optional[str] = None
    ) -> Tuple[Optional[str], str]:
        """
        Conservative order search in broker order book.
        Matches by available broker attributes:
        - Symbol
        - Transaction side
        - Quantity
        - Order type and product
        - Trigger price and price (within tick size)
        - Tag (if present)
        Returns (order_id, match_status):
        - (order_id, "EXACT_MATCH") if exactly one order matches
        - (None, "NO_MATCH") if no orders match
        - (None, "AMBIGUOUS_MULTIPLE_MATCHES") if >1 orders match (conservative: never guess!)
        """
        orders = self.get_order_report()
        if not orders:
            return None, "NO_MATCH"

        sym_norm = symbol.strip().upper()
        expected_side = "B" if (side == OrderSide.BUY or str(side).upper() in ("B", "BUY", "OrderSide.BUY")) else "S"
        tag_candidates = []
        attr_candidates = []

        for o in orders:
            o_sym = str(o.get("trdSym") or o.get("tradingSymbol") or o.get("symbol", "")).strip().upper()
            if o_sym != sym_norm:
                continue

            o_side = str(o.get("trnsTp") or o.get("transactionType") or "").strip().upper()
            if o_side and o_side != expected_side:
                continue

            try:
                o_qty = int(o.get("qty") or o.get("quantity") or 0)
            except (ValueError, TypeError):
                o_qty = 0
            if o_qty != qty:
                continue

            o_type = str(o.get("ordTyp") or o.get("orderType") or "").strip().upper()
            if ord_type and o_type and o_type != ord_type.upper():
                continue

            if trigger_price and trigger_price > 0:
                try:
                    o_trg = float(o.get("trgPrc") or o.get("triggerPrice") or 0.0)
                    if abs(o_trg - trigger_price) > 0.06:
                        continue
                except (ValueError, TypeError):
                    pass

            ord_id = str(o.get("nOrdNo") or o.get("order_id", "")).strip()
            if not ord_id or ord_id in ("0", "None"):
                continue

            o_tag = str(o.get("tag") or o.get("GuiOrdId") or o.get("ordModNo") or o.get("ordTag") or "").strip().upper()
            if tag and o_tag and o_tag == tag.strip().upper():
                tag_candidates.append(ord_id)
            else:
                attr_candidates.append(ord_id)

        if tag_candidates:
            if len(tag_candidates) == 1:
                return tag_candidates[0], "EXACT_MATCH"
            else:
                logger.warning(
                    f"AMBIGUOUS_MATCH Multiple ({len(tag_candidates)}) candidate orders found for tag {tag}: {tag_candidates}. Conservative policy applied."
                )
                return None, "AMBIGUOUS_MULTIPLE_MATCHES"

        if len(attr_candidates) == 1:
            return attr_candidates[0], "EXACT_MATCH"
        elif len(attr_candidates) > 1:
            logger.warning(
                f"AMBIGUOUS_MATCH Multiple ({len(attr_candidates)}) candidate orders found for {sym_norm} {expected_side} {qty}: {attr_candidates}. Conservative policy applied."
            )
            return None, "AMBIGUOUS_MULTIPLE_MATCHES"
        else:
            return None, "NO_MATCH"

    def place_order(
        self, symbol: str, side: OrderSide, qty: int, ord_type: str,
        price: float, trigger_price: float = 0.0, tag: str = "",
        product: Optional[str] = None, intent: str = "PROTECTIVE_EXIT"
    ) -> str:
        """
        Places order on broker or simulation.
        Enforces automated entry prohibition (AUTO_ENTRY_ENABLED=false).
        Handles duplicate Client Order ID conservatively.
        """
        if intent in ("ENTRY", OrderIntent.ENTRY.value, "AUTO_ENTRY"):
            if not getattr(settings, "AUTO_ENTRY_ENABLED", False):
                err = "Automated order entry is prohibited (AUTO_ENTRY_ENABLED is False). Bot only manages manually placed entries."
                logger.error(f"AUTOMATED_ENTRY_PROHIBITED {err}")
                raise RuntimeError(err)
            if getattr(self, "store", None):
                is_locked, lock_reason = self.store.is_entry_locked()
                if is_locked:
                    err = f"Automated entry prohibited by persistent entry lock: {lock_reason}"
                    logger.error(f"ENTRY_LOCKED {err}")
                    raise RuntimeError(err)

        if intent in ("RE_ENTRY", "AUTO_REENTRY"):
            if not getattr(settings, "AUTO_REENTRY_ENABLED", False):
                err = "Automated order re-entry is prohibited (AUTO_REENTRY_ENABLED is False)."
                logger.error(f"AUTOMATED_REENTRY_PROHIBITED {err}")
                raise RuntimeError(err)
            if getattr(self, "store", None):
                is_locked, lock_reason = self.store.is_entry_locked()
                if is_locked:
                    err = f"Automated re-entry prohibited by persistent entry lock: {lock_reason}"
                    logger.error(f"REENTRY_LOCKED {err}")
                    raise RuntimeError(err)

        # SDK v3 place_order accepts only CNC/NRML/MIS/MTF (rejects BO/CO and aliases).
        from .order_feed import resolve_protective_product, PLACE_ORDER_PRODUCTS
        prod, prod_err = resolve_protective_product(product, fallback=settings.PRODUCT_TYPE)
        if not prod or prod not in PLACE_ORDER_PRODUCTS:
            raise RuntimeError(
                prod_err
                or (
                    f"Invalid product '{product}' for place_order. "
                    f"Allowed: {sorted(PLACE_ORDER_PRODUCTS)}. "
                    f"Check entry product / PRODUCT_TYPE setting."
                )
            )
        if self.dry_run:
            ord_id = f"FAST-{uuid.uuid4().hex[:6].upper()}"
            status = "OPEN" if ord_type == "L" else "TRIGGER_PENDING"
            record = {
                "nOrdNo": ord_id, "order_id": ord_id, "trdSym": symbol,
                "trnsTp": "B" if side == OrderSide.BUY else "S",
                "qty": qty, "ordTyp": ord_type, "prc": price, "trgPrc": trigger_price,
                "ordSt": status, "tag": tag, "prd": prod, "time": time.time(),
                "intent": intent
            }
            self._mock_orders[ord_id] = record
            logger.info(
                f"🧪 [SIM_ORDER] ({intent}) {ord_type} {side.value} {qty}x {symbol} ({prod}) | "
                f"Price: ₹{price:.2f} | Trigger: ₹{trigger_price:.2f} (ID: {ord_id}, Tag: {tag})"
            )
            return ord_id

        # Live Kotak Neo API Call
        price_str = f"{price:.2f}" if isinstance(price, (int, float)) else str(price)
        trigger_str = f"{trigger_price:.2f}" if (ord_type == "SL" and trigger_price > 0) else "0"
        qty_str = str(qty)

        def _do_place():
            return self.sdk.place_order(
                exchange_segment=settings.EXCHANGE_SEGMENT,
                product=prod,
                price=price_str,
                order_type=ord_type,
                quantity=qty_str,
                validity="DAY",
                trading_symbol=symbol,
                transaction_type="B" if side == OrderSide.BUY else "S",
                amo="NO",
                disclosed_quantity="0",
                trigger_price=trigger_str,
                tag=tag or None
            )

        resp = _do_place()

        # Handle session expiry (stCode 100008) with thread-safe re-login & retry
        if isinstance(resp, dict):
            st_code = _extract_st_code(resp)
            if str(st_code) == "100008":
                logger.warning(
                    f"Kotak Neo returned stCode 100008 (unauthorized). "
                    f"Attempting thread-safe re-authentication for {symbol} (tag={tag})..."
                )
                TelegramAlerts.auth_failure(
                    "place_order",
                    f"Session expired while placing order for {symbol} (tag={tag}). Re-authenticating...",
                )
                try:
                    self.login()
                    existing = self.find_order_by_tag(tag) if tag else None
                    if existing:
                        logger.info(f"Order with tag {tag} already placed prior to session expiration: {existing}")
                        return existing
                    resp = _do_place()
                    # Second chance if broker still rejects auth after first refresh
                    if isinstance(resp, dict) and str(_extract_st_code(resp)) == "100008":
                        logger.warning(
                            f"place_order still unauthorized after re-login for {symbol}. "
                            f"Forcing second session refresh..."
                        )
                        time.sleep(0.5)
                        self.login()
                        existing = self.find_order_by_tag(tag) if tag else None
                        if existing:
                            return existing
                        resp = _do_place()
                    if isinstance(resp, dict) and str(_extract_st_code(resp)) == "100008":
                        err_full = (
                            f"SESSION_UNAUTHORIZED place_order failed for {symbol} after re-login "
                            f"(stCode=100008 unauthorized). Restart bot / refresh Kotak session."
                        )
                        logger.error(f"ORDER_PLACEMENT_FAILED {err_full}")
                        TelegramAlerts.critical_error("Order Placement", err_full)
                        raise RuntimeError(err_full)
                except RuntimeError:
                    raise
                except Exception as auth_ex:
                    err_full = (
                        f"SESSION_UNAUTHORIZED re-authentication failed during place_order "
                        f"for {symbol}: {auth_ex}"
                    )
                    logger.error(f"ORDER_PLACEMENT_FAILED {err_full}")
                    TelegramAlerts.critical_error("Order Placement", err_full)
                    raise RuntimeError(err_full) from auth_ex

        # Handle duplicate Client Order ID (stCode 32 or duplicate error message)
        is_dup = False
        if isinstance(resp, dict):
            st_code = _extract_st_code(resp)
            err_msg_check = str(resp.get("errMsg") or resp.get("message") or (resp.get("data") or {}).get("errMsg") or "").lower()
            if str(st_code) == "32" or "duplicate" in err_msg_check or "already exists" in err_msg_check:
                is_dup = True

        if is_dup:
            logger.warning(f"Kotak Neo returned duplicate order rejection (tag={tag}). Querying broker order book with conservative matching...")
            existing = self.find_order_by_tag(tag) if tag else None
            if existing:
                logger.info(f"RECOVERED_EXISTING_ORDER Successfully recovered broker order {existing} by tag={tag}.")
                return existing

            matched_id, match_status = self.find_matching_order(
                symbol=symbol, side=side, qty=qty, ord_type=ord_type,
                price=price, trigger_price=trigger_price, tag=tag, prod=prod
            )
            if match_status == "EXACT_MATCH" and matched_id:
                logger.info(f"RECOVERED_EXISTING_ORDER Successfully recovered unique broker order {matched_id} via multi-attribute matching.")
                return matched_id
            elif match_status == "AMBIGUOUS_MULTIPLE_MATCHES":
                err_full = f"Kotak rejected duplicate order for {symbol} (tag={tag}): multiple candidates found. Classified ambiguous to prevent double placement."
                logger.error(f"ORDER_PLACEMENT_FAILED {err_full}")
                TelegramAlerts.critical_error("Order Placement", err_full)
                raise RuntimeError(err_full)
            else:
                err_full = f"Kotak rejected duplicate order for {symbol} (tag={tag}): order was not found in broker order book."
                logger.error(f"ORDER_PLACEMENT_FAILED {err_full}")
                TelegramAlerts.critical_error("Order Placement", err_full)
                raise RuntimeError(err_full)

        # Check other non-zero broker error codes
        if isinstance(resp, dict):
            st_code = _extract_st_code(resp)
            err_msg_broker = resp.get("errMsg") or resp.get("message")
            stat = resp.get("stat")
            data_dict = resp.get("data") if isinstance(resp.get("data"), dict) else {}
            if not err_msg_broker:
                err_msg_broker = data_dict.get("errMsg")
            if not stat:
                stat = data_dict.get("stat")
            if st_code is not None:
                try:
                    st_code_int = int(st_code)
                except (ValueError, TypeError):
                    st_code_int = -1

                if st_code_int not in (0, 200):
                    err_detail = f"stCode={st_code} errMsg={err_msg_broker} stat={stat}"
                    err_full = f"Kotak rejected order for {symbol}: {err_detail}"
                    logger.error(f"ORDER_PLACEMENT_FAILED {err_full}")
                    TelegramAlerts.critical_error("Order Placement", err_full)
                    raise RuntimeError(err_full)

        ord_id = None
        if isinstance(resp, dict):
            if "nOrdNo" in resp and resp["nOrdNo"]:
                ord_id = resp["nOrdNo"]
            elif "order_id" in resp and resp["order_id"]:
                ord_id = resp["order_id"]
            elif "data" in resp and isinstance(resp["data"], dict):
                ord_id = resp["data"].get("nOrdNo") or resp["data"].get("order_id")

        if not ord_id or str(ord_id).strip() in ("", "None", "0"):
            err_msg = f"place_order failed for {symbol}: response={resp}"
            logger.error(f"ORDER_PLACEMENT_FAILED {err_msg}")
            TelegramAlerts.critical_error("Order Placement", err_msg)
            raise RuntimeError(err_msg)

        return str(ord_id).strip()

    def cancel_order(self, order_id: str) -> bool:
        """
        Cancels open order on broker or simulation.
        Inspects broker API response schema, detects API-level rejection,
        and verifies actual status on broker.
        """
        if not order_id or order_id in ("SKIPPED_QTY_0", "SKIPPED_SL_ONLY"):
            return True

        if self.dry_run:
            if order_id in self._mock_orders:
                curr_st = str(self._mock_orders[order_id].get("ordSt", "")).upper()
                if curr_st in ("TRAD", "COMPLETE", "FILLED", "EXECUTED"):
                    logger.warning(f"🧪 [SIM_CANCEL] Order {order_id} already executed ({curr_st}). Cancel rejected.")
                    return False
                self._mock_orders[order_id]["ordSt"] = "CANCELLED"
                logger.info(f"🧪 [SIM_CANCEL] Cancelled order {order_id}")
                return True
            logger.warning(f"🧪 [SIM_CANCEL] Order {order_id} not found in mock orders.")
            return False

        if not self.sdk or not hasattr(self.sdk, "cancel_order"):
            return False

        try:
            resp = self.sdk.cancel_order(order_id=str(order_id))
            if isinstance(resp, dict):
                st_code = _extract_st_code(resp)
                if str(st_code) == "100008":
                    logger.warning("cancel_order unauthorized. Re-authenticating...")
                    try:
                        self.login()
                        resp = self.sdk.cancel_order(order_id=str(order_id))
                        st_code = _extract_st_code(resp)
                    except Exception as reauth_err:
                        logger.error(f"Re-auth failed during cancel_order: {reauth_err}")
                        return False

                stat = resp.get("stat") or (resp.get("data") or {}).get("stat")
                err_msg = resp.get("errMsg") or resp.get("message") or (resp.get("data") or {}).get("errMsg")

                if stat in ("Not_Ok", "ERROR", "REJECTED") or (st_code is not None and str(st_code) not in ("0", "200")):
                    logger.error(f"cancel_order rejected by broker: stCode={st_code}, stat={stat}, msg={err_msg}")
                    return False

            st = self.get_order_status(order_id)
            if st in ("CANCELLED", "CAN"):
                return True
            elif st in ("TRAD", "COMPLETE", "FILLED", "EXECUTED"):
                logger.warning(f"cancel_order: order {order_id} already traded/complete on broker.")
                return False
            logger.warning(f"cancel_order: order {order_id} status after cancel is unconfirmed: {st}")
            return False
        except Exception as e:
            logger.error(f"Exception during cancel_order for {order_id}: {e}")
            return False

    def log_schema_diagnostics(self):
        """
        Safe opt-in diagnostic mode (ENABLE_SCHEMA_DIAGNOSTICS=true).
        Logs sanitized order metadata for representative OPEN, REJECTED, and CANCELLED statuses.
        Redacts credentials and secrets. Does not place test orders.
        """
        if not getattr(settings, "ENABLE_SCHEMA_DIAGNOSTICS", False):
            return

        orders = self.get_order_report()
        if not orders:
            logger.info("SCHEMA_DIAGNOSTICS: No orders returned from broker order report.")
            return

        status_samples = {}
        for o in orders:
            st = str(o.get("ordSt") or o.get("order_status") or o.get("status") or "").upper()
            if st and st not in status_samples:
                sanitized = {}
                for k, v in o.items():
                    k_str = str(k).lower()
                    if any(secret_k in k_str for secret_k in ("token", "secret", "pass", "pwd", "auth", "ucc", "mobile")):
                        sanitized[k] = "[REDACTED]"
                    else:
                        sanitized[k] = v
                status_samples[st] = sanitized

        logger.info(f"SCHEMA_DIAGNOSTICS Captured {len(status_samples)} representative statuses: {list(status_samples.keys())}")
        for st, sample in status_samples.items():
            logger.info(
                f"SCHEMA_DIAGNOSTIC_SAMPLE [{st}]: order_id={sample.get('nOrdNo') or sample.get('order_id')}, "
                f"tag={sample.get('tag')}, sym={sample.get('trdSym') or sample.get('symbol')}, "
                f"ordTyp={sample.get('ordTyp') or sample.get('order_type')}, "
                f"rejReason={sample.get('rejRsn') or sample.get('rejection_reason') or sample.get('errMsg')}"
            )

    def get_order_details(self, order_id: str) -> Optional[Dict[str, Any]]:
        """Returns raw order dictionary from broker order book or mock records."""
        if self.dry_run:
            return self._mock_orders.get(str(order_id))
        orders = self.get_order_report()
        if orders is None:
            return None
        for o in orders:
            if str(o.get("nOrdNo") or o.get("order_id", "")) == str(order_id):
                return o
        return None

    def get_order_history(self, order_id: str) -> Optional[List[Dict[str, Any]]]:
        """
        Fetch per-order history via SDK order_history (includes rejRsn transitions).
        Prefer this when investigating REJECTED / cancelled protective legs.
        """
        if not order_id:
            return None
        if self.dry_run:
            det = self._mock_orders.get(str(order_id))
            return [det] if det else None

        if not self.sdk or not hasattr(self.sdk, "order_history"):
            logger.warning("SDK order_history not available")
            return None
        try:
            resp = self.sdk.order_history(order_id=str(order_id))
            if isinstance(resp, dict):
                st_code = _extract_st_code(resp)
                if str(st_code) == "100008":
                    try:
                        self.login()
                        resp = self.sdk.order_history(order_id=str(order_id))
                    except Exception as reauth_err:
                        logger.error(f"Re-auth failed during order_history: {reauth_err}")
                        return None
                data = resp.get("data")
                if isinstance(data, dict) and isinstance(data.get("data"), list):
                    return data.get("data")
                if isinstance(data, list):
                    return data
                if isinstance(resp.get("data"), list):
                    return resp.get("data")
            elif isinstance(resp, list):
                return resp
            logger.warning(f"order_history unrecognized response for {order_id}: {type(resp)}")
            return None
        except Exception as e:
            logger.error(f"order_history failed for {order_id}: {e}")
            return None

    def get_order_rejection_reason(self, order_id: str) -> Optional[str]:
        """Best-effort rejection / broker message from order_report or order_history."""
        det = self.get_order_details(order_id)
        if det:
            for key in ("rejRsn", "rejection_reason", "errMsg", "ordUsrMsg", "status_message"):
                val = det.get(key)
                if val is not None and str(val).strip() and str(val).strip() not in ("--", "NA", "N/A"):
                    return str(val).strip()

        hist = self.get_order_history(order_id)
        if not hist:
            return None
        # History is usually newest-first or oldest-first; scan all for rejRsn
        for row in hist:
            if not isinstance(row, dict):
                continue
            for key in ("rejRsn", "rejection_reason", "errMsg", "ordUsrMsg"):
                val = row.get(key)
                if val is not None and str(val).strip() and str(val).strip() not in ("--", "NA", "N/A"):
                    return str(val).strip()
        return None

    def summarize_order(self, order_id: str) -> Dict[str, Any]:
        """Structured snapshot for logs / Telegram (id, qty, prices, product, status, rej)."""
        det = self.get_order_details(order_id) or {}
        hist = self.get_order_history(order_id)
        latest = hist[0] if hist and isinstance(hist[0], dict) else det
        src = latest or det
        return {
            "order_id": order_id,
            "status": str(src.get("ordSt") or src.get("status") or "").upper(),
            "symbol": src.get("trdSym") or src.get("tradingSymbol") or src.get("symbol"),
            "qty": src.get("qty") or src.get("quantity"),
            "filled_qty": src.get("fldQty") or src.get("filled_quantity"),
            "price": src.get("prc") or src.get("price"),
            "trigger_price": src.get("trgPrc") or src.get("trigger_price"),
            "product": src.get("prd") or src.get("prod") or src.get("product"),
            "order_type": src.get("ordTyp") or src.get("prcTp") or src.get("order_type"),
            "tag": src.get("tag") or src.get("GuiOrdId"),
            "rejection_reason": self.get_order_rejection_reason(order_id),
            "history_len": len(hist) if hist else 0,
        }

    def get_order_status(self, order_id: str) -> Optional[str]:
        """Returns the current status of an order on the broker."""
        det = self.get_order_details(order_id)
        if det:
            return str(det.get("ordSt") or det.get("status", "")).upper()
        return None

    def create_order_feed(self, **kwargs):
        """Creates the official SDK WebSocket order feed client (LIVE mode)."""
        if self.dry_run or not self.sdk or not hasattr(self.sdk, "create_order_feed"):
            return None
        return self.sdk.create_order_feed(**kwargs)

    def get_order_report(self) -> Optional[List[Dict[str, Any]]]:
        """
        Retrieves the broker order report.
        Returns a list of order dicts if successful (may be empty if no orders exist).
        Returns None if broker call fails, session is unauthorized, or rate-limited.
        Distinguishes genuine empty order books from broker API errors.
        """
        if self.dry_run:
            return list(self._mock_orders.values())

        if not self.sdk or not hasattr(self.sdk, "order_report"):
            return None

        with self._order_report_lock:
            try:
                resp = self.sdk.order_report()

                # Handle session expiry with thread-safe re-authentication retry
                if isinstance(resp, dict):
                    st_code = _extract_st_code(resp)
                    if str(st_code) == "100008" or "Complete the 2fa process" in str(resp.get("Error Message", "")):
                        logger.warning("order_report unauthorized/session expired. Re-authenticating...")
                        TelegramAlerts.auth_failure("order_report", "Session expired. Re-authenticating...")
                        try:
                            self.login()
                            resp = self.sdk.order_report()
                        except Exception as reauth_err:
                            logger.error(f"Re-auth failed during order_report: {reauth_err}")
                            return None

                if isinstance(resp, list):
                    return resp

                if isinstance(resp, dict):
                    # Check for other error codes
                    st_code = _extract_st_code(resp)
                    if st_code is not None:
                        try:
                            st_code_int = int(st_code)
                            if st_code_int not in (0, 200):
                                logger.error(f"Broker returned error in order_report: stCode={st_code}, resp={resp}")
                                return None
                        except (ValueError, TypeError):
                            pass

                    if "Error" in resp or "Error Message" in resp:
                        logger.error(f"Broker order_report error: {resp}")
                        return None

                    if "data" in resp:
                        data = resp.get("data")
                        if isinstance(data, list):
                            return data
                        elif data is None:
                            return []

                    # If dict doesn't contain data or order IDs, check if unrecognized error
                    if not any(k in resp for k in ("nOrdNo", "order_id", "data")):
                        logger.warning(f"order_report returned unrecognized dict: {resp}")
                        return None

                return None
            except Exception as e:
                logger.error(f"Failed to fetch order report from broker: {e}")
                return None

    def simulate_entry_fill(self, symbol: str, side: OrderSide, qty: int, price: float) -> str:
        """Injects a fully-executed manual entry order into the mock broker (DRY_RUN only)."""
        import uuid, time as _time
        order_id = f"ENTRY-{uuid.uuid4().hex[:6].upper()}"
        self._mock_orders[order_id] = {
            "nOrdNo": order_id, "order_id": order_id,
            "trdSym": symbol,
            "trnsTp": "B" if side == OrderSide.BUY else "S",
            "qty": qty, "fldQty": qty, "ordTyp": "L",
            "prc": price, "avgPrc": price, "trgPrc": 0.0,
            "ordSt": "TRAD", "tag": "MANUAL_ENTRY", "time": _time.time()
        }
        logger.info(f"🧪 [INJECT] Simulated entry fill: {qty}x {symbol} ({side.value}) @ ₹{price:.2f} (ID: {order_id})")
        return order_id

    def set_mock_orders(self, orders: Optional[List[Dict[str, Any]]]):
        """Injects simulated mock orders for offline tests (DRY_RUN only)."""
        if orders is None:
            self._mock_orders = {}
        else:
            self._mock_orders = {str(o.get("nOrdNo") or o.get("order_id", i)): dict(o) for i, o in enumerate(orders)}

