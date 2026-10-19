"""
FastAPI Application Factory & Production Runtime Entrypoint.
Registers:
  - Startup/shutdown lifecycle (FastAPI lifespan)
  - Single-instance OS process lock
  - Background async workers via WorkerCoordinator (WebSocket, REST poller, Reconciler, Heartbeat)
  - API routes with prefix /api and root /health endpoint
  - Custom OpenAPI metadata
"""

import logging
import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import JSONResponse

import os
from pathlib import Path
from .config import settings, BASE_DIR
from .engine import engine
from .routes import router
from .workers import coordinator, order_feed
from .notifier import TelegramAlerts


class ProcessLock:
    """Acquires an exclusive OS-level file lock on a PID file to prevent duplicate bot instances."""

    def __init__(self, lock_file: Path = None):
        self.lock_file = Path(lock_file or (BASE_DIR / "data" / "trading_bot.lock"))
        self._fd = None
        self._locked = False

    def acquire(self) -> bool:
        if self._locked:
            return True
        self.lock_file.parent.mkdir(parents=True, exist_ok=True)
        try:
            import fcntl
            self._fd = open(self.lock_file, "a+")
            try:
                fcntl.flock(self._fd.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except (IOError, OSError) as e:
                self._fd.seek(0)
                pid = self._fd.read().strip()
                self._fd.close()
                self._fd = None
                raise RuntimeError(
                    f"Another bot instance is already running (PID: {pid or 'unknown'})."
                ) from e
            self._fd.seek(0)
            self._fd.truncate()
            self._fd.write(f"{os.getpid()}\n")
            self._fd.flush()
            self._locked = True
            logger.info(f"🔒 Acquired exclusive process lock at {self.lock_file} (PID: {os.getpid()})")
            return True
        except ImportError:
            try:
                import msvcrt
                self._fd = open(self.lock_file, "r+") if self.lock_file.exists() else open(self.lock_file, "w+")
                try:
                    msvcrt.locking(self._fd.fileno(), msvcrt.LK_NBLCK, 1)
                except (IOError, OSError) as e:
                    self._fd.close()
                    self._fd = None
                    raise RuntimeError("Another bot instance is already running on this system.") from e
                self._fd.seek(0)
                self._fd.truncate()
                self._fd.write(f"{os.getpid()}\n")
                self._fd.flush()
                self._locked = True
                logger.info(f"🔒 Acquired exclusive process lock at {self.lock_file} (PID: {os.getpid()})")
                return True
            except Exception as ex:
                logger.warning(f"Could not engage Windows file lock ({ex}); continuing without lock.")
                self._locked = True
                return True

    def release(self):
        if not self._locked:
            return
        try:
            if self._fd is not None:
                try:
                    import fcntl
                    fcntl.flock(self._fd.fileno(), fcntl.LOCK_UN)
                except (ImportError, OSError):
                    try:
                        import msvcrt
                        self._fd.seek(0)
                        msvcrt.locking(self._fd.fileno(), msvcrt.LK_UNLCK, 1)
                    except Exception:
                        pass
                self._fd.close()
                self._fd = None
            if self.lock_file.exists():
                try:
                    self.lock_file.unlink()
                except OSError:
                    pass
            logger.info("🔓 Released process lock.")
        except Exception as e:
            logger.warning(f"Error releasing process lock: {e}")
        finally:
            self._locked = False


process_lock = ProcessLock()

# Configure logging
logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL, logging.INFO),
    format="[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("FastAPIBot.App")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Production FastAPI lifespan context:
      STARTUP  → Single instance lock → Initialize broker & engine → Launch background workers → READY
      SHUTDOWN → Stop workers cleanly → Flush state & broker resources → Release lock → Clean exit
    """
    logger.info("=" * 60)
    logger.info("  KOTAK NEO F&O FASTAPI TRADING BOT STARTING")
    logger.info(f"  Mode: {'DRY_RUN' if settings.DRY_RUN else 'LIVE'}")
    logger.info("  Primary detector: WebSocket Order & Position Feed")
    logger.info(f"  Backup detector:  REST Poller ({settings.REST_BACKUP_POLL_INTERVAL_SEC:.1f}s)")
    logger.info(f"  Reconciliation:   3-Way Audit ({settings.RECONCILIATION_INTERVAL_SEC:.0f}s)")
    logger.info("=" * 60)

    # 1. Acquire single-instance process lock
    try:
        process_lock.acquire()
    except RuntimeError as ex:
        logger.critical(f"FATAL: Process lock acquisition failed: {ex}")
        TelegramAlerts.critical_error("Startup Failure", str(ex))
        raise

    # 2. Boot trading engine (auth + recovery + startup reconciliation)
    try:
        engine.startup()
    except Exception as ex:
        logger.critical(f"FATAL: TradingEngine startup failed: {ex}", exc_info=True)
        TelegramAlerts.critical_error("Engine Startup", f"Fatal initialization failure: {ex}")
        process_lock.release()
        raise

    # 3. Launch persistent background workers through WorkerCoordinator
    # (Guarantees exactly one task each: WebSocket, REST poller, Reconciler, Heartbeat)
    await coordinator.start_all()
    logger.info("✅ Application startup complete. Bot is READY and accepting traffic.")

    yield  # ← app is live and running here

    # 4. Graceful shutdown sequence on SIGTERM / SIGINT
    logger.info("🛑 Graceful shutdown initiated...")
    try:
        await coordinator.stop_all(timeout=10.0)
    except Exception as e:
        logger.warning(f"Error during coordinator shutdown: {e}")

    try:
        engine.shutdown()
    except Exception as e:
        logger.warning(f"Error during engine shutdown: {e}")

    process_lock.release()
    TelegramAlerts.send("🛑 <b>Kotak Neo FastAPI Bot Stopped</b>")
    logger.info("✅ Bot shutdown complete. Clean exit.")


# Build the FastAPI application
app = FastAPI(
    title="Kotak Neo NIFTY 50 F&O Trading Bot",
    description=(
        "Automated order-management bot for NIFTY 50 F&O (tested against a simulated broker, pending live Kotak validation).\n\n"
        "**Features:**\n"
        "- Detects manual entry limit order fills via WebSocket (Primary) and REST (Backup)\n"
        "- Places 50/50 Target (+₹1) and Stop-Loss (-₹1 with ₹10 buffer) orders\n"
        "- Active OCO: cancels companion order when one leg executes\n"
        "- Handles BUY (Long) and SELL (Short) entry positions\n"
        "- Handles partial fills (AGGREGATED or IMMEDIATE mode)\n"
        "- Idempotent: duplicate events never create duplicate orders\n"
        "- 3-way reconciliation every 15 seconds\n"
        "- Restart recovery from JSON persistence\n"
        "- Real-time Telegram alerts with secret masking\n"
    ),
    version="2.0.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc"
)

# Mount root health check endpoint directly
@app.get("/health", tags=["Health"], summary="System Health Check")
def root_health():
    """Reports health, connection states, and worker timestamps."""
    return engine.get_health()

# Mount API routes
app.include_router(router, prefix="/api", tags=["Trading Bot"])


@app.get("/", include_in_schema=False)
def root():
    return JSONResponse({
        "service": "Kotak Neo NIFTY 50 F&O Trading Bot (FastAPI)",
        "mode": "DRY_RUN" if settings.DRY_RUN else "LIVE",
        "docs": "/docs",
        "health": "/health",
        "status": "/api/status"
    })
