import os
import threading
import time
from flask import Flask, jsonify
from config import (
    ENABLE_BUZZHEAVIER,
    ENABLE_STRUCTURED_LOGGING,
    LOG_LEVEL,
)
from utils.logging_config import setup_structured_logging, print_banner, get_modern_logger

from api.routes import api as core_api
from api.routes import init_recovery_on_startup, register_signal_handlers, init_routes_services
from api.tokenroutes import token_api
from api.new_photo_routes import new_photo_api, recover_pending_new_photo_tasks
from api.pd_lost import pd_lost_api, recover_pd_lost_on_startup, periodic_pd_lost_enqueue
from api.metrics import metrics_api
from plugins.database_factory import init_database
from plugins.linkchecker import LinkChecker


def _start_background_threads() -> None:
    """Start long-running background services."""

    def start_link_checker():
        try:
            checker = LinkChecker()
            checker.run()
        except Exception as e:
            print(f"[ERROR] Link checker failed to start: {e}")
            # Don't crash the entire app if link checker fails

    def start_new_photo_recovery():
        time.sleep(2)
        recover_pending_new_photo_tasks()

    def start_pd_lost_recovery():
        recover_pd_lost_on_startup()

    def start_periodic_pd_lost():
        """Periodically enqueue remaining pd_lost tasks."""
        time.sleep(60)  # Initial delay
        while True:
            try:
                periodic_pd_lost_enqueue()
                time.sleep(300)  # Check every 5 minutes
            except Exception as e:
                print(f"[ERROR] Periodic pd_lost thread: {e}")
                time.sleep(60)  # Back off on error

    # Link checker
    link_checker_thread = threading.Thread(target=start_link_checker, daemon=True)
    link_checker_thread.start()

    # New photo recovery
    recovery_thread = threading.Thread(target=start_new_photo_recovery, daemon=True)
    recovery_thread.start()

    # PD lost recovery
    pd_lost_thread = threading.Thread(target=start_pd_lost_recovery, daemon=True)
    pd_lost_thread.start()
    
    # Periodic pd_lost enqueue
    periodic_pd_lost_thread = threading.Thread(target=start_periodic_pd_lost, daemon=True)
    periodic_pd_lost_thread.start()


def create_app(config: dict | None = None, enable_background: bool | None = None, register_signals: bool | None = None) -> Flask:
    """Application factory to create a configured Flask app.

    Args:
        config: Optional mapping to update app.config.
        enable_background: Override env to start background threads. Defaults to env ENABLE_BACKGROUND_THREADS (true).
        register_signals: Register SIGINT/SIGTERM handlers. Defaults to env REGISTER_SIGNALS (false).
    """
    # Setup modern Rich logging
    enable_structured = ENABLE_STRUCTURED_LOGGING
    log_level = LOG_LEVEL
    setup_structured_logging(level=log_level, enable_structured=enable_structured)
    
    # Print beautiful startup banner
    print_banner(
        "🚀 GCloud File Processor", 
        "Modern file processing system with Redis queue and multiple cloud providers"
    )
    
    app = Flask(__name__)

    if config:
        app.config.update(config)

    # Register API blueprints
    app.register_blueprint(core_api, url_prefix='/api')
    app.register_blueprint(token_api)
    app.register_blueprint(new_photo_api, url_prefix='/api')
    app.register_blueprint(pd_lost_api, url_prefix='/api')
    app.register_blueprint(metrics_api, url_prefix='/api')

    # Initialize database
    db = init_database()
    
    # Initialize route services
    init_routes_services()

    @app.route('/')
    def api_status():
        try:
            db_status = "connected" if db.ping() else "disconnected"
            services = {
                'pixeldrain': 'enabled' if os.getenv('ENABLE_PIXELDRAIN', 'true').lower() == 'true' else 'disabled',
                'buzzheavier': 'enabled' if ENABLE_BUZZHEAVIER else 'disabled',
                'google_photos': 'enabled' if os.getenv('ENABLE_GPHOTOS', 'true').lower() == 'true' else 'disabled',
                'new_google_photos': 'enabled' if os.getenv('NEW_ENABLE_GPHOTOS', 'false').lower() == 'true' else 'disabled',
            }
            return jsonify({
                'status': 'running',
                'version': 'v2',
                'timestamp': time.time(),
                'uptime': 'active',
                'database': db_status,
                'services': services,
                'message': 'GCloud Master API is running successfully'
            })
        except Exception as e:
            return jsonify({
                'status': 'error',
                'version': 'v2',
                'timestamp': time.time(),
                'error': str(e),
                'message': 'API is running but encountered an error'
            }), 500

    @app.route('/health')
    def health_check():
        return jsonify({'status': 'healthy', 'version': 'v2', 'timestamp': time.time()})

    # Initialize recovery from core api module
    init_recovery_on_startup()

    # Background services
    if enable_background is None:
        enable_background = os.getenv('ENABLE_BACKGROUND_THREADS', 'true').lower() == 'true'
    if enable_background:
        # Start Redis worker loop
        from queueing.redis_worker import start_redis_worker
        redis_worker_thread = threading.Thread(target=start_redis_worker, daemon=True)
        redis_worker_thread.start()
        # Only show worker started message once per process
        if not hasattr(start_redis_worker, '_worker_started_shown'):
            from rich.console import Console
            from rich.text import Text
            console = Console()
            
            worker_text = Text()
            worker_text.append("✅ ", style="bold bright_green")
            worker_text.append("Redis queue worker ", style="bold bright_blue")
            worker_text.append("started", style="bold bright_green")
            console.print(worker_text)
            start_redis_worker._worker_started_shown = True
        _start_background_threads()

    # Signal handlers (usually off in gunicorn)
    if register_signals is None:
        register_signals = os.getenv('REGISTER_SIGNALS', 'false').lower() == 'true'
    if register_signals:
        register_signal_handlers()

    return app


