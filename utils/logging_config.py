import logging
import os
import time
import uuid
from datetime import datetime
from contextvars import ContextVar
from typing import Any, Dict, Optional
import threading

from rich.console import Console
from rich.logging import RichHandler
from rich.text import Text
from rich.panel import Panel
from rich.columns import Columns
from rich.table import Table
from rich import box
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TimeElapsedColumn

# Initialize Rich console with colors disabled
console = Console(width=120, force_terminal=False, markup=False, highlight=False, color_system=None)

# Thread-local storage for correlation IDs (keeping existing compatibility)
_correlation_context = threading.local()

class ModernRichHandler(RichHandler):
    """Custom Rich handler with modern styling"""
    
    def __init__(self, *args, **kwargs):
        super().__init__(
            console=console,
            show_time=True,
            show_level=True,
            show_path=False,
            markup=False,
            rich_tracebacks=False,
            tracebacks_show_locals=False,
            *args, **kwargs
        )
    
    def format(self, record):
        # Remove correlation ID formatting to keep logs clean
        return super().format(record)

def setup_structured_logging(level: str = "INFO", enable_structured: bool = True) -> None:
    """Setup modern Rich logging configuration"""
    log_level = getattr(logging, level.upper())
    
    # Setup root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    
    # Clear existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Create Rich handler
    rich_handler = ModernRichHandler(level=log_level)
    root_logger.addHandler(rich_handler)
    
    # Reduce noise from external libraries
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('requests').setLevel(logging.WARNING)
    logging.getLogger('werkzeug').setLevel(logging.WARNING)
    logging.getLogger('asyncio').setLevel(logging.WARNING)

def get_metrics_logger():
    """Get or create a metrics logger with Rich formatting."""
    metrics_logger = logging.getLogger('metrics_rich')
    
    # Only setup once
    if not metrics_logger.handlers:
        metrics_logger.setLevel(logging.INFO)
        
        # Create Rich handler for metrics data
        handler = ModernRichHandler()
        metrics_logger.addHandler(handler)
        
        # Prevent propagation to avoid showing in main logs
        metrics_logger.propagate = False
    
    return metrics_logger

def set_correlation_id(correlation_id: Optional[str] = None) -> str:
    """Set correlation ID for current thread context."""
    if correlation_id is None:
        correlation_id = str(uuid.uuid4())[:8]  # Short correlation ID
    _correlation_context.correlation_id = correlation_id
    return correlation_id

def get_correlation_id() -> Optional[str]:
    """Get correlation ID for current thread."""
    return getattr(_correlation_context, 'correlation_id', None)

def clear_correlation_id() -> None:
    """Clear correlation ID for current thread."""
    if hasattr(_correlation_context, 'correlation_id'):
        delattr(_correlation_context, 'correlation_id')

class LogContext:
    """Context manager for logging with correlation ID."""
    
    def __init__(self, correlation_id: Optional[str] = None, **kwargs):
        self.correlation_id = correlation_id
        self.extra_fields = kwargs
        self.old_correlation_id = None
        
    def __enter__(self):
        self.old_correlation_id = get_correlation_id()
        set_correlation_id(self.correlation_id)
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.old_correlation_id:
            set_correlation_id(self.old_correlation_id)
        else:
            clear_correlation_id()

# Performance tracking decorator with Rich styling
def track_performance(operation_name: str):
    """Decorator to track operation performance with Rich styling"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            start_time = time.time()
            correlation_id = get_correlation_id() or str(uuid.uuid4())[:8]
            logger = logging.getLogger(func.__module__)
            
            logger.info(f"🚀 Starting {operation_name}")
            
            try:
                result = func(*args, **kwargs)
                duration = time.time() - start_time
                logger.info(f"✅ Completed {operation_name} in {duration:.2f}s")
                return result
                
            except Exception as e:
                duration = time.time() - start_time
                logger.error(f"❌ Failed {operation_name} after {duration:.2f}s: {str(e)}")
                raise
                
        return wrapper
    return decorator

# Modern logging utilities
class ModernLogger:
    """Modern logger with Rich styling and emojis"""
    
    def __init__(self, name: str):
        self.logger = logging.getLogger(name)
        self.console = console
    
    def success(self, message: str, **kwargs):
        """Log success message with bright green styling"""
        self.logger.info(f"✅ {message}", **kwargs)
    
    def error(self, message: str, **kwargs):
        """Log error message with bright red styling"""
        self.logger.error(f"❌ {message}", **kwargs)
    
    def warning(self, message: str, **kwargs):
        """Log warning message with bright yellow styling"""
        self.logger.warning(f"⚠️ {message}", **kwargs)
    
    def info(self, message: str, **kwargs):
        """Log info message with bright blue styling"""
        self.logger.info(f"{message}", **kwargs)
    
    def debug(self, message: str, **kwargs):
        """Log debug message with dim cyan styling"""
        self.logger.debug(f"🔧 {message}", **kwargs)
    
    def process_start(self, task_name: str, token: str = None):
        """Log process start with modern styling"""
        if token:
            self.logger.info(f"🚀 Starting {task_name} ({token[:8]}...)")
        else:
            self.logger.info(f"🚀 Starting {task_name}")
    
    def process_complete(self, task_name: str, token: str = None, duration: float = None):
        """Log process completion with modern styling"""
        duration_text = f" in {duration:.2f}s" if duration else ""
        if token:
            self.logger.info(f"✅ Completed {task_name} ({token[:8]}...){duration_text}")
        else:
            self.logger.info(f"✅ Completed {task_name}{duration_text}")
    
    def upload_start(self, service: str, filename: str):
        """Log upload start"""
        self.logger.info(f"📤 Uploading to {service}: {filename}")
    
    def upload_success(self, service: str, file_id: str):
        """Log upload success"""
        self.logger.info(f"✅ {service} upload completed: {file_id}")
    
    def upload_failed(self, service: str, error: str):
        """Log upload failure"""
        self.logger.error(f"❌ {service} upload failed: {error}")
    
    def skip(self, service: str, reason: str):
        """Log service skip"""
        self.logger.info(f"⏭️ {service} skipped: {reason}")

def get_modern_logger(name: str) -> ModernLogger:
    """Get a modern logger instance"""
    return ModernLogger(name)

# Rich console utilities
def print_banner(title: str, subtitle: str = None):
    """Print a beautiful enhanced banner (only once per process)"""
    # Only show banner once per process to avoid Flask debug mode duplicates
    if hasattr(print_banner, '_banner_shown'):
        return
    print_banner._banner_shown = True
    
    # Create gradient-like title with multiple colors
    text = Text()
    text.append("🚀 ")
    text.append("GCloud ")
    text.append("File ")
    text.append("Processor")
    
    if subtitle:
        text.append(f"\n{subtitle}")
    
    panel = Panel(
        text,
        box=box.DOUBLE_EDGE,
        padding=(1, 3),
        style="bold bright_blue",
        border_style="bright_magenta"
    )
    console.print(panel)

def print_status_table(stats: dict):
    """Print a beautiful status table"""
    table = Table(title="Queue Status", box=box.ROUNDED, show_header=True)
    table.add_column("Job Type", style="cyan", no_wrap=True)
    table.add_column("Queued", style="yellow", justify="right")
    table.add_column("Processing", style="green", justify="right")
    table.add_column("Workers", style="blue", justify="right")
    
    for job_type, data in stats.items():
        if job_type != 'overall':
            queued = str(data.get('queued', 0))
            processing = str(data.get('processing', 0))
            workers = f"{processing}/{data.get('max_workers', 0)}"
            table.add_row(job_type, queued, processing, workers)
    
    console.print(table)