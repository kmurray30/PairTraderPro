"""
Centralized Logging System

Provides a unified Logger class with semantic log levels and automatic color-coding.
This replaces scattered print() and print_color() calls throughout the codebase.

Log Levels (priority order):
    ERROR (0)   - Red     - Critical failures, frozen state
    WARNING (1) - Yellow  - Recoverable issues, important alerts
    INFO (2)    - White   - Normal events, state changes, trades
    VERBOSE (3) - Gray    - High-frequency data (every poll)
    DEBUG (4)   - Cyan    - Detailed diagnostics (optional)

Usage:
    from .logger import logger
    
    logger.error("Order rejected")
    logger.warning("Insufficient buying power")
    logger.info("Trade executed")
    logger.verbose("Quote fetched")
    logger.debug("Internal state check")
"""

from datetime import datetime
from typing import Optional

# ANSI color codes
RED = '\033[91m'
YELLOW = '\033[93m'
WHITE = '\033[97m'
GRAY = '\033[90m'
CYAN = '\033[96m'
RESET = '\033[0m'


class Logger:
    """
    Centralized logger with semantic log levels and color-coding.
    
    Attributes:
        level: Current log level threshold (0-4)
        show_timestamp: Whether to prepend timestamps
    """
    
    # Log level constants
    ERROR = 0
    WARNING = 1
    INFO = 2
    VERBOSE = 3
    DEBUG = 4
    
    # Level names for display
    LEVEL_NAMES = {
        ERROR: "ERROR",
        WARNING: "WARNING",
        INFO: "INFO",
        VERBOSE: "VERBOSE",
        DEBUG: "DEBUG"
    }
    
    # Color mapping
    LEVEL_COLORS = {
        ERROR: RED,
        WARNING: YELLOW,
        INFO: WHITE,
        VERBOSE: GRAY,
        DEBUG: CYAN
    }
    
    def __init__(self, level: int = INFO, show_timestamp: bool = True):
        """
        Initialize the logger.
        
        Args:
            level: Minimum log level to display (0=ERROR, 4=DEBUG)
            show_timestamp: Whether to show timestamps in output
        """
        self.level = level
        self.show_timestamp = show_timestamp
    
    def set_level(self, level: int) -> None:
        """Change the log level threshold."""
        self.level = level
    
    def _log(self, level: int, message: str, prefix: Optional[str] = None) -> None:
        """
        Internal logging method.
        
        Args:
            level: Log level of this message
            message: Message to log
            prefix: Optional prefix (emoji, label, etc.)
        """
        # Check if this message should be displayed
        if level > self.level:
            return
        
        # Build the log line
        color = self.LEVEL_COLORS.get(level, WHITE)
        parts = []
        
        # Add timestamp if enabled
        if self.show_timestamp:
            timestamp = datetime.now().strftime("%H:%M:%S")
            parts.append(timestamp)
        
        # Add prefix if provided
        if prefix:
            parts.append(prefix)
        
        # Add message
        parts.append(message)
        
        # Join and print with color
        log_line = " | ".join(parts) if len(parts) > 1 else message
        print(f"{color}{log_line}{RESET}")
    
    def error(self, message: str) -> None:
        """
        Log an ERROR message (red).
        
        Use for: Critical failures, frozen state, order rejections
        """
        self._log(self.ERROR, message, "❌")
    
    def warning(self, message: str) -> None:
        """
        Log a WARNING message (yellow).
        
        Use for: Recoverable issues, insufficient buying power, swap cutoff
        """
        self._log(self.WARNING, message, "⚠️")
    
    def info(self, message: str) -> None:
        """
        Log an INFO message (white).
        
        Use for: Normal events, state changes, trades, heartbeat
        """
        self._log(self.INFO, message)
    
    def verbose(self, message: str) -> None:
        """
        Log a VERBOSE message (gray).
        
        Use for: High-frequency data, quote fetches, metrics, every-poll status
        """
        self._log(self.VERBOSE, message)
    
    def debug(self, message: str) -> None:
        """
        Log a DEBUG message (cyan).
        
        Use for: Detailed diagnostics, internal state checks
        """
        self._log(self.DEBUG, message, "🔍")


# Singleton instance - import this throughout the codebase
logger = Logger(level=Logger.INFO)


def set_log_level_from_config(level_value: int) -> None:
    """
    Set the global log level from configuration.
    
    Args:
        level_value: Log level (0=ERROR, 1=WARNING, 2=INFO, 3=VERBOSE, 4=DEBUG)
    """
    logger.set_level(level_value)

