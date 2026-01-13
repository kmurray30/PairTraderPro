"""
Terminal Color Utilities - DEPRECATED

⚠️  DEPRECATED: This module is deprecated in favor of the centralized logger.py

Use the centralized logger instead:
    from .logger import logger
    logger.error("message")    # Red
    logger.warning("message")  # Yellow
    logger.info("message")     # White
    logger.verbose("message")  # Gray
    logger.debug("message")    # Cyan

This file is kept for color constant references only.
All print_* functions should no longer be used.
"""

from datetime import datetime
from typing import Any

# ANSI color codes - kept for reference
GREY = '\033[90m'
WHITE = '\033[97m'
YELLOW = '\033[93m'
RED = '\033[91m'
RESET = '\033[0m'

# Additional colors
CYAN = '\033[96m'
GREEN = '\033[92m'
MAGENTA = '\033[95m'


# ============================================================================
# DEPRECATED FUNCTIONS - DO NOT USE
# Use logger.py instead
# ============================================================================

def print_grey(message: str, timestamp: bool = True) -> None:
    """DEPRECATED: Use logger.verbose() instead."""
    raise DeprecationWarning("print_grey() is deprecated. Use logger.verbose() instead.")


def print_white(message: str, timestamp: bool = True) -> None:
    """DEPRECATED: Use logger.info() instead."""
    raise DeprecationWarning("print_white() is deprecated. Use logger.info() instead.")


def print_yellow(message: str, timestamp: bool = True) -> None:
    """DEPRECATED: Use logger.warning() instead."""
    raise DeprecationWarning("print_yellow() is deprecated. Use logger.warning() instead.")


def print_red(message: str, timestamp: bool = True) -> None:
    """DEPRECATED: Use logger.error() instead."""
    raise DeprecationWarning("print_red() is deprecated. Use logger.error() instead.")


# ============================================================================
# Formatting utilities - kept for backward compatibility
# ============================================================================

def format_currency(value: float) -> str:
    """Format value as currency with comma separators."""
    return f"${value:,.2f}"


def format_percent(value: float, decimals: int = 3) -> str:
    """Format value as percentage with sign."""
    return f"{value:+.{decimals}f}%"


def format_ratio(value: float, decimals: int = 5) -> str:
    """Format ratio value with fixed decimals."""
    return f"{value:.{decimals}f}"

