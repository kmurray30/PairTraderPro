"""
Terminal Color Utilities for Logging

Provides ANSI color codes and helper functions for colored terminal output.
Used to add visual importance hierarchy to algo logs:
    - Grey: High-frequency data (every poll)
    - White: Normal events (state changes, orders)
    - Yellow: Warnings
    - Red: Errors and critical events
"""

from datetime import datetime
from typing import Any

# ANSI color codes
GREY = '\033[90m'
WHITE = '\033[97m'
YELLOW = '\033[93m'
RED = '\033[91m'
RESET = '\033[0m'

# Additional colors for potential use
CYAN = '\033[96m'
GREEN = '\033[92m'
MAGENTA = '\033[95m'


def print_grey(message: str, timestamp: bool = True) -> None:
    """
    Print message in grey (for high-frequency data).
    
    Args:
        message: Message to print
        timestamp: Whether to prepend timestamp
    """
    if timestamp:
        time_str = datetime.now().strftime("%H:%M:%S")
        print(f"{GREY}{time_str} | {message}{RESET}")
    else:
        print(f"{GREY}{message}{RESET}")


def print_white(message: str, timestamp: bool = True) -> None:
    """
    Print message in white (for normal events).
    
    Args:
        message: Message to print
        timestamp: Whether to prepend timestamp
    """
    if timestamp:
        time_str = datetime.now().strftime("%H:%M:%S")
        print(f"{WHITE}{time_str} | {message}{RESET}")
    else:
        print(f"{WHITE}{message}{RESET}")


def print_yellow(message: str, timestamp: bool = True) -> None:
    """
    Print message in yellow (for warnings).
    
    Args:
        message: Message to print
        timestamp: Whether to prepend timestamp
    """
    if timestamp:
        time_str = datetime.now().strftime("%H:%M:%S")
        print(f"{YELLOW}{time_str} | {message}{RESET}")
    else:
        print(f"{YELLOW}{message}{RESET}")


def print_red(message: str, timestamp: bool = True) -> None:
    """
    Print message in red (for errors/critical).
    
    Args:
        message: Message to print
        timestamp: Whether to prepend timestamp
    """
    if timestamp:
        time_str = datetime.now().strftime("%H:%M:%S")
        print(f"{RED}{time_str} | {message}{RESET}")
    else:
        print(f"{RED}{message}{RESET}")


def format_currency(value: float) -> str:
    """Format value as currency with comma separators."""
    return f"${value:,.2f}"


def format_percent(value: float, decimals: int = 3) -> str:
    """Format value as percentage with sign."""
    return f"{value:+.{decimals}f}%"


def format_ratio(value: float, decimals: int = 5) -> str:
    """Format ratio value with fixed decimals."""
    return f"{value:.{decimals}f}"

