"""
Sell Counter Persistence Module

This module manages the daily sell counter to prevent Good Faith Violations (GFV).
The counter tracks the number of SELL operations performed each trading day and
persists this count to disk to survive app crashes and restarts.

Good Faith Violation Prevention:
    - Cash from a sale settles T+1 (next business day)
    - Buying with unsettled sale proceeds is allowed (limited margin)
    - Selling a position bought with unsettled funds triggers GFV
    - Solution: Limit sells per day to prevent selling unsettled positions

Counter Persistence:
    - File location: live_trading/state/sell_counter.txt
    - File format: 
        Line 1: YYYY-MM-DD (trading date)
        Line 2: <count> (number of sells)
    - Counter resets automatically on new trading day
    - Write-before-execute: Counter persisted BEFORE sell executes

Safety Features:
    - Validation: File write is validated by reading back immediately
    - Fail-safe: If validation fails, raises RuntimeError (app enters ERROR)
    - Atomic operation: Counter increment and persistence are atomic

Usage:
    # On startup
    manager = SellCounterManager()
    sells_today = manager.load_counter()
    
    # Before each sell
    if not manager.can_sell(limit=1):
        # Block sell - limit reached
        return
    
    # Before executing sell order
    new_count = manager.increment_and_persist()
    
    # Then execute sell order
    api.orders.place_order(...)
"""

from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Optional

# Import centralized logger
from .logger import logger


# Eastern timezone for trading dates
EASTERN_TZ = ZoneInfo("America/New_York")

# Sell counter file location
COUNTER_FILE = Path(__file__).parent / "state" / "sell_counter.txt"


class SellCounterManager:
    """
    Manages daily sell counter with file persistence.
    
    This class provides methods to:
        - Load counter on startup (with automatic daily reset)
        - Check if sells are allowed (under limit)
        - Increment and persist counter before sell execution
        - Validate file writes for safety
    
    The counter is stored in-memory and synced to disk before each sell.
    
    Attributes:
        sells_today: Current sell count for today
        current_date: Current trading date (YYYY-MM-DD in Eastern Time)
    """
    
    def __init__(self):
        """Initialize the sell counter manager."""
        self.sells_today = 0
        self.current_date = self._get_current_date()
        
        # Ensure state directory exists
        COUNTER_FILE.parent.mkdir(parents=True, exist_ok=True)
    
    def _get_current_date(self) -> str:
        """
        Get current trading date in Eastern Time.
        
        Returns:
            Date string in YYYY-MM-DD format
        """
        now_eastern_time = datetime.now(EASTERN_TZ)
        return now_eastern_time.strftime("%Y-%m-%d")
    
    def load_counter(self) -> int:
        """
        Load sell counter from file on startup.
        
        This method reads the persisted counter from disk. If the file doesn't
        exist or contains a stale date (not today), returns 0 (fresh start).
        
        Returns:
            Sell count for today (0 if file missing or stale)
        
        Example:
            >>> manager = SellCounterManager()
            >>> sells_today = manager.load_counter()
            >>> print(f"Sells today: {sells_today}")
        """
        if not COUNTER_FILE.exists():
            logger.info("Sell counter file not found - starting fresh (sells_today=0)")
            self.sells_today = 0
            self.current_date = self._get_current_date()
            return 0
        
        try:
            content = COUNTER_FILE.read_text().strip()
            lines = content.split('\n')
            
            if len(lines) < 2:
                logger.warning(f"Invalid sell counter file format - starting fresh")
                self.sells_today = 0
                self.current_date = self._get_current_date()
                return 0
            
            file_date = lines[0].strip()
            counter_value = int(lines[1].strip())
            
            today = self._get_current_date()
            
            if file_date == today:
                # Counter is current - load it
                logger.info(f"Loaded sell counter: {counter_value} sells on {file_date}")
                self.sells_today = counter_value
                self.current_date = file_date
                return counter_value
            else:
                # Counter is stale (old date) - start fresh
                logger.info(
                    f"Sell counter is stale (file date: {file_date}, today: {today}) - "
                    f"starting fresh (sells_today=0)"
                )
                self.sells_today = 0
                self.current_date = today
                return 0
        
        except Exception as exception:
            logger.error(f"Error loading sell counter: {exception}")
            logger.warning("Starting with sells_today=0")
            self.sells_today = 0
            self.current_date = self._get_current_date()
            return 0
    
    def can_sell(self, limit: int) -> bool:
        """
        Check if a sell is allowed under the daily limit.
        
        Args:
            limit: Maximum sells allowed per day
        
        Returns:
            True if under limit (sell allowed), False if at/over limit
        
        Example:
            >>> if manager.can_sell(limit=1):
            ...     # Proceed with sell
            ...     manager.increment_and_persist()
            ...     execute_sell_order()
            ... else:
            ...     # Block sell - limit reached
            ...     logger.warning("Daily sell limit reached")
        """
        # Check if we need to reset for new day
        today = self._get_current_date()
        if today != self.current_date:
            logger.info(f"New trading day detected: {self.current_date} -> {today}")
            logger.info("Resetting sell counter to 0")
            self.sells_today = 0
            self.current_date = today
        
        allowed = self.sells_today < limit
        
        if allowed:
            logger.verbose(f"Sell allowed: {self.sells_today}/{limit} sells today")
        else:
            logger.warning(f"Sell BLOCKED: {self.sells_today}/{limit} sells today (limit reached)")
        
        return allowed
    
    def increment_and_persist(self) -> int:
        """
        Increment sell counter and persist to file with validation.
        
        This method MUST be called BEFORE executing the sell order to ensure
        the counter is persisted before the trade happens. This prevents
        Good Faith Violations if the app crashes after the sell.
        
        Steps:
            1. Increment in-memory counter
            2. Write to file with current date
            3. Validate by reading back the file
            4. If validation fails, raise RuntimeError (CRITICAL)
            5. Return new counter value
        
        Returns:
            New sell counter value after increment
        
        Raises:
            RuntimeError: If file write validation fails (CRITICAL - prevents sell)
        
        Example:
            >>> # BEFORE executing sell order
            >>> try:
            ...     new_count = manager.increment_and_persist()
            ...     # Now safe to execute sell
            ...     result = executor.place_and_wait_for_fill(...)
            ... except RuntimeError as error:
            ...     # Validation failed - DO NOT execute sell
            ...     logger.critical(f"Sell counter persistence failed: {error}")
            ...     enter_error_state()
        """
        # Check if we need to reset for new day
        today = self._get_current_date()
        if today != self.current_date:
            logger.info(f"New trading day detected during increment: {self.current_date} -> {today}")
            logger.info("Resetting sell counter to 0 before increment")
            self.sells_today = 0
            self.current_date = today
        
        # Step 1: Increment in-memory counter
        self.sells_today += 1
        logger.info(f"Incrementing sell counter: {self.sells_today - 1} -> {self.sells_today}")
        
        # Step 2: Write to file
        try:
            content = f"{self.current_date}\n{self.sells_today}\n"
            COUNTER_FILE.write_text(content)
            logger.verbose(f"Wrote sell counter to {COUNTER_FILE}: {content.strip()}")
        except Exception as exception:
            error_message = f"Failed to write sell counter file: {exception}"
            logger.critical(error_message)
            raise RuntimeError(error_message) from exception
        
        # Step 3: Validate by reading back
        try:
            validation_content = COUNTER_FILE.read_text().strip()
            validation_lines = validation_content.split('\n')
            
            if len(validation_lines) < 2:
                raise ValueError("Invalid file format after write")
            
            validated_date = validation_lines[0].strip()
            validated_count = int(validation_lines[1].strip())
            
            if validated_date != self.current_date or validated_count != self.sells_today:
                raise ValueError(
                    f"Validation mismatch: expected ({self.current_date}, {self.sells_today}), "
                    f"got ({validated_date}, {validated_count})"
                )
            
            logger.info(f"✓ Sell counter persisted and validated: {self.sells_today} sells on {self.current_date}")
            
        except Exception as exception:
            error_message = f"Sell counter file validation FAILED: {exception}"
            logger.critical(error_message)
            raise RuntimeError(error_message) from exception
        
        # Step 4: Return new counter value
        return self.sells_today
    
    def get_counter(self) -> int:
        """
        Get current sell counter value without modifying it.
        
        Returns:
            Current sell count for today
        """
        # Check if we need to reset for new day
        today = self._get_current_date()
        if today != self.current_date:
            logger.info(f"New trading day detected: {self.current_date} -> {today}")
            self.sells_today = 0
            self.current_date = today
        
        return self.sells_today
    
    def reset_counter(self) -> None:
        """
        Manually reset the sell counter (typically not needed - auto-resets daily).
        
        This method is provided for testing and manual intervention.
        The counter automatically resets on new trading days.
        """
        logger.info(f"Manually resetting sell counter from {self.sells_today} to 0")
        self.sells_today = 0
        self.current_date = self._get_current_date()
        
        # Optionally delete the file
        if COUNTER_FILE.exists():
            COUNTER_FILE.unlink()
            logger.verbose(f"Deleted sell counter file: {COUNTER_FILE}")

