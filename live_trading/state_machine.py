"""
State Machine Module - Algorithm State Management

This module defines the state machine that controls the pairs trading algorithm.
The state machine ensures correct sequencing of operations and prevents invalid
actions based on the current state.

States:
    WARMING_UP: Fetching historical bars to bootstrap moving average
    CLEANUP_CASH: All cash, buying undervalued stock (ignores trigger)
    CLEANUP_MIXED: Partial position, resolving to full position
    CLEANUP_CONFLICT: Both stocks held, resolving to one stock
    CASH: No position, ready to buy undervalued stock
    PENDING_BUY: Buy order placed, awaiting fill confirmation
    HOLDING_WAITING: Position held, monitoring for trigger condition
    HOLDING_TRIGGERED: Trigger condition met, about to initiate swap
    PENDING_SELL: Sell order placed, awaiting fill (buy follows)
    HOLDING_DAILY_LIMIT: Position held, max trades for day reached
    ERROR: Fatal error occurred, app is frozen

State Transitions:
    See the VALID_TRANSITIONS dictionary for all allowed transitions.
    Invalid transitions raise InvalidStateTransition exception.
"""

from enum import Enum, auto
from typing import Optional, Set, Dict
from dataclasses import dataclass
from datetime import datetime


class TradingState(Enum):
    """
    Enumeration of all possible algorithm states.
    
    Each state represents a distinct phase of the trading lifecycle.
    The numeric values are used for Prometheus metrics.
    """
    WARMING_UP = 0          # Bootstrapping moving average from historical data
    CLEANUP_CASH = 1        # All cash, buying undervalued stock (ignores trigger threshold)
    CLEANUP_MIXED = 2       # Partial position, resolving to full position (ignores trigger)
    CLEANUP_CONFLICT = 3    # Both stocks held, resolving conflict (should never happen)
    CASH = 4                # No position, ready to initiate buy (intermediary in swap)
    PENDING_BUY = 5         # Buy order in flight, waiting for fill
    HOLDING_WAITING = 6     # Holding position, waiting for trigger
    HOLDING_TRIGGERED = 7   # Trigger met, about to swap
    PENDING_SELL = 8        # Sell order in flight (buy follows after)
    HOLDING_DAILY_LIMIT = 9 # Holding position, daily trade limit reached
    ERROR = 10              # Fatal error, app frozen


class StockHeld(Enum):
    """Which stock is currently held (or None if in cash)."""
    NONE = "none"
    TICKER_A = "ticker_a"
    TICKER_B = "ticker_b"


# Define valid state transitions
# Key: current state, Value: set of valid next states
VALID_TRANSITIONS: Dict[TradingState, Set[TradingState]] = {
    TradingState.WARMING_UP: {
        TradingState.CLEANUP_CASH,    # MA ready, no position
        TradingState.CLEANUP_MIXED,   # MA ready, partial position
        TradingState.CLEANUP_CONFLICT, # MA ready, both stocks (conflict)
        TradingState.CASH,            # MA ready, no position (legacy)
        TradingState.HOLDING_WAITING, # MA ready, clean position (recovery)
        TradingState.PENDING_BUY,     # MA ready, pending buy order
        TradingState.PENDING_SELL,    # MA ready, pending sell order
        TradingState.ERROR,           # Something went wrong during warmup
    },
    
    TradingState.CLEANUP_CASH: {
        TradingState.PENDING_BUY,     # Initiating buy of undervalued stock
        TradingState.ERROR,           # API failure, etc.
    },
    
    TradingState.CLEANUP_MIXED: {
        TradingState.PENDING_BUY,     # Topping up position
        TradingState.PENDING_SELL,    # Selling to flip to other stock
        TradingState.ERROR,           # API failure, etc.
    },
    
    TradingState.CLEANUP_CONFLICT: {
        TradingState.PENDING_SELL,    # Selling non-optimal stock
        TradingState.ERROR,           # API failure, etc.
    },
    
    TradingState.CASH: {
        TradingState.PENDING_BUY,     # Initiating buy of undervalued stock
        TradingState.ERROR,           # API failure, etc.
    },
    
    TradingState.PENDING_BUY: {
        TradingState.HOLDING_WAITING, # Buy order filled (clean position)
        TradingState.CLEANUP_MIXED,   # Buy order filled (still partial)
        TradingState.ERROR,           # Order rejected
    },
    
    TradingState.HOLDING_WAITING: {
        TradingState.HOLDING_TRIGGERED,   # Trigger condition met
        TradingState.HOLDING_DAILY_LIMIT, # Daily limit reached
        TradingState.ERROR,               # Position mismatch, etc.
    },
    
    TradingState.HOLDING_TRIGGERED: {
        TradingState.PENDING_SELL,    # Initiating swap (sell first)
        TradingState.HOLDING_WAITING, # Aborted (past cutoff, etc.)
        TradingState.ERROR,           # API failure
    },
    
    TradingState.PENDING_SELL: {
        TradingState.CASH,            # Sell filled, now in cash (normal swap)
        TradingState.CLEANUP_CASH,    # Sell filled, now in cash (from cleanup)
        TradingState.CLEANUP_MIXED,   # Sell filled, other stock remains (from conflict)
        TradingState.ERROR,           # Order rejected
    },
    
    TradingState.HOLDING_DAILY_LIMIT: {
        TradingState.HOLDING_WAITING, # New trading day started
        TradingState.ERROR,           # Position mismatch, etc.
    },
    
    TradingState.ERROR: {
        # ERROR is a terminal state - only manual restart can recover
        # No valid transitions out of ERROR state
    },
}


class InvalidStateTransition(Exception):
    """Exception raised when an invalid state transition is attempted."""
    pass


@dataclass
class StateData:
    """
    Additional data associated with the current state.
    
    This dataclass tracks information that persists across the main loop
    but is associated with the current state context.
    
    Attributes:
        current_stock: Which stock we're holding (or NONE if in cash)
        pending_order_id: Order ID when in PENDING states
        sells_today: Number of SELL operations executed today (for GFV prevention)
        last_trade_day: Date of the last trade (for daily reset detection)
        portfolio_value_at_trade_start: Value when current trade sequence began
    """
    current_stock: StockHeld = StockHeld.NONE
    pending_order_id: Optional[str] = None
    sells_today: int = 0
    last_trade_day: Optional[str] = None
    portfolio_value_at_trade_start: float = 0.0
    

class StateMachine:
    """
    State machine for the pairs trading algorithm.
    
    This class manages state transitions and validates that only legal
    transitions are performed. It also tracks associated state data.
    
    Usage:
        sm = StateMachine()
        sm.transition_to(TradingState.CASH, reason="MA bootstrap complete")
        if sm.can_transition_to(TradingState.PENDING_BUY):
            sm.transition_to(TradingState.PENDING_BUY, reason="Initiating buy")
    
    Attributes:
        state: Current TradingState
        data: StateData with additional context
        transition_callback: Optional callback invoked on state changes
    """
    
    def __init__(
        self,
        initial_state: TradingState = TradingState.WARMING_UP,
        on_transition=None
    ):
        """
        Initialize the state machine.
        
        Args:
            initial_state: Starting state (default: WARMING_UP)
            on_transition: Optional callback(from_state, to_state, reason)
                          called on every state transition
        """
        self._state = initial_state
        self.data = StateData()
        self.transition_callback = on_transition
        self._state_history: list = []
        
        # Record initial state in history
        self._record_state_change(None, initial_state, "initialization")
    
    @property
    def state(self) -> TradingState:
        """Get the current state."""
        return self._state
    
    @property
    def state_name(self) -> str:
        """Get the current state name as a string."""
        return self._state.name
    
    @property
    def state_value(self) -> int:
        """Get the current state numeric value (for metrics)."""
        return self._state.value
    
    def can_transition_to(self, new_state: TradingState) -> bool:
        """
        Check if a transition to the given state is valid.
        
        Args:
            new_state: The state to check transition validity for
        
        Returns:
            True if transition is valid, False otherwise
        """
        valid_next_states = VALID_TRANSITIONS.get(self._state, set())
        return new_state in valid_next_states
    
    def transition_to(
        self,
        new_state: TradingState,
        reason: str = ""
    ) -> None:
        """
        Transition to a new state.
        
        Args:
            new_state: The state to transition to
            reason: Human-readable reason for the transition
        
        Raises:
            InvalidStateTransition: If the transition is not valid
        """
        if not self.can_transition_to(new_state):
            valid_states = VALID_TRANSITIONS.get(self._state, set())
            valid_names = [s.name for s in valid_states]
            raise InvalidStateTransition(
                f"Cannot transition from {self._state.name} to {new_state.name}. "
                f"Valid transitions: {valid_names}"
            )
        
        old_state = self._state
        self._state = new_state
        self._record_state_change(old_state, new_state, reason)
        
        # Invoke callback if registered
        if self.transition_callback:
            self.transition_callback(old_state, new_state, reason)
    
    def force_error_state(self, reason: str) -> None:
        """
        Force transition to ERROR state from any state.
        
        This is a special method that bypasses normal transition validation
        because errors can occur from any state.
        
        Args:
            reason: Reason for entering error state
        """
        old_state = self._state
        self._state = TradingState.ERROR
        self._record_state_change(old_state, TradingState.ERROR, reason)
        
        if self.transition_callback:
            self.transition_callback(old_state, TradingState.ERROR, reason)
    
    def _record_state_change(
        self,
        from_state: Optional[TradingState],
        to_state: TradingState,
        reason: str
    ) -> None:
        """Record a state change in history."""
        self._state_history.append({
            "timestamp": datetime.now().isoformat(),
            "from_state": from_state.name if from_state else None,
            "to_state": to_state.name,
            "reason": reason
        })
    
    def get_state_history(self) -> list:
        """Get the complete state transition history."""
        return self._state_history.copy()
    
    # =========================================================================
    # State Data Management Methods
    # =========================================================================
    
    def set_current_stock(self, stock: StockHeld) -> None:
        """Set which stock is currently held."""
        self.data.current_stock = stock
    
    def set_pending_order(self, order_id: Optional[str]) -> None:
        """Set the pending order ID (or None to clear)."""
        self.data.pending_order_id = order_id
    
    def increment_sells_today(self) -> int:
        """
        Increment and return the sells today counter.
        
        Note: This is called by the sell counter manager after persisting to file.
        The in-memory counter tracks the same value as the persisted file.
        
        Returns:
            New sell counter value after increment
        """
        self.data.sells_today += 1
        return self.data.sells_today
    
    def reset_sells_today(self, new_day: str) -> None:
        """Reset the daily sell counter for a new trading day."""
        self.data.sells_today = 0
        self.data.last_trade_day = new_day
    
    def set_sells_today(self, count: int) -> None:
        """
        Set the sells today counter (used on startup to load from file).
        
        Args:
            count: Sell count to set
        """
        self.data.sells_today = count
    
    def set_portfolio_value_at_trade_start(self, value: float) -> None:
        """Record portfolio value at the start of a trade sequence."""
        self.data.portfolio_value_at_trade_start = value
    
    # =========================================================================
    # State Query Methods
    # =========================================================================
    
    def is_holding_position(self) -> bool:
        """Check if we're currently holding a position."""
        return self._state in {
            TradingState.HOLDING_WAITING,
            TradingState.HOLDING_TRIGGERED,
            TradingState.HOLDING_DAILY_LIMIT,
            TradingState.CLEANUP_MIXED  # Partial position still counts as holding
        }
    
    def is_order_pending(self) -> bool:
        """Check if we have an order pending."""
        return self._state in {
            TradingState.PENDING_BUY,
            TradingState.PENDING_SELL
        }
    
    def is_in_error(self) -> bool:
        """Check if we're in error state."""
        return self._state == TradingState.ERROR
    
    def is_ready_to_trade(self) -> bool:
        """Check if we're in a state where new trades can be initiated."""
        return self._state in {
            TradingState.CASH,
            TradingState.CLEANUP_CASH,
            TradingState.CLEANUP_MIXED,
            TradingState.CLEANUP_CONFLICT,
            TradingState.HOLDING_WAITING
        }
    
    def is_warming_up(self) -> bool:
        """Check if we're still warming up (bootstrapping MA)."""
        return self._state == TradingState.WARMING_UP
    
    def has_reached_daily_limit(self) -> bool:
        """Check if daily trade limit has been reached."""
        return self._state == TradingState.HOLDING_DAILY_LIMIT
    
    def can_initiate_swap(self) -> bool:
        """Check if we can initiate a new swap."""
        return self._state == TradingState.HOLDING_WAITING
    
    def is_holding_stock(self, stock: StockHeld) -> bool:
        """Check if we're holding a specific stock."""
        return (
            self.is_holding_position() and 
            self.data.current_stock == stock
        )
    
    def is_in_cleanup(self) -> bool:
        """Check if we're in any cleanup state."""
        return self._state in {
            TradingState.CLEANUP_CASH,
            TradingState.CLEANUP_MIXED,
            TradingState.CLEANUP_CONFLICT
        }


