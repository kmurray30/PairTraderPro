"""Shared utility functions for TradeStation API demos."""


def to_float(value, default=0.0):
    """
    Safely convert string/numeric values to float.
    
    Args:
        value: The value to convert (can be string, number, or None)
        default: Default value to return if conversion fails
        
    Returns:
        float: The converted value or default
    """
    try:
        return float(value) if value is not None else default
    except (ValueError, TypeError):
        return default

