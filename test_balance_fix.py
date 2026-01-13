"""
Test Balance Parsing Fix

This script verifies that the balance parsing bug has been fixed
by making a real API call and checking the returned values.
"""

import sys
from pathlib import Path

# Add project to path
sys.path.insert(0, str(Path(__file__).parent))

from tradestation.api import TradeStationAPI
from live_trading.order_executor import OrderExecutor
from live_trading.reconciliation import Reconciler
from live_trading.logger import logger, set_log_level_from_config

def test_balance_parsing():
    """Test that balance parsing returns actual values, not $0.00"""
    print("=" * 70)
    print("Balance Parsing Fix - Verification Test")
    print("=" * 70)
    
    # Initialize API in SIM mode
    print("\n1. Initializing API (SIM mode)...")
    api = TradeStationAPI("sim")
    account_id = api.config.account_id
    
    if not account_id.startswith('SIM'):
        print("❌ ERROR: Not a SIM account!")
        return False
    
    print(f"   ✓ Using account: {account_id}")
    
    # Test OrderExecutor.get_buying_power()
    print("\n2. Testing OrderExecutor.get_buying_power()...")
    executor = OrderExecutor(api, account_id, allocated_cash=1000)
    buying_power = executor.get_buying_power()
    
    print(f"   Result: ${buying_power:,.2f}")
    
    if buying_power == 0:
        print("   ❌ FAILED: Still returning $0.00")
        return False
    elif buying_power == 1000:
        print("   ✓ PASSED: Correctly capped at allocated_cash ($1000)")
    else:
        print(f"   ✓ PASSED: Returned actual buying power")
    
    # Test Reconciler.get_buying_power()
    print("\n3. Testing Reconciler.get_buying_power()...")
    reconciler = Reconciler(api, account_id, "V", "MA", allocated_cash=1000)
    buying_power2 = reconciler.get_buying_power()
    
    print(f"   Result: ${buying_power2:,.2f}")
    
    if buying_power2 == 0:
        print("   ❌ FAILED: Still returning $0.00")
        return False
    elif buying_power2 == 1000:
        print("   ✓ PASSED: Correctly capped at allocated_cash ($1000)")
    else:
        print(f"   ✓ PASSED: Returned actual buying power")
    
    # Test Reconciler.get_portfolio_value()
    print("\n4. Testing Reconciler.get_portfolio_value()...")
    portfolio_value = reconciler.get_portfolio_value()
    
    print(f"   Result: ${portfolio_value:,.2f}")
    
    if portfolio_value == 0:
        print("   ❌ FAILED: Still returning $0.00")
        return False
    else:
        print(f"   ✓ PASSED: Returned actual portfolio value")
    
    # Test without allocated_cash cap
    print("\n5. Testing without allocated_cash cap...")
    executor_uncapped = OrderExecutor(api, account_id, allocated_cash=0)
    buying_power_uncapped = executor_uncapped.get_buying_power()
    
    print(f"   Result: ${buying_power_uncapped:,.2f}")
    
    if buying_power_uncapped == 0:
        print("   ❌ FAILED: Still returning $0.00")
        return False
    elif buying_power_uncapped > 1000:
        print(f"   ✓ PASSED: Full account buying power returned (uncapped)")
    else:
        print(f"   ⚠️  WARNING: Buying power seems low for SIM account")
    
    # Summary
    print("\n" + "=" * 70)
    print("✅ ALL TESTS PASSED")
    print("=" * 70)
    print("\nBalance parsing bug has been successfully fixed!")
    print(f"- Buying power (capped):   ${buying_power:,.2f}")
    print(f"- Buying power (uncapped): ${buying_power_uncapped:,.2f}")
    print(f"- Portfolio value:         ${portfolio_value:,.2f}")
    
    return True

if __name__ == "__main__":
    try:
        success = test_balance_parsing()
        sys.exit(0 if success else 1)
    except Exception as error:
        print(f"\n❌ TEST FAILED WITH ERROR: {error}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

