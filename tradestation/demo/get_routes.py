"""Get available order routing options.

CLI Usage:
    python -m tradestation.demo.get_routes
    python -m tradestation.demo.get_routes --environment sim
    python -m tradestation.demo.get_routes --environment prod
"""

import json


def get_routes(api):
    """
    Get list of available order routing options.
    
    What is routing? It determines which exchange/market maker receives your order.
    Different routes can have different: speeds, fees, liquidity.
    
    Args:
        api: TradeStation API instance
    """
    routes = api.orders.get_routes()
    
    print("Available Order Routing Options:")
    print(json.dumps(routes, indent=2))
    
    if 'Routes' in routes and len(routes['Routes']) > 0:
        print(f"\nFound {len(routes['Routes'])} available route(s):\n")
        
        for route in routes['Routes']:
            name = route.get('Route', 'N/A')
            description = route.get('Description', 'No description available')
            
            print(f"  • {name}")
            if description and description != 'No description available':
                print(f"    {description}")
            print()
    else:
        print("  No routes found")
    
    print("\nRecommended: Use 'Intelligent' routing")
    print("  - TradeStation's smart order routing")
    print("  - Automatically finds best execution")
    print("  - Works for most strategies")
    print("  - No need to manually select exchanges")


def main():
    """CLI entry point."""
    import argparse
    from .setup_api import setup_api
    
    parser = argparse.ArgumentParser(description='Get available routing options')
    parser.add_argument('--environment', default='sim', choices=['sim', 'prod'])
    args = parser.parse_args()
    
    api, _ = setup_api(args.environment)
    get_routes(api)


if __name__ == '__main__':
    main()

