import sys
import datetime
import update_prices
import analyze_portfolio
import json
import pandas as pd
import os

def update_config_date():
    """Ensure the config file allows fetching up to today."""
    try:
        with open("data.json", 'r') as f:
            data = json.load(f)
        
        # We just ensure the latest_date is not in the past
        # Setting it to a future date is safe because the scripts clamp to datetime.now()
        data['latest_date'] = (datetime.datetime.now() + datetime.timedelta(days=365)).strftime('%Y-%m-%d')
        
        with open("data.json", 'w') as f:
            json.dump(data, f, indent=4)
    except Exception:
        pass # If fails, we trust the user's config

def main():
    print("========================================")
    print(f"  POKEMON TRACKER - DAILY UPDATE")
    print(f"  Date: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("========================================")

    # Step 0: Auto-extend the config date so we don't get stuck in the past
    update_config_date()

    # Step 1: Fetch latest prices from the web
    print("\n>>> STEP 1: Updating Historical Prices...")
    try:
        update_prices.main()
    except Exception as e:
        print(f"CRITICAL ERROR in Price Update: {e}")
        # We continue even if price update fails, to at least see current basis
        # content with old prices

    # Step 2: Recalculate Portfolio Value & Basis
    print("\n>>> STEP 2: Analyzing Portfolio Performance...")
    try:
        analyze_portfolio.run_analysis()
    except Exception as e:
        print(f"CRITICAL ERROR in Analysis: {e}")
        sys.exit(1)

    # Step 3: Generate Summary JSON for Widget (Serverless)
    print("\n>>> STEP 3: Generating Widget Summary...")
    try:
        if os.path.exists("daily_tracker.csv"):
            df = pd.read_csv("daily_tracker.csv")
            if not df.empty:
                last_row = df.iloc[-1]
                summary = {
                    "total_value": float(last_row['Total Value']),
                    "total_cost": float(last_row['Cost Basis']),
                    "profit": float(last_row['Total Value']) - float(last_row['Cost Basis']),
                    "date": str(last_row['Date']),
                    "items_owned": int(last_row['Items Owned']) if 'Items Owned' in df.columns and pd.notna(last_row['Items Owned']) else 0
                }

                with open("summary.json", "w") as f:
                    json.dump(summary, f, indent=4)
                print("  summary.json : Created")
    except Exception as e:
        print(f"Error generating summary json: {e}")

    print("\n========================================")
    print("  UPDATE COMPLETE")
    print("  1. transactions.csv : Processed")
    print("  2. daily_tracker.csv: Updated")
    print("  3. portfolio_graph.html: Refreshed")
    print("========================================")

if __name__ == "__main__":
    main()
