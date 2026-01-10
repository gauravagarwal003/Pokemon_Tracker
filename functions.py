import requests
import subprocess
import os
import shutil
from datetime import datetime, timedelta
from pathlib import Path
import csv
import json

with open("data.json") as f:
    data = json.load(f)
MAPPINGS_FILE = data.get("mappings_file")
TRANSACTIONS_FILE = data.get("transactions_file")

# collect_historical_data("2025-08-11", "2025-08-13", 24269, 628395)
# [{'date': '2025-08-11', 'marketPrice': 14.2}, {'date': '2025-08-12', 'marketPrice': None}, {'date': '2025-08-13', 'marketPrice': 14.42}]
def collect_historical_data(start_date_str, end_date_str, group_id, product_id):
    """
    Return a list of dicts with only date and marketPrice for the specified
    group_id and product_id over the date range:
      - date (YYYY-MM-DD)
      - marketPrice (float or None)
    Missing or error days will have marketPrice set to None.
    """
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
    current_date = start_date
    results = []

    while current_date <= end_date:
        date_str = current_date.strftime('%Y-%m-%d')
        archive_url = f"https://tcgcsv.com/archive/tcgplayer/prices-{date_str}.ppmd.7z"
        archive_filename = f"prices-{date_str}.ppmd.7z"
        extracted_folder = date_str

        try:
            resp = requests.get(archive_url, stream=True)
            if resp.status_code != 200:
                results.append({'date': date_str, 'marketPrice': None})
                current_date += timedelta(days=1)
                continue

            with open(archive_filename, 'wb') as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    f.write(chunk)

            # extract
            result = subprocess.run(['7z', 'x', archive_filename, '-y'],
                                    capture_output=True, text=True)
            if result.returncode != 0:
                results.append({'date': date_str, 'marketPrice': None})
                cleanup_files(archive_filename, extracted_folder)
                current_date += timedelta(days=1)
                continue

            prices_file = Path(extracted_folder) / "3" / str(group_id) / "prices"
            if not prices_file.exists():
                results.append({'date': date_str, 'marketPrice': None})
                cleanup_files(archive_filename, extracted_folder)
                current_date += timedelta(days=1)
                continue

            # read JSON and find product
            import json
            with open(prices_file, 'r') as f:
                data = json.load(f)

            found_price = None
            if isinstance(data, dict) and 'results' in data and isinstance(data['results'], list):
                for prod in data['results']:
                    if str(prod.get('productId')) == str(product_id):
                        mp = prod.get('marketPrice')
                        if mp is None or mp == '':
                            found_price = None
                        else:
                            try:
                                found_price = float(mp)
                            except (ValueError, TypeError):
                                found_price = None
                        break

            results.append({'date': date_str, 'marketPrice': found_price})

            cleanup_files(archive_filename, extracted_folder)

        except Exception:
            cleanup_files(archive_filename, extracted_folder)
            results.append({'date': date_str, 'marketPrice': None})

        current_date += timedelta(days=1)

    return results

def cleanup_files(archive_filename, extracted_folder):
    """
    Clean up downloaded and extracted files
    """
    try:
        if os.path.exists(archive_filename):
            os.remove(archive_filename)
        if os.path.exists(extracted_folder):
            shutil.rmtree(extracted_folder)
    except Exception as e:
        print(f"  ⚠️  Warning: Could not clean up files: {e}")    
        
def get_product_info_from_ids(group_id, product_id):
    """
    Given group_id and product_id, return info (imageUrl, name, categoryId, and url) using the provided mappings dictionary.
    """
    if not os.path.exists(MAPPINGS_FILE):
        raise FileNotFoundError(f"Mappings file '{MAPPINGS_FILE}' not found.")

    with open(MAPPINGS_FILE, 'r') as f:
        mappings = json.load(f)

    group_str = str(group_id)
    product_str = str(product_id)

    for mapping in mappings:
        if mapping["group_id"] == group_str and mapping["product_id"] == product_str:
            return {
                'categoryId': mapping.get('categoryId'),
                'name': mapping.get('name'),
                'imageUrl': mapping.get('imageUrl'),
                'url': mapping.get('url')
            }

    return None

def get_product_info_from_name(product_name):
    """
    Given product name, return info (group_id, product_id, imageUrl, categoryId, and url) using the provided mappings dictionary.
    """
    if not os.path.exists(MAPPINGS_FILE):
        raise FileNotFoundError(f"Mappings file '{MAPPINGS_FILE}' not found.")

    with open(MAPPINGS_FILE, 'r') as f:
        mappings = json.load(f)

    name = str(product_name)

    for mapping in mappings:
        if mapping["name"] == name:
            return {
                'group_id': mapping.get('group_id'),
                'product_id': mapping.get('product_id'),
                'categoryId': mapping.get('categoryId'),
                'imageUrl': mapping.get('imageUrl'),
                'url': mapping.get('url')
            }

    return None    

def update_historical_price_files(start_date_str, end_date_str, group_id, product_id, output_folder='historical_prices'):
    """
    Update historical price files for the specified group_id and product_id
    over the date range. Saves individual date files in output_folder.
    Any missing dates are filled with a best-guess price using nearby known values.
    """
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")

    if start_date > end_date:
        raise ValueError("start_date must be on or before end_date")

    records = collect_historical_data(start_date_str, end_date_str, group_id, product_id)

    def best_guess_price(idx):
        price = records[idx].get('marketPrice')
        if price is not None:
            return price

        prev_price = None
        for j in range(idx - 1, -1, -1):
            candidate = records[j].get('marketPrice')
            if candidate is not None:
                prev_price = candidate
                break

        next_price = None
        for j in range(idx + 1, len(records)):
            candidate = records[j].get('marketPrice')
            if candidate is not None:
                next_price = candidate
                break

        if prev_price is not None and next_price is not None:
            return round((prev_price + next_price) / 2, 2)
        if prev_price is not None:
            return prev_price
        if next_price is not None:
            return next_price
        return None

    base_path = Path(output_folder) / str(group_id) / str(product_id)
    base_path.mkdir(parents=True, exist_ok=True)

    saved_files = []

    for idx, record in enumerate(records):
        date_str = record.get('date')
        if not date_str:
            continue  # Skip malformed entries

        file_path = base_path / f"{date_str}.json"

        # Ignore/overwrite any existing data for the date in range
        if file_path.exists():
            file_path.unlink()

        payload = {
            'date': date_str,
            'group_id': str(group_id),
            'product_id': str(product_id),
            'marketPrice': best_guess_price(idx)
        }

        with open(file_path, 'w') as f:
            json.dump(payload, f)

        saved_files.append(str(file_path))

    return saved_files

