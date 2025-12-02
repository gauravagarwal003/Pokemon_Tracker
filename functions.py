import requests
import subprocess
import os
import shutil
from datetime import datetime, timedelta
from pathlib import Path
import csv
import json

# data = collect_historical_data("2024-10-30", "2024-11-05", group_id=2178, product_id=155663)
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


def _normalize_name(s: str) -> str:
    if s is None:
        return ""
    # collapse whitespace and lowercase for more robust matching
    return " ".join(s.split()).strip().lower()


def enrich_transactions(transactions_csv: str = 'transactions.csv',
                        mappings_json: str = 'mappings.json',
                        out_csv: str = 'transactions_enriched.csv',
                        item_field: str = 'Item') -> dict:
    """
    Read `transactions_csv` and `mappings_json`, match each row's `item_field`
    to a mapping `name`, and append `product_id` and `group_id` columns.

    Matching is done by normalizing whitespace and case. If an exact
    normalized match is not found, a fallback substring match is attempted
    (mapping name in item or item in mapping name).

    Returns a dict with stats: matched_count, unmatched_count, unmatched_items.
    Writes results to `out_csv`.
    """
    # load mappings
    with open(mappings_json, 'r', encoding='utf-8') as f:
        mappings = json.load(f)

    norm_map = {}
    for m in mappings:
        name = m.get('name', '')
        norm = _normalize_name(name)
        if norm and norm not in norm_map:
            norm_map[norm] = m

    matched = 0
    unmatched_items = []
    rows_out = []

    with open(transactions_csv, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        fieldnames = list(reader.fieldnames or [])
        # ensure new columns exist
        if 'product_id' not in fieldnames:
            fieldnames.append('product_id')
        if 'group_id' not in fieldnames:
            fieldnames.append('group_id')

        for row in reader:
            item_name = row.get(item_field, '')
            norm_item = _normalize_name(item_name)
            found = None

            if norm_item in norm_map:
                found = norm_map[norm_item]
            else:
                # fallback: substring match (be conservative)
                for m_norm, m in norm_map.items():
                    if m_norm in norm_item or norm_item in m_norm:
                        found = m
                        break

            if found:
                row['product_id'] = found.get('product_id', '')
                row['group_id'] = found.get('group_id', '')
                matched += 1
            else:
                row['product_id'] = ''
                row['group_id'] = ''
                unmatched_items.append(item_name)

            rows_out.append(row)

    # write out enriched csv
    with open(out_csv, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows_out:
            writer.writerow(r)

    stats = {
        'matched_count': matched,
        'unmatched_count': len(unmatched_items),
        'unmatched_items': sorted(set(unmatched_items)),
        'output_file': out_csv,
    }

    return stats