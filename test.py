from functions import collect_historical_data, enrich_transactions


if __name__ == '__main__':
	print(collect_historical_data("2025-08-11", "2025-08-13", 24269, 628395))

	# Enrich transactions.csv with product_id and group_id from mappings.json
	stats = enrich_transactions('transactions.csv', 'mappings.json', 'transactions_enriched.csv', item_field='Item')
	print("Enrichment stats:", stats)
