import happybase
import csv

connection = happybase.Connection(host='0.0.0.0', port=9090)

tables_to_process = [
    {
        'csv_file': 'the-reddit-climate-change-dataset-comments.csv',
        'table_name': 'reddit_climate_change_comments',
        'row_key': 'Post ID'
    },
    {
        'csv_file': 'the-reddit-climate-change-dataset-posts.csv',
        'table_name': 'reddit_climate_change_posts',
        'row_key': 'Post ID'
    }
]

for config in tables_to_process:
    table_name = config['table_name']
    csv_file = config['csv_file']
    row_key_field = config['row_key']

    if table_name.encode() not in connection.tables():
        connection.create_table(table_name, {'cf': dict()})

    table = connection.table(table_name)

    with open(csv_file, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            row_key = row[row_key_field]
            data = {f'cf:{k}': v for k, v in row.items() if k != row_key_field}
            table.put(row_key, {k.encode(): v.encode() for k, v in data.items()})

    count = sum(1 for _ in table.scan())
    print(f"✅ {csv_file} inserted into '{table_name}' — total rows: {count}")

connection.close()
