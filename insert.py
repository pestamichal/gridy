import happybase
import csv

connection = happybase.Connection(host='0.0.0.0', port=9090)
table_name = 'social_media'

families = {'cf': dict()}
if table_name.encode() not in connection.tables():
    connection.create_table(table_name, families)

table = connection.table(table_name)

with open('social_media_engagement_data.csv', newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        row_key = row['Post ID']
        data = {f'cf:{k}': v for k, v in row.items() if k != 'Post ID'}
        table.put(row_key, {k.encode(): v.encode() for k, v in data.items()})

print("✅ Data inserted successfully.")

count = sum(1 for _ in table.scan())
print(f"📊 Total rows in 'social_media': {count}")

connection.close()