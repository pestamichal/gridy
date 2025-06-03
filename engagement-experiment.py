import happybase
from collections import defaultdict

connection = happybase.Connection('localhost')
table = connection.table('social_media')

platform_totals = defaultdict(float)
platform_counts = defaultdict(int)

for key, data in table.scan():
    platform = data.get(b'cf:Platform')
    engagement = data.get(b'cf:Engagement Rate')
    
    if platform and engagement:
        platform_str = platform.decode('utf-8')
        try:
            engagement_value = float(engagement.decode('utf-8'))
            platform_totals[platform_str] += engagement_value
            platform_counts[platform_str] += 1
        except ValueError:
            continue

print("Average Engagement Rate per Platform:")
for platform in platform_totals:
    avg = platform_totals[platform] / platform_counts[platform]
    print(f"{platform}: {avg:.2f}")