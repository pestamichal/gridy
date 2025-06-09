import happybase
import csv
import time
import logging
import socket
import json
import threading
from queue import Queue
from datetime import datetime
import hashlib

# Logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class OptimizedHBaseLoader:
    """
    Optimized class for loading social media data into HBase
    with fixes for multithreading issues and broken pipe errors
    """
    def __init__(self, hbase_host='172.104.141.218', hbase_port=9090):
        self.hbase_host = hbase_host
        self.hbase_port = hbase_port
        self.main_connection = None
        self.performance_metrics = {
            'load_time': 0,
            'records_per_second': 0,
            'total_records': 0,
            'batch_times': [],
            'connection_time': 0
        }

        # Conservative settings for stability
        self.batch_size = 2000
        self.max_workers = 3
        self.connection_timeout = 30
        self.socket_timeout = 60
        self.max_retries = 3

        # Thread control
        self.batch_queue = Queue()
        self.results_queue = Queue()
        self.stop_threads = threading.Event()

    def create_single_connection(self):
        """Create a single, stable connection"""
        start_time = time.time()

        try:
            # Configure socket timeout
            socket.setdefaulttimeout(self.socket_timeout)

            self.main_connection = happybase.Connection(
                host=self.hbase_host,
                port=self.hbase_port,
                autoconnect=False,
                compat='0.98',
            )

            # Test connection
            self.main_connection.open()
            tables = self.main_connection.tables()
            logger.info(f"✅ Connected to HBase. Available tables: {len(tables)}")

            self.performance_metrics['connection_time'] = time.time() - start_time
            return True

        except Exception as e:
            logger.error(f"❌ Error connecting to HBase: {e}")
            return False

    def setup_optimized_table(self, table_name='social_media_optimized'):
        """Create an optimized HBase table structure"""
        try:
            existing_tables = self.main_connection.tables()
            if table_name.encode() in existing_tables:
                logger.info(f"🗑️ Deleting existing table {table_name}")
                self.main_connection.delete_table(table_name, disable=True)
                time.sleep(3)

            families = {
                'cf': {
                    'max_versions': 1,
                    'compression': 'NONE',
                    'bloom_filter_type': 'ROW',
                    'block_cache_enabled': True
                },
                'metrics': {
                    'max_versions': 1,
                    'compression': 'NONE',
                    'bloom_filter_type': 'ROW',
                    'block_cache_enabled': True
                }
            }

            self.main_connection.create_table(table_name, families)
            logger.info(f"📊 Created HBase table: {table_name}")
            time.sleep(2)
            return True

        except Exception as e:
            logger.error(f"❌ Error creating table: {e}")
            return False

    def generate_simple_row_key(self, row, counter):
        """Generate a simple, unique row key"""
        platform = row.get('Platform', 'unknown')[:10]
        post_id = str(row.get('Post ID', ''))
        post_id_short = post_id[:8] if len(post_id) > 8 else post_id
        row_key = f"{platform}_{post_id_short}_{counter:08d}"
        return row_key

    def prepare_simple_data(self, row):
        """Prepare data in a simplified structure tailored to the CSV"""
        data = {}
        basic_mapping = {
            'Platform': 'platform',
            'Post ID': 'post_id',
            'Post Type': 'post_type',
            'Post Content': 'post_content',
            'Post Timestamp': 'post_timestamp',
            'Audience Age': 'audience_age',
            'Audience Gender': 'audience_gender',
            'Audience Location': 'audience_location',
            'Audience Interests': 'audience_interests',
            'Campaign ID': 'campaign_id',
            'Sentiment': 'sentiment',
            'Influencer ID': 'influencer_id'
        }

        for csv_field, hbase_field in basic_mapping.items():
            val = str(row.get(csv_field, '')).strip()
            if val and val.lower() not in ['nan', 'none', '']:
                if csv_field == 'Post Content':
                    val = val[:500]
                elif csv_field == 'Audience Interests':
                    val = val[:200]
                elif csv_field == 'Audience Location':
                    val = val[:100]
                else:
                    val = val[:50]
                data[f'cf:{hbase_field}'] = val

        metrics_mapping = {
            'Likes': 'likes',
            'Comments': 'comments',
            'Shares': 'shares',
            'Impressions': 'impressions',
            'Reach': 'reach',
            'Engagement Rate': 'engagement_rate'
        }

        for csv_field, hbase_field in metrics_mapping.items():
            val = self._safe_convert(row.get(csv_field, '0'))
            if val is not None:
                data[f'metrics:{hbase_field}'] = str(val)

        if not data:
            return {}

        byte_data = {}
        for key, val in data.items():
            if val and val.strip():
                try:
                    byte_data[key.encode('utf-8')] = val.encode('utf-8')
                except Exception as e:
                    logger.warning(f"⚠️ Encoding issue for {key}: {e}")
        return byte_data

    def _safe_convert(self, value):
        """Safe conversion of values tailored to real data"""
        if not value or str(value).strip().lower() in ['', 'nan', 'none', 'null']:
            return 0
        val_str = str(value).strip()
        try:
            f = float(val_str)
            if f.is_integer() and abs(f) < 2147483647:
                return int(f)
            return round(f, 2)
        except ValueError:
            logger.debug(f"Cannot convert '{val_str}' to number")
            return 0
        except Exception as e:
            logger.warning(f"Unexpected conversion error '{val_str}': {e}")
            return 0

    def load_batch_with_retry(self, table_name, batch_data, batch_id):
        """Load a batch with retry mechanism and exponential backoff"""
        for attempt in range(self.max_retries):
            try:
                start_time = time.time()
                if attempt > 0:
                    logger.info(f"🔄 Batch {batch_id} - attempt {attempt + 1}")
                    time.sleep(2 * attempt)

                table = self.main_connection.table(table_name)
                success_count = 0
                micro_size = min(500, len(batch_data))

                for i in range(0, len(batch_data), micro_size):
                    micro_batch = batch_data[i:i + micro_size]
                    try:
                        with table.batch(batch_size=len(micro_batch)) as batch:
                            for key, data in micro_batch:
                                batch.put(key, data)
                        success_count += len(micro_batch)
                    except Exception as micro_err:
                        logger.warning(f"⚠️ Micro-batch {i//micro_size + 1} error: {micro_err}")
                        for key, data in micro_batch:
                            try:
                                table.put(key, data)
                                success_count += 1
                            except Exception as single_err:
                                logger.warning(f"⚠️ Error writing record {key}: {single_err}")

                batch_time = time.time() - start_time
                if success_count > 0:
                    logger.info(f"✅ Batch {batch_id}: {success_count}/{len(batch_data)} records in {batch_time:.2f}s")
                    return {'success': True, 'batch_id': batch_id, 'records': success_count, 'time': batch_time, 'attempt': attempt + 1}
            except Exception as e:
                logger.warning(f"⚠️ Batch {batch_id} attempt {attempt + 1} failed: {e}")
                if attempt == self.max_retries - 1:
                    logger.error(f"❌ Batch {batch_id} failed after {self.max_retries} attempts")
        return {'success': False, 'batch_id': batch_id, 'records': 0, 'time': 0, 'attempt': self.max_retries}

    def load_data_sequential(self, csv_file_path, table_name='social_media_optimized'):
        """Sequential data load—more stable than multithreaded"""
        logger.info("🚀 Starting sequential load to HBase...")
        start_time = time.time()
        total_records = 0
        successful_records = 0
        batch_count = 0
        current_batch = []

        try:
            with open(csv_file_path, 'r', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                logger.info(f"📋 CSV columns: {reader.fieldnames}")

                for idx, row in enumerate(reader):
                    total_records += 1
                    row_key = self.generate_simple_row_key(row, idx)
                    data = self.prepare_simple_data(row)
                    if data:
                        current_batch.append((row_key, data))

                    if len(current_batch) >= self.batch_size:
                        batch_count += 1
                        result = self.load_batch_with_retry(table_name, current_batch, batch_count)
                        if result['success']:
                            successful_records += result['records']
                            self.performance_metrics['batch_times'].append(result['time'])
                        current_batch = []
                        if batch_count % 5 == 0:
                            elapsed = time.time() - start_time
                            rate = successful_records / elapsed if elapsed > 0 else 0
                            logger.info(f"📊 Progress: {successful_records:,}/{total_records:,} records, {rate:.1f} rec/s")

                if current_batch:
                    batch_count += 1
                    result = self.load_batch_with_retry(table_name, current_batch, batch_count)
                    if result['success']:
                        successful_records += result['records']

        except Exception as e:
            logger.error(f"❌ Error during load: {e}")
            return False

        total_time = time.time() - start_time
        self.performance_metrics.update({
            'load_time': total_time,
            'total_records': total_records,
            'successful_records': successful_records,
            'records_per_second': successful_records / total_time if total_time > 0 else 0,
            'success_rate': (successful_records / total_records * 100) if total_records > 0 else 0,
            'total_batches': batch_count
        })

        logger.info("="*60)
        logger.info("📊 HBASE LOAD REPORT")
        logger.info("="*60)
        logger.info(f"📝 Processed records: {total_records:,}")
        logger.info(f"✅ Loaded records: {successful_records:,}")
        logger.info(f"📈 Success rate: {self.performance_metrics['success_rate']:.1f}%")
        logger.info(f"⏱️ Total time: {total_time:.2f}s")
        logger.info(f"🚀 Performance: {self.performance_metrics['records_per_second']:,.1f} rec/s")
        logger.info(f"📦 Number of batches: {batch_count}")
        logger.info("="*60)

        return successful_records > 0

    def verify_data_simple(self, table_name='social_media_optimized', sample_size=5):
        """Simple verification of loaded data—structure-aware"""
        logger.info("🔍 Verifying data...")
        try:
            table = self.main_connection.table(table_name)
            count = 0
            logger.info("📋 Sample of loaded data:")

            for key, data in table.scan(limit=sample_size):
                count += 1
                row_key = key.decode('utf-8')
                logger.info(f"\n📝 Record {count}: {row_key}")
                cf_data = {}
                metrics_data = {}

                for k, v in data.items():
                    col = k.decode('utf-8')
                    val = v.decode('utf-8')
                    if col.startswith('cf:'):
                        cf_data[col] = val
                    elif col.startswith('metrics:'):
                        metrics_data[col] = val

                if cf_data:
                    logger.info("   📄 Basic data (cf):")
                    for k, v in list(cf_data.items())[:5]:
                        logger.info(f"      {k}: {v[:50]}{'...' if len(v) > 50 else ''}")
                if metrics_data:
                    logger.info("   📊 Metrics:")
                    for k, v in metrics_data.items():
                        logger.info(f"      {k}: {v}")

            logger.info("\n📊 Counting records in table...")
            total_count = 0
            for _ in table.scan():
                total_count += 1
                if total_count % 1000 == 0:
                    logger.info(f"   Counted: {total_count:,}")
                if total_count >= 10000:
                    logger.info(f"   Stopped counting at {total_count:,} records")
                    break
            logger.info(f"\n✅ Verification complete:")
            logger.info(f"   📋 Checked sample: {count} records")
            logger.info(f"   📊 Counted in table: {total_count:,} records")

            return True
        except Exception as e:
            logger.error(f"❌ Verification error: {e}")
            return False

    def cleanup(self):
        """Close the connection"""
        if self.main_connection:
            try:
                self.main_connection.close()
                logger.info("🔒 Connection to HBase closed")
            except:
                pass

    def get_performance_report(self):
        """Return performance report"""
        return self.performance_metrics

def run_base_loading():
    """Example usage of the stable loader"""
    csv_file_path = '../social_media_engagement_data.csv'
    table_name = 'social_media_optimized'

    loader = OptimizedHBaseLoader()
    try:
        if not loader.create_single_connection():
            return
        if not loader.setup_optimized_table(table_name):
            return
        if loader.load_data_sequential(csv_file_path, table_name):
            loader.verify_data_simple(table_name)
            report = loader.get_performance_report()
            with open('hbase_stable_report.json', 'w') as f:
                json.dump(report, f, indent=2)
            logger.info("✅ Loading completed successfully!")
        else:
            logger.error("❌ Loading failed")
    except Exception as e:
        logger.error(f"❌ Error: {e}")
    finally:
        loader.cleanup()

if __name__ == "__main__":
    run_base_loading()
