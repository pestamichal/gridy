"""
Simple SQLite data loader for social media data
Basic implementation to compare performance with HBase
"""

import sqlite3
import csv
import time
import logging
import json
import os

# Logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class BasicSQLiteLoader:
    """
    Basic class for loading social media data into SQLite
    No optimizations - for direct comparison with HBase
    """
    
    def __init__(self, db_path='social_media_basic.db', recreate=False):
        self.db_path = db_path
        self.recreate = recreate
        self.connection = None
        self.performance_metrics = {
            'connection_time': 0,
            'table_creation_time': 0,
            'load_time': 0,
            'records_per_second': 0,
            'total_records': 0,
            'successful_records': 0
        }
        
    def create_connection(self):
        """Basic SQLite connection"""
        start_time = time.time()
        
        try:
            # Remove existing database if present
            if self.recreate and os.path.exists(self.db_path):
                os.remove(self.db_path)
                logger.info(f"🗑️ Removed existing database: {self.db_path}")

            # Basic connection
            self.connection = sqlite3.connect(self.db_path)
            
            # Test connection
            cursor = self.connection.cursor()
            cursor.execute("SELECT sqlite_version()")
            version = cursor.fetchone()[0]
            logger.info(f"✅ Connected to SQLite version: {version}")
            
            self.performance_metrics['connection_time'] = time.time() - start_time
            return True
            
        except Exception as e:
            logger.error(f"❌ SQLite connection error: {e}")
            return False
    
    def create_basic_table(self, table_name='social_media'):
        """Create a basic SQLite table"""
        start_time = time.time()
        
        try:
            cursor = self.connection.cursor()
            
            # Drop table if exists
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
            
            # Basic table - mirrors CSV structure
            create_table_sql = f"""
            CREATE TABLE {table_name} (
                id INTEGER PRIMARY KEY,
                platform TEXT,
                post_id TEXT,
                post_type TEXT,
                post_content TEXT,
                post_timestamp TEXT,
                likes INTEGER,
                comments INTEGER,
                shares INTEGER,
                impressions INTEGER,
                reach INTEGER,
                engagement_rate REAL,
                audience_age INTEGER,
                audience_gender TEXT,
                audience_location TEXT,
                audience_interests TEXT,
                campaign_id TEXT,
                sentiment TEXT,
                influencer_id TEXT
            )
            """
            
            cursor.execute(create_table_sql)
            self.connection.commit()
            logger.info(f"📊 Created basic table: {table_name}")
            
            self.performance_metrics['table_creation_time'] = time.time() - start_time
            return True
            
        except Exception as e:
            logger.error(f"❌ Error creating table: {e}")
            return False
    
    def clean_value(self, value):
        """Basic value cleaning"""
        if not value or str(value).strip() in ['', 'nan', 'None']:
            return None
        return str(value).strip()
    
    def convert_to_number(self, value):
        """Basic conversion to number"""
        if not value or str(value).strip() in ['', 'nan', 'None']:
            return None
        try:
            if '.' in str(value):
                return float(value)
            else:
                return int(value)
        except:
            return None
    
    def prepare_data(self, row):
        """Prepare data from CSV"""
        return {
            'platform': self.clean_value(row.get('Platform')),
            'post_id': self.clean_value(row.get('Post ID')),
            'post_type': self.clean_value(row.get('Post Type')),
            'post_content': self.clean_value(row.get('Post Content')),
            'post_timestamp': self.clean_value(row.get('Post Timestamp')),
            'likes': self.convert_to_number(row.get('Likes')),
            'comments': self.convert_to_number(row.get('Comments')),
            'shares': self.convert_to_number(row.get('Shares')),
            'impressions': self.convert_to_number(row.get('Impressions')),
            'reach': self.convert_to_number(row.get('Reach')),
            'engagement_rate': self.convert_to_number(row.get('Engagement Rate')),
            'audience_age': self.convert_to_number(row.get('Audience Age')),
            'audience_gender': self.clean_value(row.get('Audience Gender')),
            'audience_location': self.clean_value(row.get('Audience Location')),
            'audience_interests': self.clean_value(row.get('Audience Interests')),
            'campaign_id': self.clean_value(row.get('Campaign ID')),
            'sentiment': self.clean_value(row.get('Sentiment')),
            'influencer_id': self.clean_value(row.get('Influencer ID'))
        }
    
    def load_data_basic(self, csv_file_path, table_name='social_media'):
        """Basic data loading into SQLite"""
        logger.info("🚀 Starting basic SQLite data load...")
        start_time = time.time()
        
        total_records = 0
        successful_records = 0
        
        try:
            cursor = self.connection.cursor()
            
            # Insert SQL
            insert_sql = f"""
            INSERT INTO {table_name} (
                platform, post_id, post_type, post_content, post_timestamp,
                likes, comments, shares, impressions, reach, engagement_rate,
                audience_age, audience_gender, audience_location, audience_interests,
                campaign_id, sentiment, influencer_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            with open(csv_file_path, 'r', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                
                logger.info(f"📋 CSV Columns: {reader.fieldnames}")
                
                for row in reader:
                    total_records += 1
                    
                    # Prepare data
                    data = self.prepare_data(row)
                    
                    try:
                        # Insert single record
                        cursor.execute(insert_sql, (
                            data['platform'],
                            data['post_id'],
                            data['post_type'],
                            data['post_content'],
                            data['post_timestamp'],
                            data['likes'],
                            data['comments'],
                            data['shares'],
                            data['impressions'],
                            data['reach'],
                            data['engagement_rate'],
                            data['audience_age'],
                            data['audience_gender'],
                            data['audience_location'],
                            data['audience_interests'],
                            data['campaign_id'],
                            data['sentiment'],
                            data['influencer_id']
                        ))
                        
                        successful_records += 1
                        
                        # Commit every 1000 records
                        if successful_records % 1000 == 0:
                            self.connection.commit()
                            logger.info(f"📦 Saved {successful_records:,} records")
                        
                    except Exception as e:
                        logger.warning(f"⚠️ Record insertion error {total_records}: {e}")
                        continue
                
                # Final commit
                self.connection.commit()
        
        except Exception as e:
            logger.error(f"❌ Error during data load: {e}")
            return False
        
        # Calculate metrics
        total_time = time.time() - start_time
        self.performance_metrics.update({
            'load_time': total_time,
            'total_records': total_records,
            'successful_records': successful_records,
            'records_per_second': successful_records / total_time if total_time > 0 else 0,
            'success_rate': (successful_records / total_records * 100) if total_records > 0 else 0
        })
        
        # Report
        logger.info("="*60)
        logger.info("📊 SQLITE LOADING REPORT")
        logger.info("="*60)
        logger.info(f"📝 Records processed: {total_records:,}")
        logger.info(f"✅ Records loaded: {successful_records:,}")
        logger.info(f"📈 Success rate: {self.performance_metrics['success_rate']:.1f}%")
        logger.info(f"⏱️ Total time: {total_time:.2f}s")
        logger.info(f"🚀 Performance: {self.performance_metrics['records_per_second']:,.1f} rec/s")
        logger.info("="*60)
        
        return successful_records > 0
    
    def verify_data(self, table_name='social_media', sample_size=5):
        """Verification of loaded data"""
        logger.info("🔍 Verifying data...")
        
        try:
            cursor = self.connection.cursor()
            
            # Check table structure
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = cursor.fetchall()
            logger.info(f"📋 Table structure ({len(columns)} columns):")
            for col in columns:
                logger.info(f"   {col[1]} ({col[2]})")
            
            # Sample data
            cursor.execute(f"SELECT * FROM {table_name} LIMIT {sample_size}")
            sample_rows = cursor.fetchall()
            
            logger.info(f"\n📝 Sample of {len(sample_rows)} records:")
            for i, row in enumerate(sample_rows, 1):
                logger.info(f"   Record {i}:")
                logger.info(f"      Platform: {row[1]}")
                logger.info(f"      Post ID: {row[2]}")
                logger.info(f"      Post Type: {row[3]}")
                logger.info(f"      Likes: {row[6]}")
                logger.info(f"      Comments: {row[7]}")
                logger.info(f"      Sentiment: {row[18]}")
            
            # Record count
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            total_count = cursor.fetchone()[0]
            logger.info(f"\n📊 Total number of records: {total_count:,}")
            
            # Basic stats
            cursor.execute(f"""
                SELECT 
                    platform,
                    COUNT(*) as count,
                    AVG(likes) as avg_likes
                FROM {table_name} 
                WHERE platform IS NOT NULL
                GROUP BY platform
                ORDER BY count DESC
                LIMIT 5
            """)
            platform_stats = cursor.fetchall()
            
            logger.info(f"\n📈 Platform statistics:")
            for platform, count, avg_likes in platform_stats:
                logger.info(f"   {platform}: {count:,} posts, average {avg_likes:.1f} likes")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Verification error: {e}")
            return False
    
    def cleanup(self):
        """Close connection"""
        if self.connection:
            self.connection.close()
            logger.info("🔒 SQLite connection closed")
    
    def get_performance_report(self):
        """Return performance report"""
        return self.performance_metrics

def run_sqlite_loading():
    """Example usage of basic SQLite loader"""
    
    csv_file_path = '../social_media_engagement_data.csv'
    table_name = 'social_media'
    
    loader = BasicSQLiteLoader(recreate=False)
    
    try:
        # Connection
        if not loader.create_connection():
            return
        
        # Table creation
        if not loader.create_basic_table(table_name):
            return
        
        # Data loading
        if loader.load_data_basic(csv_file_path, table_name):
            # Verification
            loader.verify_data(table_name)
            
            # Report
            report = loader.get_performance_report()
            with open('sqlite_basic_report.json', 'w') as f:
                json.dump(report, f, indent=2)
            
            logger.info("✅ Basic SQLite loading completed successfully!")
        else:
            logger.error("❌ Data loading failed")
            
    except Exception as e:
        logger.error(f"❌ Error: {e}")
    finally:
        loader.cleanup()

if __name__ == "__main__":
    run_sqlite_loading()
