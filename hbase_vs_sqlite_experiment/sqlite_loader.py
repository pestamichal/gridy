"""
Prosty loader danych do SQLite dla social media data
Podstawowa implementacja do porównania wydajności z HBase
"""

import sqlite3
import csv
import time
import logging
import json
import os

# Konfiguracja logowania
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class BasicSQLiteLoader:
    """
    Podstawowa klasa do ładowania danych social media do SQLite
    Bez optymalizacji - dla czystego porównania z HBase
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
        """Podstawowe połączenie SQLite"""
        start_time = time.time()
        
        try:
            # Usunięcie istniejącej bazy jeśli istnieje
            if self.recreate and os.path.exists(self.db_path):
                os.remove(self.db_path)
                logger.info(f"🗑️ Usunięto istniejącą bazę: {self.db_path}")

            # Podstawowe połączenie
            self.connection = sqlite3.connect(self.db_path)
            
            # Test połączenia
            cursor = self.connection.cursor()
            cursor.execute("SELECT sqlite_version()")
            version = cursor.fetchone()[0]
            logger.info(f"✅ Połączono z SQLite wersja: {version}")
            
            self.performance_metrics['connection_time'] = time.time() - start_time
            return True
            
        except Exception as e:
            logger.error(f"❌ Błąd połączenia z SQLite: {e}")
            return False
    
    def create_basic_table(self, table_name='social_media'):
        """Utworzenie podstawowej tabeli SQLite"""
        start_time = time.time()
        
        try:
            cursor = self.connection.cursor()
            
            # Usunięcie tabeli jeśli istnieje
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
            
            # Podstawowa tabela - dokładnie jak w CSV
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
            logger.info(f"📊 Utworzono podstawową tabelę: {table_name}")
            
            self.performance_metrics['table_creation_time'] = time.time() - start_time
            return True
            
        except Exception as e:
            logger.error(f"❌ Błąd tworzenia tabeli: {e}")
            return False
    
    def clean_value(self, value):
        """Podstawowe oczyszczenie wartości"""
        if not value or str(value).strip() in ['', 'nan', 'None']:
            return None
        return str(value).strip()
    
    def convert_to_number(self, value):
        """Podstawowa konwersja na liczbę"""
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
        """Przygotowanie danych z CSV"""
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
        """Podstawowe ładowanie danych do SQLite"""
        logger.info("🚀 Rozpoczęcie podstawowego ładowania do SQLite...")
        start_time = time.time()
        
        total_records = 0
        successful_records = 0
        
        try:
            cursor = self.connection.cursor()
            
            # SQL do wstawiania danych
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
                
                logger.info(f"📋 Kolumny CSV: {reader.fieldnames}")
                
                for row in reader:
                    total_records += 1
                    
                    # Przygotowanie danych
                    data = self.prepare_data(row)
                    
                    try:
                        # Wstawienie pojedynczego rekordu
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
                        
                        # Commit co 1000 rekordów
                        if successful_records % 1000 == 0:
                            self.connection.commit()
                            logger.info(f"📦 Zapisano {successful_records:,} rekordów")
                        
                    except Exception as e:
                        logger.warning(f"⚠️ Błąd wstawiania rekordu {total_records}: {e}")
                        continue
                
                # Ostatni commit
                self.connection.commit()
        
        except Exception as e:
            logger.error(f"❌ Błąd podczas ładowania: {e}")
            return False
        
        # Obliczenie metryk
        total_time = time.time() - start_time
        self.performance_metrics.update({
            'load_time': total_time,
            'total_records': total_records,
            'successful_records': successful_records,
            'records_per_second': successful_records / total_time if total_time > 0 else 0,
            'success_rate': (successful_records / total_records * 100) if total_records > 0 else 0
        })
        
        # Raport
        logger.info("="*60)
        logger.info("📊 RAPORT ŁADOWANIA SQLITE")
        logger.info("="*60)
        logger.info(f"📝 Przetworzonych rekordów: {total_records:,}")
        logger.info(f"✅ Załadowanych rekordów: {successful_records:,}")
        logger.info(f"📈 Skuteczność: {self.performance_metrics['success_rate']:.1f}%")
        logger.info(f"⏱️ Całkowity czas: {total_time:.2f}s")
        logger.info(f"🚀 Wydajność: {self.performance_metrics['records_per_second']:,.1f} rek/s")
        logger.info("="*60)
        
        return successful_records > 0
    
    def verify_data(self, table_name='social_media', sample_size=5):
        """Weryfikacja załadowanych danych"""
        logger.info("🔍 Weryfikacja danych...")
        
        try:
            cursor = self.connection.cursor()
            
            # Sprawdzenie struktury tabeli
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = cursor.fetchall()
            logger.info(f"📋 Struktura tabeli ({len(columns)} kolumn):")
            for col in columns:
                logger.info(f"   {col[1]} ({col[2]})")
            
            # Próbka danych
            cursor.execute(f"SELECT * FROM {table_name} LIMIT {sample_size}")
            sample_rows = cursor.fetchall()
            
            logger.info(f"\n📝 Próbka {len(sample_rows)} rekordów:")
            for i, row in enumerate(sample_rows, 1):
                logger.info(f"   Rekord {i}:")
                logger.info(f"      Platform: {row[1]}")
                logger.info(f"      Post ID: {row[2]}")
                logger.info(f"      Post Type: {row[3]}")
                logger.info(f"      Likes: {row[6]}")
                logger.info(f"      Comments: {row[7]}")
                logger.info(f"      Sentiment: {row[18]}")
            
            # Liczba rekordów
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            total_count = cursor.fetchone()[0]
            logger.info(f"\n📊 Łączna liczba rekordów: {total_count:,}")
            
            # Podstawowe statystyki
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
            
            logger.info(f"\n📈 Statystyki platform:")
            for platform, count, avg_likes in platform_stats:
                logger.info(f"   {platform}: {count:,} postów, średnio {avg_likes:.1f} like'ów")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Błąd weryfikacji: {e}")
            return False
    
    def cleanup(self):
        """Zamknięcie połączenia"""
        if self.connection:
            self.connection.close()
            logger.info("🔒 Zamknięto połączenie z SQLite")
    
    def get_performance_report(self):
        """Zwrócenie raportu wydajności"""
        return self.performance_metrics

def main():
    """Przykład użycia podstawowego SQLite loadera"""
    
    csv_file_path = '../social_media_engagement_data.csv'
    table_name = 'social_media'
    
    loader = BasicSQLiteLoader(recreate=False)
    
    try:
        # Połączenie
        if not loader.create_connection():
            return
        
        # Utworzenie tabeli
        if not loader.create_basic_table(table_name):
            return
        
        # Ładowanie danych
        if loader.load_data_basic(csv_file_path, table_name):
            # Weryfikacja
            loader.verify_data(table_name)
            
            # Raport
            report = loader.get_performance_report()
            with open('sqlite_basic_report.json', 'w') as f:
                json.dump(report, f, indent=2)
            
            logger.info("✅ Podstawowe ładowanie SQLite zakończone pomyślnie!")
        else:
            logger.error("❌ Ładowanie nie powiodło się")
            
    except Exception as e:
        logger.error(f"❌ Błąd: {e}")
    finally:
        loader.cleanup()

if __name__ == "__main__":
    main()