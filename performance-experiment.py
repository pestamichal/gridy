"""
Zoptymalizowany loader danych do HBase dla social media data
Skupia się na maksymalnej wydajności HBase
"""

import happybase
import csv
import time
import logging
import hashlib
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import json

# Konfiguracja logowania
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class OptimizedHBaseLoader:
    """
    Zoptymalizowana klasa do ładowania danych social media do HBase
    z fokusem na wydajność i skalowanie
    """
    
    def __init__(self, hbase_host='172.104.141.218', hbase_port=9090):
        self.hbase_host = hbase_host
        self.hbase_port = hbase_port
        self.connection_pool = []
        self.performance_metrics = {
            'load_time': 0,
            'records_per_second': 0,
            'total_records': 0,
            'batch_times': [],
            'connection_time': 0
        }
        
        # Optymalizowane ustawienia
        self.batch_size = 5000  # Większe batche dla lepszej wydajności
        self.thread_pool_size = 8  # Wielowątkowość
        self.auto_flush = False  # Wyłączenie auto-flush dla batchy
        
    def create_connection_pool(self, pool_size=4):
        """Tworzenie puli połączeń dla wielowątkowości"""
        start_time = time.time()
        
        try:
            for i in range(pool_size):
                conn = happybase.Connection(
                    host=self.hbase_host,
                    port=self.hbase_port,
                    autoconnect=False,
                    compat='0.98',  # Kompatybilność dla lepszej wydajności
                    transport='buffered',  # Buforowane połączenia
                )
                conn.open()
                self.connection_pool.append(conn)
                logger.info(f"✅ Utworzono połączenie {i+1}/{pool_size}")
            
            self.performance_metrics['connection_time'] = time.time() - start_time
            logger.info(f"🔗 Pula {pool_size} połączeń utworzona w {self.performance_metrics['connection_time']:.2f}s")
            return True
            
        except Exception as e:
            logger.error(f"❌ Błąd tworzenia puli połączeń: {e}")
            return False
    
    def setup_optimized_table(self, table_name='social_media_optimized'):
        """
        Utworzenie zoptymalizowanej struktury tabeli HBase
        Z wieloma rodzinami kolumn dla lepszej wydajności
        """
        try:
            # Używamy pierwszego połączenia do operacji administracyjnych
            admin_conn = self.connection_pool[0]
            
            # Definicja rodzin kolumn zoptymalizowana pod dane social media
            families = {
                # Podstawowe informacje o poście
                'post': {
                    'max_versions': 1,
                    'compression': 'SNAPPY',  # Kompresja dla oszczędności miejsca
                    'bloom_filter_type': 'ROW',  # Bloom filter dla szybszego wyszukiwania
                    'block_cache_enabled': True,
                    'time_to_live': 31536000  # 1 rok TTL
                },
                # Metryki zaangażowania
                'metrics': {
                    'max_versions': 1,
                    'compression': 'SNAPPY',
                    'bloom_filter_type': 'ROW',
                    'block_cache_enabled': True,
                    'in_memory': True  # Często używane dane w pamięci
                },
                # Informacje o audience
                'audience': {
                    'max_versions': 1,
                    'compression': 'SNAPPY',
                    'bloom_filter_type': 'ROW'
                },
                # Kampanie i influencerzy
                'campaign': {
                    'max_versions': 1,
                    'compression': 'SNAPPY',
                    'bloom_filter_type': 'ROW'
                }
            }
            
            # Usunięcie tabeli jeśli istnieje
            if table_name.encode() in admin_conn.tables():
                logger.info(f"🗑️ Usuwanie istniejącej tabeli {table_name}")
                admin_conn.delete_table(table_name, disable=True)
            
            # Utworzenie nowej tabeli
            admin_conn.create_table(table_name, families)
            
            logger.info(f"📊 Utworzono zoptymalizowaną tabelę HBase: {table_name}")
            logger.info(f"📋 Rodziny kolumn: {list(families.keys())}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Błąd tworzenia tabeli: {e}")
            return False
    
    def generate_optimized_row_key(self, row):
        """
        Generowanie zoptymalizowanego klucza wiersza
        Używa hash + timestamp dla lepszej dystrybucji
        """
        platform = row.get('Platform', 'unknown')
        post_id = row.get('Post ID', '')
        timestamp = row.get('Post Timestamp', '')
        
        # Parsowanie timestamp dla lepszego sortowania
        try:
            if timestamp:
                # Zakładając format typu "2024-01-15 10:30:00"
                dt = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
                ts_prefix = dt.strftime('%Y%m%d')
            else:
                ts_prefix = '20240101'
        except:
            ts_prefix = '20240101'
        
        # Hash dla lepszej dystrybucji (unikanie hotspotów)
        hash_suffix = hashlib.md5(f"{platform}_{post_id}".encode()).hexdigest()[:8]
        
        # Format: YYYYMMDD_PLATFORM_HASH
        # To zapewnia chronologiczne sortowanie i równomierną dystrybucję
        row_key = f"{ts_prefix}_{platform}_{hash_suffix}"
        
        return row_key
    
    def prepare_optimized_data(self, row):
        """
        Przygotowanie danych w zoptymalizowanej strukturze
        Podział na logiczne rodziny kolumn
        """
        data = {}
        
        # Rodzina 'post' - podstawowe informacje
        post_data = {
            'post:id': str(row.get('Post ID', '')),
            'post:type': str(row.get('Post Type', '')),
            'post:content': str(row.get('Post Content', ''))[:1000],  # Ograniczenie długości
            'post:timestamp': str(row.get('Post Timestamp', '')),
            'post:platform': str(row.get('Platform', ''))
        }
        
        # Rodzina 'metrics' - metryki zaangażowania (konwersja na int gdzie możliwe)
        metrics_data = {
            'metrics:likes': str(self._safe_int(row.get('Likes', 0))),
            'metrics:comments': str(self._safe_int(row.get('Comments', 0))),
            'metrics:shares': str(self._safe_int(row.get('Shares', 0))),
            'metrics:impressions': str(self._safe_int(row.get('Impressions', 0))),
            'metrics:reach': str(self._safe_int(row.get('Reach', 0))),
            'metrics:engagement_rate': str(self._safe_float(row.get('Engagement Rate', 0.0)))
        }
        
        # Rodzina 'audience' - informacje o odbiorach
        audience_data = {
            'audience:age': str(row.get('Audience Age', '')),
            'audience:gender': str(row.get('Audience Gender', '')),
            'audience:location': str(row.get('Audience Location', '')),
            'audience:interests': str(row.get('Audience Interests', ''))
        }
        
        # Rodzina 'campaign' - kampanie i influencerzy
        campaign_data = {
            'campaign:id': str(row.get('Campaign ID', '')),
            'campaign:sentiment': str(row.get('Sentiment', '')),
            'campaign:influencer_id': str(row.get('Influencer ID', ''))
        }
        
        # Łączenie wszystkich danych
        for family_data in [post_data, metrics_data, audience_data, campaign_data]:
            for key, value in family_data.items():
                if value and value != 'nan':  # Filtrowanie pustych wartości
                    data[key.encode()] = value.encode()
        
        return data
    
    def _safe_int(self, value):
        """Bezpieczna konwersja na int"""
        try:
            if value == '' or value is None or str(value).lower() == 'nan':
                return 0
            return int(float(value))
        except:
            return 0
    
    def _safe_float(self, value):
        """Bezpieczna konwersja na float"""
        try:
            if value == '' or value is None or str(value).lower() == 'nan':
                return 0.0
            return float(value)
        except:
            return 0.0
    
    def load_batch_threaded(self, connection, table_name, batch_data):
        """
        Ładowanie pojedynczego batcha w osobnym wątku
        """
        start_time = time.time()
        
        try:
            table = connection.table(table_name)
            
            # Użycie batch z większym buforem
            with table.batch(batch_size=len(batch_data), transaction=False) as batch:
                for row_key, data in batch_data:
                    batch.put(row_key.encode(), data)
            
            batch_time = time.time() - start_time
            logger.info(f"📦 Batch {len(batch_data)} rekordów załadowany w {batch_time:.3f}s")
            
            return {
                'success': True,
                'records': len(batch_data),
                'time': batch_time
            }
            
        except Exception as e:
            logger.error(f"❌ Błąd ładowania batcha: {e}")
            return {
                'success': False,
                'records': 0,
                'time': 0
            }
    
    def load_data_optimized(self, csv_file_path, table_name='social_media_optimized'):
        """
        Główna funkcja ładowania z optymalizacjami:
        - Wielowątkowość
        - Większe batche
        - Zoptymalizowana struktura danych
        - Monitoring wydajności
        """
        logger.info("🚀 Rozpoczęcie zoptymalizowanego ładowania do HBase...")
        start_time = time.time()
        
        total_records = 0
        current_batch = []
        batch_futures = []
        
        try:
            with open(csv_file_path, 'r', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                
                # Sprawdzenie nagłówków
                logger.info(f"📋 Kolumny CSV: {reader.fieldnames}")
                
                # Przygotowanie ThreadPoolExecutor
                with ThreadPoolExecutor(max_workers=self.thread_pool_size) as executor:
                    
                    for row in reader:
                        total_records += 1
                        
                        # Przygotowanie danych
                        row_key = self.generate_optimized_row_key(row)
                        optimized_data = self.prepare_optimized_data(row)
                        
                        current_batch.append((row_key, optimized_data))
                        
                        # Wysłanie batcha gdy osiągnie odpowiedni rozmiar
                        if len(current_batch) >= self.batch_size:
                            # Wybór połączenia z puli (round-robin)
                            connection = self.connection_pool[len(batch_futures) % len(self.connection_pool)]
                            
                            # Wysłanie batcha w osobnym wątku
                            future = executor.submit(
                                self.load_batch_threaded,
                                connection,
                                table_name,
                                current_batch.copy()
                            )
                            batch_futures.append(future)
                            
                            logger.info(f"📤 Wysłano batch {len(batch_futures)} ({len(current_batch)} rekordów)")
                            current_batch = []
                            
                            # Monitoring postępu co 10 batchy
                            if len(batch_futures) % 10 == 0:
                                logger.info(f"📊 Postęp: {total_records} rekordów, {len(batch_futures)} batchy w toku")
                    
                    # Wysłanie ostatniego batcha
                    if current_batch:
                        connection = self.connection_pool[0]
                        future = executor.submit(
                            self.load_batch_threaded,
                            connection,
                            table_name,
                            current_batch
                        )
                        batch_futures.append(future)
                        logger.info(f"📤 Wysłano ostatni batch ({len(current_batch)} rekordów)")
                    
                    # Oczekiwanie na zakończenie wszystkich batchy
                    logger.info("⏳ Oczekiwanie na zakończenie wszystkich batchy...")
                    successful_batches = 0
                    total_batch_time = 0
                    
                    for future in as_completed(batch_futures):
                        result = future.result()
                        if result['success']:
                            successful_batches += 1
                            total_batch_time += result['time']
                            self.performance_metrics['batch_times'].append(result['time'])
        
        except Exception as e:
            logger.error(f"❌ Błąd podczas ładowania: {e}")
            return False
        
        # Obliczenie metryk wydajności
        total_time = time.time() - start_time
        self.performance_metrics.update({
            'load_time': total_time,
            'total_records': total_records,
            'records_per_second': total_records / total_time if total_time > 0 else 0,
            'successful_batches': successful_batches,
            'total_batches': len(batch_futures),
            'avg_batch_time': total_batch_time / successful_batches if successful_batches > 0 else 0
        })
        
        # Raport wydajności
        logger.info("="*60)
        logger.info("📊 RAPORT WYDAJNOŚCI ŁADOWANIA HBASE")
        logger.info("="*60)
        logger.info(f"✅ Załadowano rekordów: {total_records:,}")
        logger.info(f"⏱️ Całkowity czas: {total_time:.2f}s")
        logger.info(f"🚀 Wydajność: {self.performance_metrics['records_per_second']:,.1f} rekordów/s")
        logger.info(f"📦 Batche: {successful_batches}/{len(batch_futures)} udanych")
        logger.info(f"⚡ Średni czas batcha: {self.performance_metrics['avg_batch_time']:.3f}s")
        logger.info(f"🔗 Czas połączeń: {self.performance_metrics['connection_time']:.2f}s")
        logger.info("="*60)
        
        return True
    
    def verify_data_loading(self, table_name='social_media_optimized', sample_size=10):
        """Weryfikacja poprawności załadowanych danych"""
        logger.info("🔍 Weryfikacja załadowanych danych...")
        
        try:
            table = self.connection_pool[0].table(table_name)
            
            # Pobranie próbki danych
            sample_count = 0
            for key, data in table.scan(limit=sample_size):
                sample_count += 1
                logger.info(f"📝 Rekord {sample_count}: {key.decode()}")
                
                # Pokazanie przykładowych danych z każdej rodziny kolumn
                for family in ['post', 'metrics', 'audience', 'campaign']:
                    family_data = {k.decode(): v.decode() for k, v in data.items() 
                                 if k.decode().startswith(family)}
                    if family_data:
                        logger.info(f"   {family}: {dict(list(family_data.items())[:3])}")
            
            # Szybki count (przybliżony)
            logger.info("📊 Liczenie rekordów w tabeli...")
            count = 0
            for _ in table.scan():
                count += 1
                if count % 10000 == 0:
                    logger.info(f"   Policzono: {count:,} rekordów...")
                if count > 100000:  # Limit dla szybkości
                    logger.info(f"   Przerwano liczenie na {count:,} rekordach")
                    break
            
            logger.info(f"✅ Weryfikacja zakończona. Próbka: {sample_count}, Policzono: {count:,}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Błąd weryfikacji: {e}")
            return False
    
    def cleanup(self):
        """Zamknięcie wszystkich połączeń"""
        for conn in self.connection_pool:
            try:
                conn.close()
            except:
                pass
        logger.info("🔒 Zamknięto wszystkie połączenia")
    
    def get_performance_report(self):
        """Zwrócenie raportu wydajności"""
        return self.performance_metrics

def main():
    """Przykład użycia zoptymalizowanego loadera"""
    
    # Ścieżka do pliku CSV
    csv_file_path = 'social_media_engagement_data.csv'
    table_name = 'social_media_optimized'
    
    # Utworzenie loadera
    loader = OptimizedHBaseLoader()
    
    try:
        # Utworzenie puli połączeń
        if not loader.create_connection_pool(pool_size=4):
            return
        
        # Konfiguracja tabeli
        if not loader.setup_optimized_table(table_name):
            return
        
        # Ładowanie danych
        if loader.load_data_optimized(csv_file_path, table_name):
            # Weryfikacja
            loader.verify_data_loading(table_name)
            
            # Zapisanie raportu wydajności
            report = loader.get_performance_report()
            with open('hbase_performance_report.json', 'w') as f:
                json.dump(report, f, indent=2)
            
            logger.info("✅ Optymalizowane ładowanie zakończone pomyślnie!")
        else:
            logger.error("❌ Ładowanie nie powiodło się")
            
    except Exception as e:
        logger.error(f"❌ Błąd podczas eksperymentu: {e}")
    finally:
        loader.cleanup()

if __name__ == "__main__":
    main()