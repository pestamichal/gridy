"""
Zoptymalizowany loader danych do HBase dla social media data
Wersja z poprawkami dla problemów wielowątkowych i broken pipe
"""

import happybase
import csv
import time
import logging
import hashlib
from datetime import datetime
import json
import threading
from queue import Queue
import socket

# Konfiguracja logowania
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class OptimizedHBaseLoader:
    """
    Zoptymalizowana klasa do ładowania danych social media do HBase
    z poprawkami dla problemów wielowątkowych
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
        
        # Konserwatywne ustawienia dla stabilności
        self.batch_size = 2000  # Mniejsze batche dla stabilności
        self.max_workers = 3    # Mniej wątków
        self.connection_timeout = 30
        self.socket_timeout = 60
        self.max_retries = 3
        
        # Kontrola wątków
        self.batch_queue = Queue()
        self.results_queue = Queue()
        self.stop_threads = threading.Event()
        
    def create_single_connection(self):
        """Utworzenie pojedynczego, stabilnego połączenia"""
        start_time = time.time()
        
        try:
            # Konfiguracja socket timeout
            socket.setdefaulttimeout(self.socket_timeout)
            
            self.main_connection = happybase.Connection(
                host=self.hbase_host,
                port=self.hbase_port,
                autoconnect=False,
                compat='0.98',
            )
            
            # Test połączenia
            self.main_connection.open()
            tables = self.main_connection.tables()
            logger.info(f"✅ Połączono z HBase. Dostępne tabele: {len(tables)}")
            
            self.performance_metrics['connection_time'] = time.time() - start_time
            return True
            
        except Exception as e:
            logger.error(f"❌ Błąd połączenia z HBase: {e}")
            return False
    
    def setup_optimized_table(self, table_name='social_media_optimized'):
        """Utworzenie zoptymalizowanej struktury tabeli HBase"""
        try:
            # Sprawdzenie czy tabela istnieje i jej usunięcie dla świeżego startu
            existing_tables = self.main_connection.tables()
            
            if table_name.encode() in existing_tables:
                logger.info(f"🗑️ Usuwanie istniejącej tabeli {table_name}")
                self.main_connection.delete_table(table_name, disable=True)
                time.sleep(3)  # Oczekiwanie na usunięcie
            
            # Uproszczona struktura dla stabilności - tylko dwie rodziny kolumn
            families = {
                # Podstawowe dane
                'cf': {  # Zmiana na 'cf' - standardowa nazwa
                    'max_versions': 1,
                    'compression': 'NONE',  # Wyłączenie kompresji dla stabilności
                    'bloom_filter_type': 'ROW',
                    'block_cache_enabled': True
                },
                # Metryki
                'metrics': {
                    'max_versions': 1,
                    'compression': 'NONE',
                    'bloom_filter_type': 'ROW',
                    'block_cache_enabled': True
                }
            }
            
            # Utworzenie tabeli
            self.main_connection.create_table(table_name, families)
            logger.info(f"📊 Utworzono tabelę HBase: {table_name}")
            
            # Krótkie oczekiwanie na utworzenie tabeli
            time.sleep(2)
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Błąd tworzenia tabeli: {e}")
            return False
    
    def generate_simple_row_key(self, row, counter):
        """Generowanie prostego, unikalnego klucza wiersza"""
        platform = row.get('Platform', 'unknown')[:10]
        post_id = str(row.get('Post ID', ''))
        
        # Wyciągnięcie tylko pierwszych 8 znaków z Post ID dla skrócenia
        if len(post_id) > 8:
            post_id_short = post_id[:8]
        else:
            post_id_short = post_id
        
        # Format: PLATFORM_POSTID_COUNTER
        row_key = f"{platform}_{post_id_short}_{counter:08d}"
        
        return row_key
    
    def prepare_simple_data(self, row):
        """Przygotowanie danych w uproszczonej strukturze - dostosowane do rzeczywistego CSV"""
        data = {}
        
        # Rodzina 'cf' - wszystkie podstawowe informacje
        # Mapowanie dokładnie na kolumny z przykładowych danych
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
            value = str(row.get(csv_field, '')).strip()
            if value and value.lower() not in ['nan', 'none', '']:
                # Specjalne ograniczenia dla różnych typów pól
                if csv_field == 'Post Content':
                    value = value[:500]  # Ograniczenie treści posta
                elif csv_field == 'Audience Interests':
                    value = value[:200]  # Ograniczenie listy zainteresowań
                elif csv_field == 'Audience Location':
                    value = value[:100]  # Ograniczenie lokalizacji
                else:
                    value = value[:50]   # Standardowe ograniczenie
                
                data[f'cf:{hbase_field}'] = value
        
        # Rodzina 'metrics' - pola numeryczne
        # Mapowanie metryk z odpowiednią konwersją
        metrics_mapping = {
            'Likes': 'likes',
            'Comments': 'comments', 
            'Shares': 'shares',
            'Impressions': 'impressions',
            'Reach': 'reach',
            'Engagement Rate': 'engagement_rate'
        }
        
        for csv_field, hbase_field in metrics_mapping.items():
            value = self._safe_convert(row.get(csv_field, '0'))
            if value is not None:
                data[f'metrics:{hbase_field}'] = str(value)
        
        # Konwersja na bytes tylko jeśli mamy jakieś dane
        if not data:
            return {}
            
        byte_data = {}
        for key, value in data.items():
            if value and str(value).strip():
                try:
                    byte_data[key.encode('utf-8')] = str(value).encode('utf-8')
                except Exception as e:
                    logger.warning(f"⚠️ Problem z kodowaniem {key}: {e}")
                    continue
        
        return byte_data
    
    def _safe_convert(self, value):
        """Bezpieczna konwersja wartości - dostosowana do rzeczywistych danych"""
        if not value or str(value).strip() in ['', 'nan', 'None', 'null']:
            return 0
        
        # Konwersja string na liczbę
        value_str = str(value).strip()
        
        try:
            # Próba konwersji na float
            float_val = float(value_str)
            
            # Jeśli to liczba całkowita, zwróć int
            if float_val.is_integer() and abs(float_val) < 2147483647:  # Limit int32
                return int(float_val)
            else:
                # Zaokrąglenie float do 2 miejsc po przecinku
                return round(float_val, 2)
                
        except ValueError:
            # Jeśli konwersja się nie udała, zwróć 0
            logger.debug(f"Nie można skonwertować '{value_str}' na liczbę")
            return 0
        except Exception as e:
            logger.warning(f"Nieoczekiwany błąd konwersji '{value_str}': {e}")
            return 0
    
    def load_batch_with_retry(self, table_name, batch_data, batch_id):
        """Ładowanie batcha z mechanizmem retry"""
        
        for attempt in range(self.max_retries):
            try:
                start_time = time.time()
                
                if attempt > 0:
                    logger.info(f"🔄 Batch {batch_id} - próba {attempt + 1}")
                    time.sleep(2 * attempt)  # Exponential backoff
                
                # Użycie głównego połączenia
                table = self.main_connection.table(table_name)
                
                # Użycie batch API zamiast pojedynczych put - bardziej efektywne
                success_count = 0
                batch_size = min(500, len(batch_data))  # Mniejsze micro-batche
                
                for i in range(0, len(batch_data), batch_size):
                    micro_batch = batch_data[i:i + batch_size]
                    
                    try:
                        # Użycie batch context manager
                        with table.batch(batch_size=len(micro_batch)) as batch:
                            for row_key, data in micro_batch:
                                batch.put(row_key, data)  # Bez .encode() - już jest string
                        
                        success_count += len(micro_batch)
                        
                    except Exception as micro_error:
                        logger.warning(f"⚠️ Błąd micro-batch {i//batch_size + 1}: {micro_error}")
                        
                        # Fallback na pojedyncze operacje
                        for row_key, data in micro_batch:
                            try:
                                table.put(row_key, data)
                                success_count += 1
                            except Exception as single_error:
                                logger.warning(f"⚠️ Błąd pojedynczego rekordu {row_key}: {single_error}")
                                continue
                
                batch_time = time.time() - start_time
                
                if success_count > 0:
                    logger.info(f"✅ Batch {batch_id}: {success_count}/{len(batch_data)} rekordów w {batch_time:.2f}s")
                    return {
                        'success': True,
                        'batch_id': batch_id,
                        'records': success_count,
                        'time': batch_time,
                        'attempt': attempt + 1
                    }
                
            except Exception as e:
                logger.warning(f"⚠️ Batch {batch_id} próba {attempt + 1} nieudana: {e}")
                if attempt == self.max_retries - 1:
                    logger.error(f"❌ Batch {batch_id} ostatecznie nieudany po {self.max_retries} próbach")
        
        return {
            'success': False,
            'batch_id': batch_id,
            'records': 0,
            'time': 0,
            'attempt': self.max_retries
        }
    
    def load_data_sequential(self, csv_file_path, table_name='social_media_optimized'):
        """
        Sekwencyjne ładowanie danych - bardziej stabilne niż wielowątkowe
        """
        logger.info("🚀 Rozpoczęcie sekwencyjnego ładowania do HBase...")
        start_time = time.time()
        
        total_records = 0
        successful_records = 0
        batch_count = 0
        current_batch = []
        
        try:
            with open(csv_file_path, 'r', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                
                logger.info(f"📋 Kolumny CSV: {reader.fieldnames}")
                
                for row_idx, row in enumerate(reader):
                    total_records += 1
                    
                    # Przygotowanie danych
                    row_key = self.generate_simple_row_key(row, row_idx)
                    data = self.prepare_simple_data(row)
                    
                    if data:  # Tylko jeśli są jakieś dane
                        current_batch.append((row_key, data))
                    
                    # Wysłanie batcha
                    if len(current_batch) >= self.batch_size:
                        batch_count += 1
                        result = self.load_batch_with_retry(table_name, current_batch, batch_count)
                        
                        if result['success']:
                            successful_records += result['records']
                            self.performance_metrics['batch_times'].append(result['time'])
                        
                        current_batch = []
                        
                        # Monitoring postępu
                        if batch_count % 5 == 0:
                            elapsed = time.time() - start_time
                            rate = successful_records / elapsed if elapsed > 0 else 0
                            logger.info(f"📊 Postęp: {successful_records:,}/{total_records:,} rekordów, {rate:.1f} rek/s")
                
                # Ostatni batch
                if current_batch:
                    batch_count += 1
                    result = self.load_batch_with_retry(table_name, current_batch, batch_count)
                    if result['success']:
                        successful_records += result['records']
        
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
            'success_rate': (successful_records / total_records * 100) if total_records > 0 else 0,
            'total_batches': batch_count
        })
        
        # Raport
        logger.info("="*60)
        logger.info("📊 RAPORT ŁADOWANIA HBASE")
        logger.info("="*60)
        logger.info(f"📝 Przetworzonych rekordów: {total_records:,}")
        logger.info(f"✅ Załadowanych rekordów: {successful_records:,}")
        logger.info(f"📈 Skuteczność: {self.performance_metrics['success_rate']:.1f}%")
        logger.info(f"⏱️ Całkowity czas: {total_time:.2f}s")
        logger.info(f"🚀 Wydajność: {self.performance_metrics['records_per_second']:,.1f} rek/s")
        logger.info(f"📦 Liczba batchy: {batch_count}")
        logger.info("="*60)
        
        return successful_records > 0
    
    def verify_data_simple(self, table_name='social_media_optimized', sample_size=5):
        """Prosta weryfikacja załadowanych danych - dostosowana do struktury"""
        logger.info("🔍 Weryfikacja danych...")
        
        try:
            table = self.main_connection.table(table_name)
            
            count = 0
            logger.info("📋 Próbka załadowanych danych:")
            
            for key, data in table.scan(limit=sample_size):
                count += 1
                row_key = key.decode('utf-8')
                logger.info(f"\n📝 Rekord {count}: {row_key}")
                
                # Podział na rodziny kolumn dla lepszej czytelności
                cf_data = {}
                metrics_data = {}
                
                for k, v in data.items():
                    column_name = k.decode('utf-8')
                    column_value = v.decode('utf-8')
                    
                    if column_name.startswith('cf:'):
                        cf_data[column_name] = column_value
                    elif column_name.startswith('metrics:'):
                        metrics_data[column_name] = column_value
                
                # Wyświetlenie podstawowych danych (cf)
                if cf_data:
                    logger.info("   📄 Podstawowe dane (cf):")
                    for k, v in list(cf_data.items())[:5]:  # Pierwsze 5
                        logger.info(f"      {k}: {v[:50]}{'...' if len(v) > 50 else ''}")
                
                # Wyświetlenie metryk
                if metrics_data:
                    logger.info("   📊 Metryki:")
                    for k, v in metrics_data.items():
                        logger.info(f"      {k}: {v}")
            
            # Sprawdzenie całkowitej liczby rekordów (z limitem dla wydajności)
            logger.info("\n📊 Liczenie rekordów w tabeli...")
            total_count = 0
            for _ in table.scan():
                total_count += 1
                if total_count % 1000 == 0:
                    logger.info(f"   Policzono: {total_count:,}")
                if total_count >= 10000:  # Limit dla szybkości
                    logger.info(f"   Przerwano liczenie na {total_count:,} rekordach")
                    break
            
            logger.info(f"\n✅ Weryfikacja zakończona:")
            logger.info(f"   📋 Sprawdzono próbkę: {count} rekordów")
            logger.info(f"   📊 Policzono w tabeli: {total_count:,} rekordów")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Błąd weryfikacji: {e}")
            return False
    
    def cleanup(self):
        """Zamknięcie połączenia"""
        if self.main_connection:
            try:
                self.main_connection.close()
                logger.info("🔒 Zamknięto połączenie z HBase")
            except:
                pass
    
    def get_performance_report(self):
        """Zwrócenie raportu wydajności"""
        return self.performance_metrics

def main():
    """Przykład użycia stabilnego loadera"""
    
    csv_file_path = '../social_media_engagement_data.csv'
    table_name = 'social_media_optimized'
    
    loader = OptimizedHBaseLoader()
    
    try:
        # Pojedyncze połączenie
        if not loader.create_single_connection():
            return
        
        # Konfiguracja tabeli
        if not loader.setup_optimized_table(table_name):
            return
        
        # Sekwencyjne ładowanie
        if loader.load_data_sequential(csv_file_path, table_name):
            # Weryfikacja
            loader.verify_data_simple(table_name)
            
            # Raport
            report = loader.get_performance_report()
            with open('hbase_stable_report.json', 'w') as f:
                json.dump(report, f, indent=2)
            
            logger.info("✅ Ładowanie zakończone pomyślnie!")
        else:
            logger.error("❌ Ładowanie nie powiodło się")
            
    except Exception as e:
        logger.error(f"❌ Błąd: {e}")
    finally:
        loader.cleanup()

if __name__ == "__main__":
    main()