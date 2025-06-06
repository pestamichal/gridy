"""
Eksperyment analizy wydajności HBase vs SQLite
Wykorzystuje istniejący kod do wczytywania danych z CSV
"""

import happybase
import csv
import sqlite3
import time
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import json
from datetime import datetime
import logging

# Konfiguracja logowania
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SocialMediaPerformanceAnalyzer:
    """
    Klasa do analizy wydajności HBase vs SQLite przy przetwarzaniu danych social media
    """
    
    def __init__(self, hbase_host='localhost', hbase_port=9090):
        """Inicjalizacja połączeń i struktur danych"""
        self.hbase_host = hbase_host
        self.hbase_port = hbase_port
        self.hbase_connection = None
        self.sqlite_connection = None
        
        # Struktura do zbierania metryk wydajności
        self.performance_metrics = {
            'hbase': {
                'write_times': [],      # Czasy zapisu danych
                'read_times': [],       # Czasy odczytu pojedynczych rekordów
                'query_times': [],      # Czasy wykonania zapytań filtrujących
                'scan_times': [],       # Czasy skanowania całej tabeli
                'aggregation_times': [] # Czasy operacji agregacyjnych
            },
            'sqlite': {
                'write_times': [],
                'read_times': [],
                'query_times': [],
                'scan_times': [],
                'aggregation_times': []
            }
        }
        
        # Dodatkowe metryki
        self.data_stats = {
            'total_records': 0,
            'csv_file_size': 0,
            'unique_platforms': set(),
            'date_range': {'start': None, 'end': None}
        }
    
    def connect_hbase(self):
        """Nawiązanie połączenia z HBase"""
        try:
            self.hbase_connection = happybase.Connection(
                host=self.hbase_host, 
                port=self.hbase_port
            )
            self.hbase_connection.open()
            logger.info("✅ Połączono z HBase")
            return True
        except Exception as e:
            logger.error(f"❌ Błąd połączenia z HBase: {e}")
            return False
    
    def connect_sqlite(self, db_path=':memory:'):
        """Nawiązanie połączenia z SQLite"""
        try:
            self.sqlite_connection = sqlite3.connect(db_path)
            logger.info("✅ Połączono z SQLite")
            return True
        except Exception as e:
            logger.error(f"❌ Błąd połączenia z SQLite: {e}")
            return False
    
    def setup_hbase_table(self, table_name='social_media'):
        """Utworzenie tabeli HBase (bazując na Twoim kodzie)"""
        start_time = time.time()
        
        try:
            families = {'cf': dict()}  # Jedna rodzina kolumn jak w Twoim kodzie
            
            if table_name.encode() not in self.hbase_connection.tables():
                self.hbase_connection.create_table(table_name, families)
                logger.info(f"📊 Utworzono tabelę HBase: {table_name}")
            else:
                logger.info(f"📊 Tabela HBase '{table_name}' już istnieje")
            
            self.hbase_table = self.hbase_connection.table(table_name)
            
            setup_time = time.time() - start_time
            logger.info(f"⏱️ Czas konfiguracji HBase: {setup_time:.3f}s")
            return True
            
        except Exception as e:
            logger.error(f"❌ Błąd tworzenia tabeli HBase: {e}")
            return False
    
    def analyze_csv_structure(self, csv_file_path):
        """Analiza struktury pliku CSV i sprawdzenie duplikatów"""
        try:
            with open(csv_file_path, newline='', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                first_row = next(reader)
                columns = list(first_row.keys())
                
                logger.info(f"📋 Znalezione kolumny w CSV: {columns}")
                
                # Przykładowe dane z pierwszego wiersza
                logger.info("📝 Przykładowe dane:")
                for col, val in first_row.items():
                    logger.info(f"   {col}: {val}")
                
                # Sprawdzenie duplikatów w kolumnie Post ID
                csvfile.seek(0)  # Reset do początku pliku
                reader = csv.DictReader(csvfile)
                post_ids = []
                row_count = 0
                
                for row in reader:
                    post_ids.append(row.get('Post ID', ''))
                    row_count += 1
                    if row_count > 10000:  # Analiza tylko pierwszych 10k dla wydajności
                        break
                
                unique_post_ids = len(set(post_ids))
                total_post_ids = len(post_ids)
                duplicates = total_post_ids - unique_post_ids
                
                logger.info(f"📊 Analiza Post ID: {total_post_ids} rekordów, {unique_post_ids} unikalnych, {duplicates} duplikatów")
                
                if duplicates > 0:
                    logger.warning(f"⚠️ Wykryto {duplicates} duplikatów w Post ID - będą automatycznie obsłużone")
                
                return columns, first_row
        except Exception as e:
            logger.error(f"❌ Błąd analizy CSV: {e}")
            return [], {}

    def setup_sqlite_table(self, csv_file_path):
        """Utworzenie tabeli SQLite z automatycznym dostosowaniem do struktury CSV"""
        start_time = time.time()
        
        try:
            # Analiza struktury CSV
            columns, sample_row = self.analyze_csv_structure(csv_file_path)
            
            if not columns:
                logger.error("❌ Nie można przeanalizować struktury CSV")
                return False
            
            cursor = self.sqlite_connection.cursor()
            
            # Usunięcie tabeli jeśli istnieje
            cursor.execute("DROP TABLE IF EXISTS social_media")
            
            # Automatyczne tworzenie struktury tabeli na podstawie CSV
            column_definitions = []
            primary_key_set = False
            
            for col in columns:
                # Oczyszczenie nazwy kolumny (zamiana spacji na podkreślenia, małe litery)
                clean_col = col.lower().replace(' ', '_').replace('-', '_').replace('(', '').replace(')', '')
                
                # Określenie typu danych na podstawie przykładowej wartości
                sample_value = sample_row.get(col, '')
                
                # Tylko pierwszy klucz z ID będzie PRIMARY KEY
                if (col.upper() == 'POST ID' or 'post' in col.lower() and 'id' in col.lower()) and not primary_key_set:
                    column_definitions.append(f"{clean_col} TEXT PRIMARY KEY")
                    primary_key_set = True
                elif 'id' in col.lower():
                    # Pozostałe kolumny z ID będą zwykłymi kolumnami tekstowymi
                    column_definitions.append(f"{clean_col} TEXT")
                elif any(keyword in col.lower() for keyword in ['like', 'share', 'comment', 'view', 'reach', 'impression', 'follower']):
                    column_definitions.append(f"{clean_col} INTEGER")
                elif any(keyword in col.lower() for keyword in ['rate', 'score', 'percentage', 'ratio']):
                    column_definitions.append(f"{clean_col} REAL")
                elif any(keyword in col.lower() for keyword in ['age']):
                    column_definitions.append(f"{clean_col} INTEGER")
                else:
                    column_definitions.append(f"{clean_col} TEXT")
            
            # Utworzenie tabeli
            create_table_sql = f"""
                CREATE TABLE social_media (
                    {', '.join(column_definitions)}
                )
            """
            
            logger.info(f"🔨 Tworzenie tabeli SQL: {create_table_sql}")
            cursor.execute(create_table_sql)
            
            self.sqlite_connection.commit()
            setup_time = time.time() - start_time
            logger.info(f"✅ Utworzono tabelę SQLite w czasie: {setup_time:.3f}s")
            
            # Zapisanie mapowania kolumn dla późniejszego użycia
            self.column_mapping = {col: col.lower().replace(' ', '_').replace('-', '_').replace('(', '').replace(')', '') 
                                 for col in columns}
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Błąd tworzenia tabeli SQLite: {e}")
            return False
    
    def load_data_to_hbase(self, csv_file_path):
        """
        Wczytanie danych do HBase z pomiarem wydajności
        Bazuje na Twoim kodzie
        """
        logger.info("🚀 Rozpoczęcie wczytywania danych do HBase...")
        start_time = time.time()
        
        row_count = 0
        batch_size = 1000
        current_batch = 0
        
        try:
            with open(csv_file_path, newline='', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                
                # Analiza struktury danych
                first_row = next(reader)
                columns = list(first_row.keys())
                logger.info(f"📋 Kolumny w CSV: {columns}")
                
                # Reset iteratora
                csvfile.seek(0)
                reader = csv.DictReader(csvfile)
                
                batch = self.hbase_table.batch(batch_size=batch_size)
                
                for row in reader:
                    row_count += 1
                    
                    # Użycie Post ID jako klucz wiersza (jak w Twoim kodzie)
                    row_key = row.get('Post ID', f'post_{row_count}')
                    
                    # Przygotowanie danych - wszystkie kolumny w rodzinie 'cf'
                    data = {}
                    for k, v in row.items():
                        if k != 'Post ID':  # Pomijamy klucz
                            column_name = f'cf:{k}'.encode()
                            value = str(v).encode() if v else b''
                            data[column_name] = value
                    
                    batch.put(row_key.encode(), data)
                    
                    # Wyślij batch co określoną liczbę rekordów
                    if row_count % batch_size == 0:
                        batch.send()
                        current_batch += 1
                        logger.info(f"📦 Wysłano batch {current_batch} ({row_count} rekordów)")
                        batch = self.hbase_table.batch(batch_size=batch_size)
                
                # Wyślij pozostałe dane
                if row_count % batch_size != 0:
                    batch.send()
                    logger.info(f"📦 Wysłano ostatni batch ({row_count % batch_size} rekordów)")
        
        except Exception as e:
            logger.error(f"❌ Błąd wczytywania do HBase: {e}")
            return False
        
        end_time = time.time()
        write_time = end_time - start_time
        
        # Zapisanie metryki wydajności
        self.performance_metrics['hbase']['write_times'].append(write_time)
        self.data_stats['total_records'] = row_count
        
        logger.info(f"✅ Wczytano {row_count} rekordów do HBase w czasie: {write_time:.2f}s")
        logger.info(f"📈 Wydajność: {row_count/write_time:.1f} rekordów/s")
        
        return True
    
    def load_data_to_sqlite(self, csv_file_path):
        """Wczytanie danych do SQLite z automatycznym mapowaniem kolumn"""
        logger.info("🚀 Rozpoczęcie wczytywania danych do SQLite...")
        start_time = time.time()
        
        try:
            # Wczytanie CSV z automatycznym dostosowaniem
            df = pd.read_csv(csv_file_path)
            
            # Oczyszczenie nazw kolumn zgodnie z mapowaniem
            df_clean = df.copy()
            df_clean.columns = [col.lower().replace(' ', '_').replace('-', '_').replace('(', '').replace(')', '') 
                               for col in df.columns]
            
            logger.info(f"📊 Oryginalne kolumny: {list(df.columns)}")
            logger.info(f"📊 Oczyszczone kolumny: {list(df_clean.columns)}")
            
            # Sprawdzenie duplikatów w post_id
            if 'post_id' in df_clean.columns:
                duplicates = df_clean['post_id'].duplicated().sum()
                if duplicates > 0:
                    logger.warning(f"⚠️ Znaleziono {duplicates} duplikatów w post_id")
                    logger.info("🔧 Usuwanie duplikatów i tworzenie unikalnych kluczy...")
                    
                    # Opcja 1: Usunięcie duplikatów (zachowanie pierwszego wystąpienia)
                    # df_clean = df_clean.drop_duplicates(subset=['post_id'], keep='first')
                    
                    # Opcja 2: Dodanie unikalnego sufiksu do duplikatów
                    df_clean['row_number'] = range(len(df_clean))
                    df_clean['post_id'] = df_clean['post_id'].astype(str) + '_' + df_clean['row_number'].astype(str)
                    df_clean = df_clean.drop('row_number', axis=1)
                    
                    logger.info(f"✅ Po usunięciu duplikatów: {len(df_clean)} rekordów")
            
            # Wstawienie danych - pandas automatycznie dopasuje kolumny
            df_clean.to_sql('social_media', self.sqlite_connection, 
                           if_exists='append', index=False)
            
            row_count = len(df_clean)
            
        except Exception as e:
            logger.error(f"❌ Błąd wczytywania do SQLite: {e}")
            logger.error(f"📋 Dostępne kolumny w DataFrame: {list(df.columns) if 'df' in locals() else 'N/A'}")
            
            # Próba z alternatywną strukturą tabeli (bez PRIMARY KEY)
            logger.info("🔄 Próba utworzenia tabeli bez PRIMARY KEY...")
            try:
                # Usunięcie tabeli i utworzenie bez PRIMARY KEY
                cursor = self.sqlite_connection.cursor()
                cursor.execute("DROP TABLE IF EXISTS social_media")
                
                # Ponowne tworzenie z post_id jako zwykłą kolumną
                columns, sample_row = self.analyze_csv_structure(csv_file_path)
                column_definitions = []
                
                for col in columns:
                    clean_col = col.lower().replace(' ', '_').replace('-', '_').replace('(', '').replace(')', '')
                    
                    if any(keyword in col.lower() for keyword in ['like', 'share', 'comment', 'view', 'reach', 'impression', 'follower']):
                        column_definitions.append(f"{clean_col} INTEGER")
                    elif any(keyword in col.lower() for keyword in ['rate', 'score', 'percentage', 'ratio']):
                        column_definitions.append(f"{clean_col} REAL")
                    elif any(keyword in col.lower() for keyword in ['age']):
                        column_definitions.append(f"{clean_col} INTEGER")
                    else:
                        column_definitions.append(f"{clean_col} TEXT")
                
                # Dodanie automatycznego klucza głównego
                create_table_sql = f"""
                    CREATE TABLE social_media (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        {', '.join(column_definitions)}
                    )
                """
                
                cursor.execute(create_table_sql)
                self.sqlite_connection.commit()
                logger.info("✅ Utworzono tabelę z automatycznym kluczem głównym")
                
                # Ponowna próba wstawienia danych
                df_clean.to_sql('social_media', self.sqlite_connection, 
                               if_exists='append', index=False)
                row_count = len(df_clean)
                
            except Exception as e2:
                logger.error(f"❌ Błąd alternatywnej próby: {e2}")
                return False
        
        end_time = time.time()
        write_time = end_time - start_time
        
        # Zapisanie metryki wydajności
        self.performance_metrics['sqlite']['write_times'].append(write_time)
        
        logger.info(f"✅ Wczytano {row_count} rekordów do SQLite w czasie: {write_time:.2f}s")
        logger.info(f"📈 Wydajność: {row_count/write_time:.1f} rekordów/s")
        
        return True
    
    def benchmark_read_operations(self, sample_size=100):
        """
        Test wydajności operacji odczytu
        Scenariusz 1: Odczyt losowych rekordów
        """
        logger.info("🔍 Test wydajności odczytu...")
        
        # Pobranie listy kluczy z HBase
        keys = []
        for key, _ in self.hbase_table.scan(limit=sample_size * 2):
            keys.append(key.decode())
        
        if not keys:
            logger.warning("⚠️ Brak danych w HBase do testowania")
            return {'hbase': 0, 'sqlite': 0, 'sample_size': 0}
        
        # Losowy wybór kluczy do testowania
        test_keys = np.random.choice(keys, min(sample_size, len(keys)), replace=False)
        
        # Test HBase
        start_time = time.time()
        for key in test_keys:
            row = self.hbase_table.row(key.encode())
        hbase_read_time = time.time() - start_time
        
        # Test SQLite - używamy pierwszej kolumny z ID lub unikalnego identyfikatora
        start_time = time.time()
        cursor = self.sqlite_connection.cursor()
        
        # Sprawdź strukturę tabeli SQLite
        cursor.execute("PRAGMA table_info(social_media)")
        columns_info = cursor.fetchall()
        column_names = [col[1] for col in columns_info]
        
        if 'post_id' in column_names:
            # Użyj post_id jeśli dostępne
            for key in test_keys:
                cursor.execute("SELECT * FROM social_media WHERE post_id LIKE ?", (f'{key}%',))
                row = cursor.fetchone()
        else:
            # Alternatywnie użyj id (autoincrement) - losowe wybieranie
            max_id_result = cursor.execute("SELECT MAX(id) FROM social_media").fetchone()
            max_id = max_id_result[0] if max_id_result[0] else len(test_keys)
            random_ids = np.random.randint(1, min(max_id + 1, len(test_keys) + 1), len(test_keys))
            
            for rand_id in random_ids:
                cursor.execute("SELECT * FROM social_media WHERE id = ?", (int(rand_id),))
                row = cursor.fetchone()
                
        sqlite_read_time = time.time() - start_time
        
        # Zapisanie metryk
        self.performance_metrics['hbase']['read_times'].append(hbase_read_time)
        self.performance_metrics['sqlite']['read_times'].append(sqlite_read_time)
        
        logger.info(f"📊 HBase odczyt {len(test_keys)} rekordów: {hbase_read_time:.3f}s")
        logger.info(f"📊 SQLite odczyt {len(test_keys)} rekordów: {sqlite_read_time:.3f}s")
        
        return {
            'hbase': hbase_read_time,
            'sqlite': sqlite_read_time,
            'sample_size': len(test_keys)
        }
    
    def benchmark_query_operations(self):
        """
        Test wydajności zapytań filtrujących
        Scenariusz 2: Zapytania według platformy
        """
        logger.info("🔎 Test wydajności zapytań...")
        
        platforms = ['Instagram', 'Facebook', 'Twitter', 'TikTok', 'LinkedIn']
        
        for platform in platforms:
            # Test HBase - skanowanie z filtrowaniem
            start_time = time.time()
            hbase_results = []
            for key, data in self.hbase_table.scan():
                platform_value = data.get(b'cf:Platform', b'').decode()
                if platform_value == platform:
                    hbase_results.append(key)
            hbase_query_time = time.time() - start_time
            
            # Test SQLite - zapytanie SQL
            start_time = time.time()
            cursor = self.sqlite_connection.cursor()
            cursor.execute("SELECT * FROM social_media WHERE platform = ?", (platform,))
            sqlite_results = cursor.fetchall()
            sqlite_query_time = time.time() - start_time
            
            # Zapisanie metryk tylko jeśli znaleziono wyniki
            if hbase_results or sqlite_results:
                self.performance_metrics['hbase']['query_times'].append(hbase_query_time)
                self.performance_metrics['sqlite']['query_times'].append(sqlite_query_time)
                
                logger.info(f"🔍 {platform} - HBase: {len(hbase_results)} wyników w {hbase_query_time:.3f}s")
                logger.info(f"🔍 {platform} - SQLite: {len(sqlite_results)} wyników w {sqlite_query_time:.3f}s")
            else:
                logger.info(f"ℹ️ {platform} - Brak danych dla tej platformy")
    
    def benchmark_aggregation_operations(self):
        """
        Test wydajności operacji agregacyjnych
        Scenariusz 3: Statystyki według platform
        """
        logger.info("📈 Test wydajności agregacji...")
        
        # Test HBase - agregacja manualna
        start_time = time.time()
        platform_stats = {}
        
        for key, data in self.hbase_table.scan():
            platform = data.get(b'cf:Platform', b'').decode()
            likes = int(data.get(b'cf:Likes', b'0').decode() or 0)
            
            if platform not in platform_stats:
                platform_stats[platform] = {'total_likes': 0, 'count': 0}
            
            platform_stats[platform]['total_likes'] += likes
            platform_stats[platform]['count'] += 1
        
        hbase_agg_time = time.time() - start_time
        
        # Test SQLite - agregacja SQL
        start_time = time.time()
        cursor = self.sqlite_connection.cursor()
        cursor.execute("""
            SELECT platform, 
                   SUM(likes) as total_likes,
                   COUNT(*) as count,
                   AVG(likes) as avg_likes
            FROM social_media 
            WHERE likes IS NOT NULL
            GROUP BY platform
        """)
        sqlite_results = cursor.fetchall()
        sqlite_agg_time = time.time() - start_time
        
        # Zapisanie metryk
        self.performance_metrics['hbase']['aggregation_times'].append(hbase_agg_time)
        self.performance_metrics['sqlite']['aggregation_times'].append(sqlite_agg_time)
        
        logger.info(f"📊 HBase agregacja: {hbase_agg_time:.3f}s")
        logger.info(f"📊 SQLite agregacja: {sqlite_agg_time:.3f}s")
        
        return {
            'hbase': {'time': hbase_agg_time, 'results': platform_stats},
            'sqlite': {'time': sqlite_agg_time, 'results': sqlite_results}
        }
    
    def generate_performance_report(self):
        """Generowanie raportu wydajności"""
        
        def safe_mean(lst):
            return np.mean(lst) if lst else 0
        
        report = {
            'summary': {
                'total_records': self.data_stats['total_records'],
                'hbase_metrics': {
                    'avg_write_time': safe_mean(self.performance_metrics['hbase']['write_times']),
                    'avg_read_time': safe_mean(self.performance_metrics['hbase']['read_times']),
                    'avg_query_time': safe_mean(self.performance_metrics['hbase']['query_times']),
                    'avg_aggregation_time': safe_mean(self.performance_metrics['hbase']['aggregation_times'])
                },
                'sqlite_metrics': {
                    'avg_write_time': safe_mean(self.performance_metrics['sqlite']['write_times']),
                    'avg_read_time': safe_mean(self.performance_metrics['sqlite']['read_times']),
                    'avg_query_time': safe_mean(self.performance_metrics['sqlite']['query_times']),
                    'avg_aggregation_time': safe_mean(self.performance_metrics['sqlite']['aggregation_times'])
                }
            },
            'detailed_metrics': self.performance_metrics
        }
        
        return report
    
    def visualize_results(self):
        """Tworzenie wykresów porównawczych"""
        fig, axes = plt.subplots(2, 2, figsize=(15, 10))
        
        # Porównanie czasów zapisu
        write_data = [
            self.performance_metrics['hbase']['write_times'],
            self.performance_metrics['sqlite']['write_times']
        ]
        if any(write_data):
            axes[0, 0].bar(['HBase', 'SQLite'], 
                          [np.mean(write_data[0]) if write_data[0] else 0,
                           np.mean(write_data[1]) if write_data[1] else 0])
            axes[0, 0].set_title('Średni czas zapisu danych')
            axes[0, 0].set_ylabel('Czas (sekundy)')
        
        # Porównanie czasów odczytu
        read_data = [
            self.performance_metrics['hbase']['read_times'],
            self.performance_metrics['sqlite']['read_times']
        ]
        if any(read_data):
            axes[0, 1].bar(['HBase', 'SQLite'],
                          [np.mean(read_data[0]) if read_data[0] else 0,
                           np.mean(read_data[1]) if read_data[1] else 0])
            axes[0, 1].set_title('Średni czas odczytu rekordów')
            axes[0, 1].set_ylabel('Czas (sekundy)')
        
        # Porównanie czasów zapytań
        query_data = [
            self.performance_metrics['hbase']['query_times'],
            self.performance_metrics['sqlite']['query_times']
        ]
        if any(query_data):
            axes[1, 0].boxplot([q for q in query_data if q], 
                              labels=['HBase', 'SQLite'])
            axes[1, 0].set_title('Rozkład czasów zapytań')
            axes[1, 0].set_ylabel('Czas (sekundy)')
        
        # Porównanie czasów agregacji
        agg_data = [
            self.performance_metrics['hbase']['aggregation_times'],
            self.performance_metrics['sqlite']['aggregation_times']
        ]
        if any(agg_data):
            axes[1, 1].bar(['HBase', 'SQLite'],
                          [np.mean(agg_data[0]) if agg_data[0] else 0,
                           np.mean(agg_data[1]) if agg_data[1] else 0])
            axes[1, 1].set_title('Średni czas operacji agregacyjnych')
            axes[1, 1].set_ylabel('Czas (sekundy)')
        
        plt.tight_layout()
        plt.savefig('hbase_vs_sqlite_performance.png', dpi=300, bbox_inches='tight')
        plt.show()
    
    def run_full_experiment(self, csv_file_path):
        """Uruchomienie pełnego eksperymentu"""
        logger.info("🎯 Rozpoczęcie pełnego eksperymentu wydajności...")
        
        # Nawiązanie połączeń
        if not self.connect_hbase() or not self.connect_sqlite():
            return False
        
        # Konfiguracja tabel
        if not self.setup_hbase_table() or not self.setup_sqlite_table(csv_file_path):
            return False
        
        # Wczytanie danych
        if not self.load_data_to_hbase(csv_file_path) or not self.load_data_to_sqlite(csv_file_path):
            return False
        
        # Eksperymenty wydajnościowe
        self.benchmark_read_operations()
        self.benchmark_query_operations()
        self.benchmark_aggregation_operations()
        
        # Generowanie raportu
        report = self.generate_performance_report()
        
        logger.info("📋 RAPORT KOŃCOWY:")
        print(json.dumps(report['summary'], indent=2))
        
        # Wizualizacja
        self.visualize_results()
        
        # Zamknięcie połączeń
        self.cleanup()
        
        return report
    
    def cleanup(self):
        """Zamknięcie połączeń"""
        if self.hbase_connection:
            self.hbase_connection.close()
        if self.sqlite_connection:
            self.sqlite_connection.close()
        logger.info("🔒 Zamknięto wszystkie połączenia")

def main():
    """Główna funkcja eksperymentu"""
    
    # Ścieżka do pliku CSV (zmień na właściwą)
    csv_file_path = 'social_media_engagement_data.csv'
    
    # Utworzenie analizatora i uruchomienie eksperymentu
    analyzer = SocialMediaPerformanceAnalyzer()
    
    try:
        report = analyzer.run_full_experiment(csv_file_path)
        
        if report:
            logger.info("✅ Eksperyment zakończony pomyślnie!")
            
            # Zapisanie raportu do pliku
            with open('performance_report.json', 'w') as f:
                json.dump(report, f, indent=2)
            logger.info("💾 Raport zapisany do performance_report.json")
        else:
            logger.error("❌ Eksperyment nie powiódł się")
            
    except Exception as e:
        logger.error(f"❌ Błąd podczas eksperymentu: {e}")
    finally:
        analyzer.cleanup()

if __name__ == "__main__":
    main()