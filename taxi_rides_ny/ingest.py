import duckdb
import requests
from pathlib import Path
import os # Dodane: Potrzebne do niektórych operacji na ścieżkach, choć pathlib często wystarcza

BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download"

def download_and_convert_files(taxi_type):
    data_dir = Path("data") / taxi_type
    data_dir.mkdir(exist_ok=True, parents=True)

    for year in [2019, 2020]:
        for month in range(1, 13):
            parquet_filename = f"{taxi_type}_tripdata_{year}-{month:02d}.parquet"
            parquet_filepath = data_dir / parquet_filename

            # Dodane logowanie: Sprawdzenie, czy plik Parquet już istnieje
            if parquet_filepath.exists():
                print(f"Skipping {parquet_filename} (already exists)")
                continue

            # Download CSV.gz file
            csv_gz_filename = f"{taxi_type}_tripdata_{year}-{month:02d}.csv.gz"
            csv_gz_filepath = data_dir / csv_gz_filename

            # Dodane logowanie i obsługa błędów dla pobierania
            print(f"Attempting to download {csv_gz_filename}...")
            try:
                response = requests.get(f"{BASE_URL}/{taxi_type}/{csv_gz_filename}", stream=True)
                response.raise_for_status() # Zgłosi wyjątek dla kodów statusu 4xx/5xx (błędy serwera/klienta)
            except requests.exceptions.RequestException as e:
                print(f"ERROR: Failed to download {csv_gz_filename}: {e}")
                # Kontynuujemy do następnego pliku, jeśli pobieranie się nie powiodło
                # Możesz zmienić to na 'raise' jeśli chcesz, aby skrypt zatrzymywał się przy pierwszym błędzie pobierania
                continue

            with open(csv_gz_filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            print(f"Downloaded {csv_gz_filename} successfully.")

            # Dodane logowanie i obsługa błędów dla konwersji do Parquet
            print(f"Converting {csv_gz_filename} to Parquet...")
            con_temp = duckdb.connect() # Użyj tymczasowego połączenia dla konwersji
            try:
                con_temp.execute(f"""
                    COPY (SELECT * FROM read_csv_auto('{csv_gz_filepath}'))
                    TO '{parquet_filepath}' (FORMAT PARQUET)
                """)
                print(f"Converted {csv_gz_filename} to Parquet successfully.")
            except Exception as e:
                print(f"ERROR: Failed to convert {csv_gz_filename} to Parquet: {e}")
                # Możesz dodać 'raise' tutaj, jeśli chcesz, aby skrypt zatrzymywał się przy błędach konwersji
            finally:
                con_temp.close() # Zawsze zamykaj połączenie

            # Remove the CSV.gz file to save space
            csv_gz_filepath.unlink()
            print(f"Completed processing for {parquet_filename}")

def update_gitignore():
    gitignore_path = Path(".gitignore")

    # Read existing content or start with empty string
    content = gitignore_path.read_text() if gitignore_path.exists() else ""

    # Upewnij się, że .gitignore kończy się znakiem nowej linii
    if content and not content.endswith('\n'):
        content += '\n'

    # Dodaj 'data/' jeśli nieobecne
    if 'data/' not in content:
        with open(gitignore_path, 'a') as f:
            f.write('\n# Data directory\ndata/\n')

    # Dodaj nazwę pliku .duckdb jeśli nieobecne
    db_filename = "taxi_rides_ny.duckdb"
    if db_filename not in content:
        with open(gitignore_path, 'a') as f:
            f.write(f'\n# DuckDB database file\n{db_filename}\n')

if __name__ == "__main__":
    # Update .gitignore to exclude data directory
    update_gitignore()

    print("--- Starting data ingestion script ---")

    print("Starting download and conversion of CSV.gz to Parquet files...")
    for taxi_type in ["yellow", "green"]:
        download_and_convert_files(taxi_type)
    print("Finished download and conversion of CSV.gz to Parquet files.")

    # --- KLUCZOWA SEKCJA: Tworzenie tabel DuckDB z plików Parquet ---
    db_file_name = "taxi_rides_ny.duckdb"
    # Upewnij się, że ścieżka do bazy danych jest prawidłowa względem miejsca uruchomienia skryptu.
    # W Twoim przypadku (uruchamiasz z taxi_rides_ny/), po prostu nazwa pliku jest ok.
    # Jeśli uruchamiałabyś z katalogu nadrzędnego, musiałabyś podać np. 'taxi_rides_ny/' + db_file_name
    db_file_path = Path(db_file_name) 
    
    con_main = None # Inicjalizacja połączenia na None, aby móc je zamknąć w finally
    try:
        print(f"Attempting to connect to DuckDB database: {db_file_path.resolve()}...") # resolve() pokaże pełną ścieżkę
        con_main = duckdb.connect(str(db_file_path)) # str() jest potrzebne, bo connect() oczekuje stringa
        print("Connected to DuckDB successfully.")

        print("Creating schema 'prod' if it does not exist...")
        con_main.execute("CREATE SCHEMA IF NOT EXISTS prod")
        print("Schema 'prod' created/verified.")

        for taxi_type in ["yellow", "green"]:
            # Pattern do odczytu plików Parquet
            # Ważne: to musi być ścieżka względna do miejsca, gdzie są pliki 'data/'
            parquet_files_pattern = str(Path('data') / taxi_type / '*.parquet')

            # Dodane: Sprawdź, czy istnieją pliki Parquet dla danego typu taksówki
            # list(Path('.').glob(...)) - sprawdza, czy są pliki pasujące do wzorca w bieżącym katalogu
            if not list(Path(parquet_files_pattern).parent.glob(Path(parquet_files_pattern).name)):
                print(f"WARNING: No parquet files found for {taxi_type} at '{parquet_files_pattern}'. Skipping table creation for this type.")
                continue

            sql_query = f"""
                CREATE OR REPLACE TABLE prod.{taxi_type}_tripdata AS
                SELECT * FROM read_parquet('{parquet_files_pattern}', union_by_name=true)
            """
            print(f"Executing SQL for prod.{taxi_type}_tripdata: {sql_query}")
            try:
                con_main.execute(sql_query)
                print(f"Table prod.{taxi_type}_tripdata created/replaced successfully.")
                # Dodatkowa weryfikacja: Sprawdź liczbę wierszy
                row_count = con_main.execute(f"SELECT COUNT(*) FROM prod.{taxi_type}_tripdata").fetchone()[0]
                print(f"Table prod.{taxi_type}_tripdata contains {row_count} rows.")
            except Exception as e:
                print(f"ERROR: Failed to create/replace table prod.{taxi_type}_tripdata: {e}")
                # Jeśli błąd podczas tworzenia tabeli, możesz chcieć zatrzymać skrypt:
                # raise e
    except Exception as e:
        print(f"CRITICAL ERROR: Issue with DuckDB connection or schema/table creation: {e}")
        # W przypadku krytycznego błędu, zatrzymaj skrypt
        # raise e
    finally:
        if con_main:
            con_main.close()
            print("DuckDB connection closed.")
    print("--- Data ingestion script finished ---")
