import duckdb
import requests
from pathlib import Path

BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download"


def download_and_convert_files(taxi_type: str, years: list[int]):
    """
    Pobiera pliki CSV.gz dla podanego taxi_type i lat z GitHuba,
    konwertuje je do Parquet i zapisuje w data/<taxi_type>/.
    """
    data_dir = Path("data") / taxi_type
    data_dir.mkdir(exist_ok=True, parents=True)

    for year in years:
        for month in range(1, 13):
            parquet_filename = f"{taxi_type}_tripdata_{year}-{month:02d}.parquet"
            parquet_filepath = data_dir / parquet_filename

            if parquet_filepath.exists():
                print(f"Skipping {parquet_filename} (already exists)")
                continue

            csv_gz_filename = f"{taxi_type}_tripdata_{year}-{month:02d}.csv.gz"
            csv_gz_filepath = data_dir / csv_gz_filename

            url = f"{BASE_URL}/{taxi_type}/{csv_gz_filename}"
            print(f"Downloading {url} ...")
            response = requests.get(url, stream=True)
            try:
                response.raise_for_status()
            except requests.HTTPError as e:
                print(f"ERROR downloading {csv_gz_filename}: {e}")
                # Jeśli pliku nie ma na serwerze (404), przechodzimy dalej
                continue

            with open(csv_gz_filepath, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

            print(f"Converting {csv_gz_filename} to Parquet...")
            con = duckdb.connect()
            con.execute(
                f"""
                COPY (SELECT * FROM read_csv_auto('{csv_gz_filepath}'))
                TO '{parquet_filepath}' (FORMAT PARQUET)
                """
            )
            con.close()

            csv_gz_filepath.unlink()
            print(f"Completed {parquet_filename}")


def update_gitignore():
    gitignore_path = Path(".gitignore")
    content = gitignore_path.read_text() if gitignore_path.exists() else ""

    lines_to_add = []
    if "data/" not in content:
        lines_to_add.append("# Data directory\ndata/")
    if "taxi_rides_ny.duckdb" not in content:
        lines_to_add.append("# DuckDB database file\ntaxi_rides_ny.duckdb")

    if lines_to_add:
        with open(gitignore_path, "a") as f:
            if content and not content.endswith("\n"):
                f.write("\n")
            f.write("\n".join(lines_to_add) + "\n")


if __name__ == "__main__":
    # Zaktualizuj .gitignore
    update_gitignore()

    print("--- Starting data ingestion script ---")

    # Yellow & green: lata 2019–2020
    for taxi_type in ["yellow", "green"]:
        download_and_convert_files(taxi_type, years=[2019, 2020])

    # FHV: tylko 2019
    download_and_convert_files("fhv", years=[2019])

    # Tworzenie / aktualizacja tabel w DuckDB
    db_path = "taxi_rides_ny.duckdb"
    con = duckdb.connect(db_path)
    print(f"Connected to DuckDB at {Path(db_path).resolve()}")

    con.execute("CREATE SCHEMA IF NOT EXISTS prod")

    # Yellow, green, fhv – każda w swojej tabeli
    for taxi_type in ["yellow", "green", "fhv"]:
        pattern = f"data/{taxi_type}/*.parquet"
        print(f"Creating prod.{taxi_type}_tripdata from {pattern} ...")
        con.execute(
            f"""
            CREATE OR REPLACE TABLE prod.{taxi_type}_tripdata AS
            SELECT * FROM read_parquet('{pattern}', union_by_name=true)
            """
        )
        row_count = con.execute(
            f"SELECT COUNT(*) FROM prod.{taxi_type}_tripdata"
        ).fetchone()[0]
        print(f"Table prod.{taxi_type}_tripdata contains {row_count} rows.")

    con.close()
    print("--- Data ingestion script finished ---")
