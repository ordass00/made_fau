import os
import sqlite3
import pandas as pd


def test_db_file_exists(db_path):
    assert os.path.exists(db_path), f"Database {db_path} does not exist."

def test_table_exists(db_path, table_name):
    conn = sqlite3.connect(db_path)
    query = f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';"
    result = pd.read_sql_query(query, conn)
    conn.close()
    assert not result.empty, f"Table {table_name} does not exist in {db_path}."

def test_table_columns(db_path, table_name, expected_columns):
    conn = sqlite3.connect(db_path)
    query = f"PRAGMA table_info({table_name});"
    result = pd.read_sql_query(query, conn)
    conn.close()
    columns = result["name"].tolist()
    assert columns == expected_columns, f"Columns do not match. Expected: {expected_columns}. Got: {columns}."

def test_table_has_data(db_path, table_name):
    conn = sqlite3.connect(db_path)
    query = f"SELECT * FROM {table_name};"
    result = pd.read_sql_query(query, conn)
    conn.close()
    assert not result.empty, f"Table {table_name} in {db_path} is empty."

def test_latitude_longitude_ranges(db_path, table_name):
    conn = sqlite3.connect(db_path)
    query = f"SELECT latitude, longitude FROM {table_name};"
    result = pd.read_sql_query(query, conn)
    conn.close()
    assert result["latitude"].between(-87.5, 87.5).all(), "Latitude values are not in the range between -87.5 and +87.5"
    assert result["longitude"].between(-177.5, 177.5).all(), "Longitude values are not in the range between -177.5 and +177.5"

def test_latitude_longitude_unique_values(db_path, table_name):
    conn = sqlite3.connect(db_path)
    query = f"SELECT latitude, longitude FROM {table_name};"
    result = pd.read_sql_query(query, conn)
    conn.close()
    unique_latitudes = result["latitude"].nunique()
    unique_longitudes = result["longitude"].nunique()
    assert unique_latitudes == 36, f"Expected 36 unique latitudes. Got {unique_latitudes}."
    assert unique_longitudes == 72, f"Expected 72 unique longitudes. Got {unique_longitudes}."

def test_time_format(db_path, table_name):
    conn = sqlite3.connect(db_path)
    query = f"SELECT time FROM {table_name};"
    result = pd.read_sql_query(query, conn)
    conn.close()
    assert pd.to_datetime(result["time"], format="%Y-%m-%d %H:%M:%S", errors="coerce").notna().all(), "Time column does not have the required datetime format '%Y-%m-%d %H:%M:%S'"

def test_non_negative_co2_values(db_path, table_name):
    conn = sqlite3.connect(db_path)
    query = f"SELECT co2, co2_per_capita, co2_per_gdp, temperature_change_from_co2 FROM {table_name};"
    result = pd.read_sql_query(query, conn)
    conn.close()
    co2_columns = ["co2", "co2_per_capita", "co2_per_gdp", "temperature_change_from_co2"]
    for column in co2_columns:
        non_null_co2 = result[column].dropna()
        assert (non_null_co2 >= 0).all(), f"{column} values cannot be negative."

def test_non_empty_country_values(db_path, table_name):
    conn = sqlite3.connect(db_path)
    query = f"SELECT country FROM {table_name};"
    result = pd.read_sql_query(query, conn)
    conn.close()
    assert result["country"].notna().all(), "Country values cannot be empty."

def main():
    co2_emission_db_path = "../data/co2_emission_dataset.sqlite"
    temperature_db_path = "../data/temperature_dataset.sqlite"

    co2_emission_table_name = "co2_emissions"
    temperature_table_name = "temperature_anomalies"

    expected_columns_for_co2_emission = ["country", "year", "co2", "co2_per_capita", "co2_per_gdp", "temperature_change_from_co2"]
    expected_columns_for_temperature = ["time", "latitude", "longitude", "temperature_anomaly"]
    
    test_db_file_exists(co2_emission_db_path)
    test_db_file_exists(temperature_db_path)

    test_table_exists(co2_emission_db_path, co2_emission_table_name)
    test_table_exists(temperature_db_path, temperature_table_name)

    test_table_columns(co2_emission_db_path, co2_emission_table_name, expected_columns_for_co2_emission)
    test_table_columns(temperature_db_path, temperature_table_name, expected_columns_for_temperature)
    
    test_table_has_data(co2_emission_db_path, co2_emission_table_name)
    test_table_has_data(temperature_db_path, temperature_table_name)

    test_latitude_longitude_ranges(temperature_db_path, temperature_table_name)
    test_latitude_longitude_unique_values(temperature_db_path, temperature_table_name)

    test_time_format(temperature_db_path, temperature_table_name)

    test_non_negative_co2_values(co2_emission_db_path, co2_emission_table_name)
    test_non_empty_country_values(co2_emission_db_path, co2_emission_table_name)
    
    print("All tests passed successfully.")
    

if __name__ == "__main__":
    main()