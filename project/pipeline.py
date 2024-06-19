import numpy as np
import pandas as pd
import sqlite3
import cftime
import requests
import tempfile
from netCDF4 import Dataset


# Temperature anomaly dataset from HADCRUT5
hadcrut5_data_url = "https://crudata.uea.ac.uk/cru/data/temperature/HadCRUT.5.0.2.0.analysis.anomalies.ensemble_mean.nc#mode=bytes"
# CO2 emission dataset from Our World in Data
co2_data_url = "https://raw.githubusercontent.com/owid/co2-data/master/owid-co2-data.csv"

def load_temperature_dataset(url):
    # The original approach (Dataset(url)) caused SSL certificate verification errors in GitHub Actions
    # Thats why the download of hadcrut5 dataset was changed to requests to bypass the SSL verfication issue encountered in CI environment
    response = requests.get(url, verify=False)
    response.raise_for_status()
    with tempfile.NamedTemporaryFile(suffix=".nc", delete=False) as tmp_file:
        tmp_file.write(response.content)
        tmp_file_path = tmp_file.name
    temperature_dataset = Dataset(tmp_file_path)
    return temperature_dataset

def convert_time_units(time_variable):
    time_units = time_variable.units
    time_values = time_variable[:]
    dates = cftime.num2date(time_values, units=time_units, calendar="standard")
    return pd.to_datetime([date.strftime("%Y-%m-%d %H:%M:%S") for date in dates])

def get_relevant_data_and_setup_df(dataset):
    time = convert_time_units(dataset.variables["time"])
    latitudes = dataset.variables["latitude"][:]
    longitudes = dataset.variables["longitude"][:]
    tas_mean = dataset.variables["tas_mean"][:]
    
    time_repeated = np.repeat(time, len(latitudes) * len(longitudes))
    latitudes_tiled = np.tile(np.repeat(latitudes, len(longitudes)), len(time))
    longitudes_tiled = np.tile(longitudes, len(time) * len(latitudes))
    tas_mean_flattened = tas_mean.flatten()

    temperature_df = pd.DataFrame({
        "time": time_repeated,
        "latitude": latitudes_tiled,
        "longitude": longitudes_tiled,
        "temperature_anomaly": tas_mean_flattened
    })
    
    return temperature_df

def save_to_sqlite(df, db_path, table_name):
    conn = sqlite3.connect(db_path)
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    conn.close()

# Dataset 1
unprocessed_temperature_dataset = load_temperature_dataset(hadcrut5_data_url)
temperature_dataset = get_relevant_data_and_setup_df(unprocessed_temperature_dataset)
save_to_sqlite(temperature_dataset, "../data/temperature_dataset.sqlite", "temperature_anomalies")

# Dataset 2
co2_data = pd.read_csv(co2_data_url)
key_columns = ["country", "year", "co2", "co2_per_capita", "co2_per_gdp", "temperature_change_from_co2"]
co2_data_filtered = co2_data[key_columns]
save_to_sqlite(co2_data_filtered, "../data/co2_emission_dataset.sqlite", "co2_emissions")