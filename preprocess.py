import pandas as pd
import os

# Raw data, metadata and clean data directories and files
raw_data_directory = os.path.join("raw_data")
raw_data_files = os.listdir(raw_data_directory)

meta_data_directory = os.path.join("meta_data")
meta_data_files = os.listdir(meta_data_directory)

clean_data_directory = os.path.join("clean_data")

print("------------ ℹ️ DATA LOCATION ------------")
print(f"Raw data directory: {raw_data_directory}")
print(f"Raw data files: {raw_data_files}\n")

print(f"Metadata directory: {meta_data_directory}")
print(f"Metadata files: {meta_data_files}")

print(f"Clean data directory: {clean_data_directory}")
print("-----------------------------------------")

# Read metadata files
# TARIC codes
taric_path = os.path.join(meta_data_directory, "TARIC.csv")
taric = pd.read_csv(
    taric_path, delimiter="\t", dtype={"cod_taric": "string"}, encoding="utf-16"
)
# HS codes
hscode_path = os.path.join(meta_data_directory, "hscodes.csv")
hscode = pd.read_csv(hscode_path, delimiter=",", dtype={"hscode": "string"})
# Country codes
countries_path = os.path.join(meta_data_directory, "COUNTRIES.csv")
countries = pd.read_csv(countries_path, delimiter="\t", encoding="utf-16")
# Province code
provinces_path = os.path.join(meta_data_directory, "PROVINCIAS.csv")
provinces = pd.read_csv(provinces_path, delimiter="\t", encoding="utf-16")

print("✅ Metada files read succesfully.")

country_code = 400  # USA

# Read raw data
for raw_data_file in raw_data_files:
    if raw_data_file.rsplit(".")[-1] == "csv":
        raw_data_path = os.path.join(raw_data_directory, raw_data_file)
        raw_data = pd.read_csv(
            raw_data_path,
            dtype={"cod_taric": "string"},
            delimiter="\t",
            encoding="utf-16",
        )  # specify data type for taric to avoid removing preceding 0s

        clean_file_name = (
            raw_data_file.rsplit(".")[0] + "_" + str(country_code) + ".csv"
        )
        clean_file_path = os.path.join(clean_data_directory, clean_file_name)

        clean_data = raw_data[raw_data["pais"] == country_code]
        clean_data.to_csv(clean_file_path, index=False)

        print(f"✅ File {raw_data_file} processed and saved as {clean_file_name}")
    else:
        print(f"❌ WARNING! Could not convert file {raw_data_file}")

# Join all cleaned data into a single dataframe called 'comex_total'
clean_data_files = os.listdir(clean_data_directory)
comex_total = pd.DataFrame()

print(f"⏳ Joining all cleaned data...")

for clean_data_file in clean_data_files:
    comex_partial = pd.read_csv(
        os.path.join(clean_data_directory, clean_data_file),
        dtype={"cod_taric": "string"},
    )  # specify type for taric to avoid removing 0s
    comex_total = pd.concat([comex_total, comex_partial])

comex_total.rename(
    columns={"pais": "cod_pais", "provincia": "cod_provincia"}, inplace=True
)  # rename columns to match convention

print(f"✅ Cleaned data has been succesfully joined")

# Join metadata to dataframe 'comex_total'
print(f"⏳ Adding metadata...")
comex_total = comex_total.merge(countries, how="inner", on="cod_pais")
comex_total = comex_total.merge(provinces, how="inner", on="cod_provincia")
comex_total = comex_total.merge(taric, how="left", on="cod_taric")
comex_total = comex_total.merge(
    hscode, how="left", left_on="cod_taric", right_on="hscode"
)

comex_total.rename(
    columns={
        "nivel_taric_x": "nivel_taric",
        "taric": "taric_esp",
        "description": "taric_eng",
    },
    inplace=True,
)
comex_total.drop(columns=["nivel_taric_y", "hscode"], inplace=True)
print(f"✅ Metadata has been succesfully added")

# Export cleaned datafram 'comex_total' to .csv file
print(f"⏳ Exporting clean .csv file...")
comex_total.to_csv(os.path.join("comex_clean.csv"), index=False)
print(f"✅ File 'comex_clean.csv' with clean data is ready to be used on Tableau")
