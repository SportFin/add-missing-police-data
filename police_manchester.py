import pandas as pd
import requests
import json
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed


def fetch_crime_data(row, month):
    api_url = f"https://data.police.uk/api/crimes-at-location?date={month}&lat={row['latitude']}&lng={row['longitude']}"
    try:
        response = requests.get(api_url, timeout=10)
        if response.status_code == 200:
            return json.loads(response.text)
        else:
            print(
                f"API request failed for {month}: Latitude {row['latitude']}, Longitude {row['longitude']}, Status code: {response.status_code}"
            )
    except requests.RequestException as e:
        print(
            f"Request exception for {month}: Latitude {row['latitude']}, Longitude {row['longitude']}: {e}"
        )
    return []


# Load the Manchester data
manchester_data = pd.read_csv("manchester_missing_long_lat.csv")

# Iterate over months from October 2023 to January 2023
months = [
    "2023-09",
    "2023-08",
    "2023-07",
    "2023-06",
    "2023-05",
    "2023-04",
    "2023-03",
    "2023-02",
    "2023-01",
]
for month in tqdm(months):
    compiled_crime_data = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_row = {
            executor.submit(fetch_crime_data, row, month): row
            for index, row in manchester_data.iterrows()
        }
        for future in as_completed(future_to_row):
            crimes = future.result()
            for crime in crimes:
                crime_entry = {
                    "Crime ID": crime.get("id", ""),
                    "Month": crime.get("month", month),
                    "Reported by": "Not specified",
                    "Falls within": "Not specified",
                    "Longitude": crime["location"]["longitude"],
                    "Latitude": crime["location"]["latitude"],
                    "Location": crime["location"]["street"]["name"],
                    "LSOA code": "",
                    "LSOA name": "",
                    "Crime type": crime["category"],
                    # Check if 'outcome_status' is not None before accessing it
                    "Last outcome category": crime["outcome_status"]["category"]
                    if crime.get("outcome_status")
                    else "",
                    "Context": crime.get("context", ""),
                }
                compiled_crime_data.append(crime_entry)

    # Convert the list to a DataFrame
    compiled_crime_df = pd.DataFrame(compiled_crime_data)

    # Save the compiled data to a CSV file
    file_name = f"compiled_crime_data_{month}.csv"
    compiled_crime_df.to_csv(file_name, index=False)
    print(
        f"Saved data for {month} to {file_name} with {compiled_crime_df.shape[0]} rows."
    )
