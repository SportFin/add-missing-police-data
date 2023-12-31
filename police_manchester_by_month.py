import pandas as pd
import requests
import json
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed


def fetch_crime_data(row):
    api_url = f"https://data.police.uk/api/crimes-at-location?date=2023-05&lat={row['latitude']}&lng={row['longitude']}"
    try:
        response = requests.get(api_url, timeout=10)
        if response.status_code == 200:
            crimes = json.loads(response.text)
            if not crimes:
                print(
                    f"No crime data for location: Latitude {row['latitude']}, Longitude {row['longitude']}"
                )
            else:
                print(
                    f"Crime data found for location: Latitude {row['latitude']}, Longitude {row['longitude']}"
                )
            return crimes
        else:
            print(
                f"API request failed with status code {response.status_code} for Latitude {row['latitude']}, Longitude {row['longitude']}"
            )
    except requests.RequestException as e:
        print(
            f"Request exception for Latitude {row['latitude']}, Longitude {row['longitude']}: {e}"
        )
    return []


# Load the Manchester data
manchester_data = pd.read_csv("manchester_missing_long_lat.csv")

# Use ThreadPoolExecutor to make requests in parallel
compiled_crime_data = []
with ThreadPoolExecutor(max_workers=10) as executor:
    future_to_row = {
        executor.submit(fetch_crime_data, row): row
        for index, row in manchester_data.iterrows()
    }
    for future in tqdm(as_completed(future_to_row), total=len(future_to_row)):
        crimes = future.result()
        for crime in crimes:
            crime_entry = {
                "Crime ID": crime.get("id", ""),
                "Month": crime.get("month", "2023-07"),
                "Reported by": "Not specified",
                "Falls within": "Not specified",
                "Longitude": crime["location"]["longitude"],
                "Latitude": crime["location"]["latitude"],
                "Location": crime["location"]["street"]["name"],
                "LSOA code": "",
                "LSOA name": "",
                "Crime type": crime["category"],
                "Last outcome category": crime.get("outcome_status", {}).get(
                    "category", ""
                ),
                "Context": crime.get("context", ""),
            }
            compiled_crime_data.append(crime_entry)

# Convert the list to a DataFrame
compiled_crime_df = pd.DataFrame(compiled_crime_data)

# Save the compiled data to a CSV file
compiled_crime_df.to_csv("compiled_crime_data.csv", index=False)

# Print summary
print(f"Total rows in the output file: {compiled_crime_df.shape[0]}")
