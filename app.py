import argparse
import csv
import json
import time
import urllib.error
import urllib.request
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

API_HOST = "https://explorer.natureserve.org"
API_PATH = "/api/data/taxon/"


def format_last_modified(last_modified: Optional[str]) -> Optional[str]:
    """Convert lastModified ISO date to human-friendly date."""
    if not last_modified:
        return None

    try:
        dt = datetime.fromisoformat(last_modified.replace("Z", "+00:00"))
        return dt.strftime("%B %d, %Y, %I:%M:%S %p %Z")
    except Exception:
        return last_modified


def extract_habitat_descriptions(
    habitat_list: Optional[list], habitat_key: str, desc_key: str
) -> Optional[str]:
    """
    Extract habitat descriptions (english language).

    Args:
        habitat_list: List of habitat objects
        habitat_key: The key name for the nested habitat object (e.g., 'marineHabitat', 'terrestrialHabitat')
        desc_key: The key name for the description (e.g., 'marineHabitatDescEn', 'terrestrialHabitatDescEn')

    Returns:
        Comma-separated habitat descriptions, or None if no descriptions found
    """
    if not habitat_list or not isinstance(habitat_list, list):
        return None

    descriptions = []
    for item in habitat_list:
        if isinstance(item, dict):
            habitat_obj = item.get(habitat_key)
            if isinstance(habitat_obj, dict):
                desc = habitat_obj.get(desc_key)
                if desc:
                    descriptions.append(str(desc))

    return ", ".join(descriptions) if descriptions else None


def extract_data(full_data: dict) -> dict:
    """
    Extract fields from the response.

    Keys:
    - elementGlobalId
    - uniqueId
    - speciesGlobal.elementGlobalId
    - primaryCommonName
    - scientificName
    - lastModified
    - grankReasons
    - speciesCharacteristics.habitatComments
    - descriptions only:
        - speciesCharacteristics.speciesMarineHabitats
        - speciesCharacteristics.speciesTerrestrialHabitats
        - speciesCharacteristics.speciesRiverineHabitats
        - speciesCharacteristics.speciesPalustrineHabitats
        - speciesCharacteristics.speciesLacustrineHabitats
        - speciesCharacteristics.speciesSubterraneanHabitats
        - speciesCharacteristics.speciesEstuarineHabitats
    """
    species_chars = full_data.get("speciesCharacteristics", {}) or {}
    species_global = full_data.get("speciesGlobal", {}) or {}
    rank_info = full_data.get("rankInfo", {}) or {}
    range_extent = rank_info.get("rangeExtent", {}) or {}

    last_modified_readable = format_last_modified(full_data.get("lastModified"))

    habitat_types = [
        (
            "speciesMarineHabitats",
            "marineHabitat",
            "marineHabitatDescEn",
            "marineHabitats",
        ),
        (
            "speciesTerrestrialHabitats",
            "terrestrialHabitat",
            "terrestrialHabitatDescEn",
            "terrestrialHabitats",
        ),
        (
            "speciesRiverineHabitats",
            "riverineHabitat",
            "riverineHabitatDescEn",
            "riverineHabitats",
        ),
        (
            "speciesPalustrineHabitats",
            "palustrineHabitat",
            "palustrineHabitatDescEn",
            "palustrineHabitats",
        ),
        (
            "speciesLacustrineHabitats",
            "lacustrineHabitat",
            "lacustrineHabitatDescEn",
            "lacustrineHabitats",
        ),
        (
            "speciesSubterraneanHabitats",
            "subterraneanHabitat",
            "subterraneanHabitatDescEn",
            "subterraneanHabitats",
        ),
        (
            "speciesEstuarineHabitats",
            "estuarineHabitat",
            "estuarineHabitatDescEn",
            "estuarineHabitats",
        ),
    ]

    extracted = {
        "elementGlobalId": full_data.get("elementGlobalId"),
        "uniqueId": full_data.get("uniqueId"),
        "speciesGlobalElementGlobalId": species_global.get("elementGlobalId"),
        "primaryCommonName": full_data.get("primaryCommonName"),
        "scientificName": full_data.get("scientificName"),
        "lastModified": last_modified_readable,
        "grankReasons": full_data.get("grankReasons"),
        "habitatComments": species_chars.get("habitatComments"),
        "rangeExtent": range_extent.get("rangeExtentDescEn"),
    }

    for api_key, habitat_key, desc_key, output_key in habitat_types:
        habitat_data = species_chars.get(api_key)
        descriptions = extract_habitat_descriptions(habitat_data, habitat_key, desc_key)
        extracted[output_key] = descriptions

    return extracted


def format_csv_value(value: Any) -> str:
    """Format for CSV output."""
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return json.dumps(value)
    return str(value)


def fetch_taxon_data(route: str, i: int) -> dict:
    """Fetch data for a single taxon route."""
    url = f"{API_HOST}{API_PATH}{route}"
    print(f"Fetching: {url}")

    try:
        with urllib.request.urlopen(url, timeout=10) as response:
            if response.status != 200:
                error_msg = f"{response.status} {response.reason}"
                print(f"Failed to fetch {url}: {error_msg}")
                return {"url": url, "error": error_msg, "data": None}

            full_data = json.loads(response.read().decode("utf-8"))
            extracted_data = extract_data(full_data)

            print(f"✓ Successfully fetched ({i + 1}/{len(CURRENT_SET)}) {url}")
            return {"url": url, "data": extracted_data}

    except urllib.error.HTTPError as error:
        error_msg = f"{error.code} {error.reason}"
        print(f"Failed to fetch {url}: {error_msg}")
        return {"url": url, "error": error_msg, "data": None}
    except Exception as error:
        error_msg = str(error)
        print(f"Error fetching {url}: {error_msg}")
        return {"url": url, "error": error_msg, "data": None}


def fetch_data(datafile_name: str):
    """Fetch all taxon data and save to CSV.

    Args:
        datafile_name: Name of the datafile (also used to generate output filename)
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    datafile_stem = Path(datafile_name).stem
    output_filename = f"{datafile_stem}_{timestamp}.csv"
    output_path = Path("results") / output_filename

    output_path.parent.mkdir(parents=True, exist_ok=True)

    headers = [
        "url",
        "elementGlobalId",
        "uniqueId",
        "speciesGlobalElementGlobalId",
        "primaryCommonName",
        "scientificName",
        "lastModified",
        "grankReasons",
        "habitatComments",
        "rangeExtent",
        "marineHabitats",
        "terrestrialHabitats",
        "riverineHabitats",
        "palustrineHabitats",
        "lacustrineHabitats",
        "subterraneanHabitats",
        "estuarineHabitats",
    ]

    rows_written = 0
    total_routes = len(CURRENT_SET)

    try:
        with open(output_path, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(headers)

            for i, route in enumerate(CURRENT_SET):
                result = fetch_taxon_data(route, i)

                if result.get("error"):
                    row = [result["url"], result["error"]] + [""] * (len(headers) - 2)
                    writer.writerow(row)
                else:
                    data = result["data"]
                    row = [
                        result["url"],
                        format_csv_value(data.get("elementGlobalId")),
                        format_csv_value(data.get("uniqueId")),
                        format_csv_value(data.get("speciesGlobalElementGlobalId")),
                        format_csv_value(data.get("primaryCommonName")),
                        format_csv_value(data.get("scientificName")),
                        format_csv_value(data.get("lastModified")),
                        format_csv_value(data.get("grankReasons")),
                        format_csv_value(data.get("habitatComments")),
                        format_csv_value(data.get("rangeExtent")),
                        format_csv_value(data.get("marineHabitats")),
                        format_csv_value(data.get("terrestrialHabitats")),
                        format_csv_value(data.get("riverineHabitats")),
                        format_csv_value(data.get("palustrineHabitats")),
                        format_csv_value(data.get("lacustrineHabitats")),
                        format_csv_value(data.get("subterraneanHabitats")),
                        format_csv_value(data.get("estuarineHabitats")),
                    ]
                    writer.writerow(row)

                rows_written += 1
                csvfile.flush()

                if i < total_routes - 1:
                    print("Rate limiting...")
                    time.sleep(0.5)

        print(
            f"\n✓ Complete: Successfully wrote all {rows_written}/{total_routes} rows to {output_path}"
        )

    except Exception as e:
        print(
            f"\n✗ Incomplete: Only {rows_written}/{total_routes} rows written to {output_path}"
        )
        print(f"Error: {e}")
        print(f"Partial results have been saved and can be found at: {output_path}")
        raise


def load_species_from_csv(file_path: str) -> list:
    """
    Load species IDs from a CSV file with a single column.

    Args:
        file_path: Path to the CSV file containing species IDs (one per row, no header)

    Returns:
        List of species IDs

    Raises:
        FileNotFoundError: If the file doesn't exist
    """
    path = Path(file_path)

    if not path.exists():
        raise FileNotFoundError(f"Data file not found: {file_path}")

    species_ids = []
    with open(path, "r", encoding="utf-8") as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            if row and row[0].strip():
                species_ids.append(row[0].strip())

    return species_ids


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Fetch taxon data from NatureServe API"
    )
    parser.add_argument(
        "--datafile",
        type=str,
        required=True,
        help="Path to CSV file containing species IDs (e.g., animals_set_1.csv)",
    )
    args = parser.parse_args()

    try:
        CURRENT_SET = load_species_from_csv(args.datafile)
        print(f"Loaded {len(CURRENT_SET)} routes from {args.datafile}\n")
        fetch_data(args.datafile)
    except FileNotFoundError as e:
        print(f"Error: {e}")
        exit(1)
