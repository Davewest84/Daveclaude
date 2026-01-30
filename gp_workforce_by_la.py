"""
GP Workforce by Local Authority

Calculates the number of GPs working in each local authority area by:
1. Downloading practice-level GP workforce data from NHS Digital
2. Mapping GP practices to local authorities using ONS postcode lookups
3. Matching with ONS mid-year population estimates

Data sources:
- NHS Digital General Practice Workforce (practice-level CSV)
- ONS National Statistics Postcode Lookup (NSPL)
- ONS Mid-Year Population Estimates by local authority
"""

import io
import os
import re
import sys
import zipfile
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DATA_DIR = Path("data")

# NHS Digital GP workforce publication page (latest available snapshot).
# Update this URL to point at the month you want to analyse.
GP_WORKFORCE_URL = (
    "https://digital.nhs.uk/data-and-information/publications/statistical/"
    "general-and-personal-medical-services/31-december-2025"
)

# ODS GP practice data – provides practice postcodes.
# This is the ODS Data Search and Export API endpoint for epraccur.
EPRACCUR_URL = (
    "https://www.odsdatasearchandexport.nhs.uk/api/getReport?report=epraccur"
)

# ONS mid-year population estimates – England & Wales by local authority.
# This links to the SAPE dataset (latest available: mid-2024, published 2025).
POPULATION_URL = (
    "https://www.ons.gov.uk/file?uri=/peoplepopulationandcommunity/"
    "populationandmigration/populationestimates/datasets/"
    "populationestimatesforukenglandandwalesscotlandandnorthernireland/"
    "mid2024/ukpopulationestimates18382024.xlsx"
)

# ONS National Statistics Postcode Lookup (NSPL) – postcode to LA mapping.
# This is the ArcGIS direct-download URL for the May 2025 NSPL ZIP (~178 MB).
NSPL_URL = (
    "https://www.arcgis.com/sharing/rest/content/items/"
    "077631e063eb4e1ab43575d01381ec33/data"
)

# Column names in the epraccur data (fixed-width / CSV without headers).
# See: https://digital.nhs.uk/services/organisation-data-service/
#      data-search-and-export/csv-downloads/gp-and-gp-practice-related-data
EPRACCUR_COLUMNS = [
    "Organisation Code",
    "Name",
    "National Grouping",
    "High Level Health Geography",
    "Address Line 1",
    "Address Line 2",
    "Address Line 3",
    "Address Line 4",
    "Address Line 5",
    "Postcode",
    "Open Date",
    "Close Date",
    "Status Code",
    "Organisation Sub-Type Code",
    "Commissioner",
    "Join Provider/Purchaser Date",
    "Left Provider/Purchaser Date",
    "Contact Telephone Number",
    "Null 1",
    "Null 2",
    "Null 3",
    "Amended Record Indicator",
    "Null 4",
    "Provider/Purchaser",
    "Null 5",
    "Prescribing Setting",
    "Null 6",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def ensure_data_dir() -> Path:
    """Create the data directory if it doesn't exist."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    return DATA_DIR


def download_file(url: str, dest: Path, description: str = "") -> Path:
    """Download a file if it doesn't already exist locally."""
    if dest.exists():
        print(f"  [cached] {description or dest.name}")
        return dest
    print(f"  Downloading {description or url} ...")
    resp = requests.get(url, timeout=120, stream=True)
    resp.raise_for_status()
    dest.write_bytes(resp.content)
    print(f"  Saved to {dest}")
    return dest


# ---------------------------------------------------------------------------
# Step 1 – GP Workforce practice-level data
# ---------------------------------------------------------------------------


def find_practice_csv_url(publication_url: str) -> str:
    """Scrape the NHS Digital publication page to find the practice-level
    CSV (ZIP) download link."""
    print(f"  Fetching publication page: {publication_url}")
    resp = requests.get(publication_url, timeout=60)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    # Look for links whose text or href contains "Practice" and ends .zip/.csv
    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"]
        text = a_tag.get_text(strip=True).lower()
        if "practice" in text and href.endswith(".zip"):
            if not href.startswith("http"):
                href = "https://digital.nhs.uk" + href
            return href

    # Fallback: look for any files.digital.nhs.uk link with "practice" in URL
    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"].lower()
        if "files.digital.nhs.uk" in href and "practice" in href:
            return a_tag["href"]

    raise RuntimeError(
        "Could not find the practice-level CSV download link on the "
        f"publication page: {publication_url}\n"
        "Please download the practice-level ZIP manually from that page "
        "and place it in the data/ directory as 'gp_workforce_practice.zip'."
    )


def load_gp_workforce(publication_url: str | None = None) -> pd.DataFrame:
    """Download and load the GP workforce practice-level data."""
    ensure_data_dir()
    zip_path = DATA_DIR / "gp_workforce_practice.zip"

    if not zip_path.exists():
        if publication_url is None:
            publication_url = GP_WORKFORCE_URL
        csv_url = find_practice_csv_url(publication_url)
        download_file(csv_url, zip_path, "GP workforce practice-level ZIP")

    # Extract the CSV from the ZIP
    print("  Extracting GP workforce data ...")
    with zipfile.ZipFile(zip_path) as zf:
        csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if not csv_names:
            raise RuntimeError(f"No CSV found inside {zip_path}")
        # Pick the first CSV (there is usually just one)
        csv_name = csv_names[0]
        print(f"  Reading {csv_name} ...")
        with zf.open(csv_name) as f:
            df = pd.read_csv(f, encoding="utf-8-sig", low_memory=False)

    # Normalise column names to uppercase with underscores
    df.columns = [c.strip().upper().replace(" ", "_") for c in df.columns]
    print(f"  Loaded {len(df):,} practice records")
    print(f"  Columns: {list(df.columns[:10])} ...")
    return df


# ---------------------------------------------------------------------------
# Step 2 – Practice postcodes (epraccur)
# ---------------------------------------------------------------------------


def load_practice_postcodes() -> pd.DataFrame:
    """Download and parse the epraccur dataset to get practice postcodes."""
    ensure_data_dir()
    download_path = DATA_DIR / "epraccur.zip"
    download_file(EPRACCUR_URL, download_path, "epraccur (practice postcodes)")

    print("  Reading epraccur data ...")
    # The file may be a ZIP or a raw CSV depending on the endpoint
    try:
        with zipfile.ZipFile(download_path) as zf:
            csv_names = zf.namelist()
            csv_name = [n for n in csv_names if "epraccur" in n.lower()][0]
            with zf.open(csv_name) as f:
                df = pd.read_csv(
                    f,
                    header=None,
                    names=EPRACCUR_COLUMNS,
                    encoding="utf-8-sig",
                    low_memory=False,
                )
    except zipfile.BadZipFile:
        # Not a ZIP — treat as raw CSV
        df = pd.read_csv(
            download_path,
            header=None,
            names=EPRACCUR_COLUMNS,
            encoding="utf-8-sig",
            low_memory=False,
        )

    # Keep only open practices (Status Code 'A' = Active)
    df = df[df["Status Code"].isin(["A", "a"])].copy()

    # Keep GP practices only (Prescribing Setting = 4)
    df = df[df["Prescribing Setting"].astype(str).str.strip() == "4"].copy()

    # Clean postcodes – remove spaces for consistent matching
    df["Postcode_Clean"] = df["Postcode"].str.strip().str.upper().str.replace(r"\s+", "", regex=True)

    print(f"  Loaded {len(df):,} active GP practices with postcodes")
    return df[["Organisation Code", "Name", "Postcode", "Postcode_Clean"]]


# ---------------------------------------------------------------------------
# Step 3 – Postcode to Local Authority mapping (NSPL)
# ---------------------------------------------------------------------------


def load_nspl() -> pd.DataFrame:
    """Load the ONS NSPL postcode-to-LA lookup.

    Downloads the NSPL ZIP (~178 MB) automatically from the ONS Open
    Geography Portal if not already cached locally.

    Expected location: data/NSPL*.csv  (the main data CSV inside the ZIP)
    """
    ensure_data_dir()

    # Check for pre-extracted CSV
    nspl_csvs = list(DATA_DIR.glob("NSPL*.csv"))
    if not nspl_csvs:
        # Also check inside subdirectories (ZIP extraction may nest)
        nspl_csvs = list(DATA_DIR.rglob("NSPL*.csv"))

    if not nspl_csvs:
        # Check for the ZIP file
        nspl_zips = list(DATA_DIR.glob("NSPL*.zip"))
        if not nspl_zips:
            # Download automatically from ONS Open Geography Portal
            zip_path = DATA_DIR / "NSPL.zip"
            print("  NSPL not found locally — downloading from ONS (~178 MB) ...")
            download_file(NSPL_URL, zip_path, "NSPL postcode lookup ZIP")
            nspl_zips = [zip_path]

        # Extract the main data CSV from the ZIP
        zip_path = nspl_zips[0]
        print(f"  Extracting NSPL from {zip_path.name} ...")
        with zipfile.ZipFile(zip_path) as zf:
            # The main CSV is usually in Data/NSPL_*.csv
            data_csvs = [
                n for n in zf.namelist()
                if re.search(r"Data/NSPL.*\.csv$", n, re.IGNORECASE)
            ]
            if not data_csvs:
                # Fallback: any CSV containing 'NSPL' in name
                data_csvs = [
                    n for n in zf.namelist()
                    if "nspl" in n.lower() and n.lower().endswith(".csv")
                ]
            if not data_csvs:
                raise RuntimeError(
                    f"Could not find NSPL data CSV inside {zip_path}. "
                    "Please extract the main data CSV into data/ manually."
                )
            csv_name = data_csvs[0]
            print(f"  Extracting {csv_name} ...")
            zf.extract(csv_name, DATA_DIR)
            nspl_csvs = [DATA_DIR / csv_name]

    nspl_path = nspl_csvs[0]
    print(f"  Reading NSPL from {nspl_path} ...")

    # Only read the columns we need to save memory
    # pcd = postcode, laua = local authority district code
    # pcds = postcode (7-char), pcd2 = postcode (8-char)
    usecols = ["pcds", "laua"]
    try:
        df = pd.read_csv(nspl_path, usecols=usecols, low_memory=False)
    except ValueError:
        # Column names may vary; try alternatives
        df = pd.read_csv(nspl_path, low_memory=False)
        print(f"  NSPL columns found: {list(df.columns[:15])} ...")
        # Try to find the right columns
        pcd_col = next(
            (c for c in df.columns if c.lower() in ("pcds", "pcd", "pcd2")),
            df.columns[0],
        )
        la_col = next(
            (c for c in df.columns if c.lower() in ("laua", "oslaua", "ladcd")),
            None,
        )
        if la_col is None:
            raise RuntimeError(
                "Could not identify the local authority column in NSPL. "
                f"Available columns: {list(df.columns)}"
            )
        df = df[[pcd_col, la_col]].rename(columns={pcd_col: "pcds", la_col: "laua"})

    # Clean postcodes for matching – remove all spaces
    df["Postcode_Clean"] = df["pcds"].str.strip().str.upper().str.replace(r"\s+", "", regex=True)
    df = df[["Postcode_Clean", "laua"]].drop_duplicates(subset="Postcode_Clean")

    print(f"  Loaded {len(df):,} postcode-to-LA mappings")
    return df


# ---------------------------------------------------------------------------
# Step 4 – LA names lookup
# ---------------------------------------------------------------------------


def load_la_names() -> pd.DataFrame:
    """Load local authority names from the NSPL Documents folder or fallback.

    The NSPL ZIP typically contains a Documents/LA_UA names and codes*.csv
    with columns like LAD25CD and LAD25NM.
    """
    ensure_data_dir()

    # Search for LA name lookup files in the data directory
    la_files = list(DATA_DIR.rglob("*LA_UA*names*codes*.csv"))
    if not la_files:
        la_files = list(DATA_DIR.rglob("*LAD*names*codes*.csv"))
    if not la_files:
        la_files = list(DATA_DIR.rglob("*local*authority*.csv"))

    if la_files:
        la_path = la_files[0]
        print(f"  Reading LA names from {la_path} ...")
        df = pd.read_csv(la_path, encoding="utf-8-sig")
        # Find the code and name columns
        code_col = next(
            (c for c in df.columns if re.match(r"LAD\d+CD", c)), df.columns[0]
        )
        name_col = next(
            (c for c in df.columns if re.match(r"LAD\d+NM", c)), df.columns[1]
        )
        return df[[code_col, name_col]].rename(
            columns={code_col: "LA_Code", name_col: "LA_Name"}
        )

    print("  LA names file not found; will use codes only.")
    return pd.DataFrame(columns=["LA_Code", "LA_Name"])


# ---------------------------------------------------------------------------
# Step 5 – Population data
# ---------------------------------------------------------------------------


def load_population() -> pd.DataFrame:
    """Download and load ONS mid-year population estimates by LA."""
    ensure_data_dir()
    xlsx_path = DATA_DIR / "population_estimates.xlsx"

    # Try downloading; this URL may change so allow for manual placement
    if not xlsx_path.exists():
        try:
            download_file(POPULATION_URL, xlsx_path, "ONS population estimates")
        except Exception as exc:
            print(
                f"\n  *** Could not download population data: {exc} ***\n"
                "  Please download the mid-year population estimates XLSX from:\n"
                "  https://www.ons.gov.uk/peoplepopulationandcommunity/"
                "populationandmigration/populationestimates/datasets/"
                "populationestimatesforukenglandandwalesscotlandandnorthernireland\n\n"
                "  Save it as data/population_estimates.xlsx\n"
            )
            return pd.DataFrame(columns=["LA_Code", "Population"])

    print("  Reading population estimates ...")

    # The ONS workbook has multiple sheets; the MYE data for LAs is typically
    # in a sheet named 'MYE2 - Persons' or similar, starting a few rows down.
    try:
        xls = pd.ExcelFile(xlsx_path)
        # Find the right sheet
        sheet_name = None
        for name in xls.sheet_names:
            if "MYE" in name.upper() and "PERSON" in name.upper():
                sheet_name = name
                break
        if sheet_name is None:
            # Try sheets containing 'population' or 'estimates'
            for name in xls.sheet_names:
                if any(kw in name.upper() for kw in ["POPULAT", "ESTIMAT", "MYE2"]):
                    sheet_name = name
                    break
        if sheet_name is None:
            sheet_name = xls.sheet_names[0]

        print(f"  Using sheet: {sheet_name}")
        df = pd.read_excel(xlsx_path, sheet_name=sheet_name, header=None)

        # Find the header row (contains 'Code' or 'Area code')
        header_row = None
        for i in range(min(15, len(df))):
            row_values = df.iloc[i].astype(str).str.upper()
            if any("CODE" in v for v in row_values):
                header_row = i
                break

        if header_row is not None:
            df.columns = df.iloc[header_row].astype(str).str.strip()
            df = df.iloc[header_row + 1:].reset_index(drop=True)

        # Identify columns
        code_col = next(
            (c for c in df.columns if "CODE" in str(c).upper() and "NAME" not in str(c).upper()),
            df.columns[0],
        )
        name_col = next(
            (c for c in df.columns if "NAME" in str(c).upper()),
            None,
        )

        # The 'All Ages' or total population column
        total_col = next(
            (c for c in df.columns if "ALL" in str(c).upper() and "AGE" in str(c).upper()),
            None,
        )
        if total_col is None:
            # Look for a column labelled 'All ages' or just pick the first numeric
            for c in df.columns:
                if str(c).strip().lower() in ("all ages", "all_ages", "total"):
                    total_col = c
                    break

        if total_col is None:
            print("  Warning: Could not identify total population column.")
            print(f"  Available columns: {list(df.columns[:20])}")
            return pd.DataFrame(columns=["LA_Code", "Population"])

        result = df[[code_col, total_col]].copy()
        result.columns = ["LA_Code", "Population"]
        result["LA_Code"] = result["LA_Code"].astype(str).str.strip()
        result["Population"] = pd.to_numeric(result["Population"], errors="coerce")
        result = result.dropna(subset=["Population"])

        # Keep only LA-level codes (E06, E07, E08, E09, W06, S12, N09)
        la_pattern = r"^(E0[6-9]|W06|S12|N09)"
        result = result[result["LA_Code"].str.match(la_pattern, na=False)]

        print(f"  Loaded population data for {len(result):,} local authorities")
        return result

    except Exception as exc:
        print(f"  Error reading population file: {exc}")
        return pd.DataFrame(columns=["LA_Code", "Population"])


# ---------------------------------------------------------------------------
# Step 6 – Identify the GP headcount / FTE columns
# ---------------------------------------------------------------------------


def identify_gp_columns(df: pd.DataFrame) -> dict:
    """Identify the relevant GP FTE and headcount columns in the workforce data."""
    cols = list(df.columns)

    result = {}

    # Practice code column
    for candidate in ["PRAC_CODE", "PRACTICE_CODE", "ORG_CODE", "ORGANISATION_CODE"]:
        if candidate in cols:
            result["practice_code"] = candidate
            break
    if "practice_code" not in result:
        # Fuzzy match
        for c in cols:
            if "PRAC" in c and "CODE" in c:
                result["practice_code"] = c
                break

    # Postcode column (some practice-level files include this)
    for candidate in ["POSTCODE", "POST_CODE", "PRAC_POSTCODE"]:
        if candidate in cols:
            result["postcode"] = candidate
            break

    # Total GPs FTE
    for candidate in ["TOTAL_GP_FTE", "TOTAL_GPs_FTE", "ALL_GP_FTE", "TOTAL_FTE_GPS"]:
        if candidate in cols:
            result["gp_fte"] = candidate
            break
    if "gp_fte" not in result:
        for c in cols:
            if "GP" in c and "FTE" in c and "TOTAL" in c:
                result["gp_fte"] = c
                break
        if "gp_fte" not in result:
            for c in cols:
                if "GP" in c and "FTE" in c:
                    result["gp_fte"] = c
                    break

    # Total GPs headcount
    for candidate in ["TOTAL_GP_HC", "TOTAL_GPs_HC", "ALL_GP_HC", "TOTAL_HC_GPS"]:
        if candidate in cols:
            result["gp_hc"] = candidate
            break
    if "gp_hc" not in result:
        for c in cols:
            if "GP" in c and "HC" in c and "TOTAL" in c:
                result["gp_hc"] = c
                break
        if "gp_hc" not in result:
            for c in cols:
                if "GP" in c and ("HC" in c or "HEADCOUNT" in c):
                    result["gp_hc"] = c
                    break

    return result


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------


def main():
    print("=" * 70)
    print("GP Workforce by Local Authority")
    print("=" * 70)

    # --- 1. Load GP workforce data ---
    print("\n[1/5] Loading GP workforce practice-level data ...")
    workforce = load_gp_workforce()
    col_map = identify_gp_columns(workforce)
    print(f"  Identified columns: {col_map}")

    if "practice_code" not in col_map:
        print("\n  ERROR: Could not find practice code column.")
        print(f"  Available columns: {list(workforce.columns)}")
        sys.exit(1)

    # --- 2. Load practice postcodes ---
    print("\n[2/5] Loading practice postcodes ...")

    # Check if the workforce data already has postcodes
    if "postcode" in col_map:
        print("  Using postcodes from workforce data directly.")
        practices = workforce[[col_map["practice_code"], col_map["postcode"]]].copy()
        practices.columns = ["Practice_Code", "Postcode"]
    else:
        # Get postcodes from epraccur
        epraccur = load_practice_postcodes()
        practices = epraccur.rename(
            columns={"Organisation Code": "Practice_Code"}
        )[["Practice_Code", "Postcode", "Postcode_Clean"]]

    practices["Practice_Code"] = practices["Practice_Code"].astype(str).str.strip()
    if "Postcode_Clean" not in practices.columns:
        practices["Postcode_Clean"] = (
            practices["Postcode"].str.strip().str.upper().str.replace(r"\s+", "", regex=True)
        )

    # --- 3. Load NSPL postcode-to-LA mapping ---
    print("\n[3/5] Loading postcode-to-LA lookup (NSPL) ...")
    nspl = load_nspl()

    # --- 4. Map practices to LAs ---
    print("\n[4/5] Mapping practices to local authorities ...")

    # Merge postcodes onto workforce data
    prac_code_col = col_map["practice_code"]
    workforce["Practice_Code"] = workforce[prac_code_col].astype(str).str.strip()

    # Join workforce -> practice postcodes
    if "postcode" in col_map:
        workforce["Postcode_Clean"] = (
            workforce[col_map["postcode"]]
            .str.strip()
            .str.upper()
            .str.replace(r"\s+", "", regex=True)
        )
    else:
        workforce = workforce.merge(
            practices[["Practice_Code", "Postcode_Clean"]],
            on="Practice_Code",
            how="left",
        )

    # Join workforce -> LA via postcode
    workforce = workforce.merge(nspl, on="Postcode_Clean", how="left")

    unmatched = workforce["laua"].isna().sum()
    total = len(workforce)
    print(f"  Matched {total - unmatched:,} / {total:,} practices to a local authority")
    if unmatched > 0:
        print(f"  ({unmatched:,} practices could not be matched)")

    # --- 5. Aggregate to LA level ---
    gp_fte_col = col_map.get("gp_fte")
    gp_hc_col = col_map.get("gp_hc")

    agg_dict = {}
    agg_labels = {}
    if gp_fte_col:
        workforce[gp_fte_col] = pd.to_numeric(workforce[gp_fte_col], errors="coerce")
        agg_dict[gp_fte_col] = "sum"
        agg_labels[gp_fte_col] = "GP_FTE"
    if gp_hc_col:
        workforce[gp_hc_col] = pd.to_numeric(workforce[gp_hc_col], errors="coerce")
        agg_dict[gp_hc_col] = "sum"
        agg_labels[gp_hc_col] = "GP_Headcount"

    # Also count practices per LA
    agg_dict["Practice_Code"] = "nunique"
    agg_labels["Practice_Code"] = "Num_Practices"

    la_summary = (
        workforce[workforce["laua"].notna()]
        .groupby("laua")
        .agg(agg_dict)
        .rename(columns=agg_labels)
        .reset_index()
        .rename(columns={"laua": "LA_Code"})
    )

    # --- 6. Add LA names ---
    la_names = load_la_names()
    if not la_names.empty:
        la_summary = la_summary.merge(la_names, on="LA_Code", how="left")
    else:
        la_summary["LA_Name"] = ""

    # --- 7. Add population data ---
    print("\n[5/5] Adding population data ...")
    population = load_population()
    if not population.empty:
        la_summary = la_summary.merge(population, on="LA_Code", how="left")

        # Calculate GPs per 100,000 population
        if "GP_FTE" in la_summary.columns:
            la_summary["GP_FTE_per_100k"] = (
                la_summary["GP_FTE"] / la_summary["Population"] * 100_000
            ).round(1)
        if "GP_Headcount" in la_summary.columns:
            la_summary["GP_HC_per_100k"] = (
                la_summary["GP_Headcount"] / la_summary["Population"] * 100_000
            ).round(1)

    # --- 8. Sort and output ---
    sort_col = "GP_FTE" if "GP_FTE" in la_summary.columns else "GP_Headcount"
    if sort_col in la_summary.columns:
        la_summary = la_summary.sort_values(sort_col, ascending=False)

    # Reorder columns for readability
    col_order = ["LA_Code", "LA_Name"]
    for c in ["Num_Practices", "GP_Headcount", "GP_FTE", "Population",
              "GP_HC_per_100k", "GP_FTE_per_100k"]:
        if c in la_summary.columns:
            col_order.append(c)
    la_summary = la_summary[[c for c in col_order if c in la_summary.columns]]

    # Save output
    output_path = DATA_DIR / "gp_workforce_by_local_authority.csv"
    la_summary.to_csv(output_path, index=False)
    print(f"\n{'=' * 70}")
    print(f"Output saved to: {output_path}")
    print(f"Total local authorities: {len(la_summary):,}")
    print(f"{'=' * 70}")

    # Print summary statistics
    print("\nSummary statistics:")
    numeric_cols = la_summary.select_dtypes(include="number").columns
    print(la_summary[numeric_cols].describe().round(1).to_string())

    # Print top 10 and bottom 10
    if "GP_FTE" in la_summary.columns:
        print("\nTop 10 LAs by GP FTE:")
        print(
            la_summary.head(10)[
                [c for c in ["LA_Name", "LA_Code", "GP_FTE", "GP_FTE_per_100k"]
                 if c in la_summary.columns]
            ].to_string(index=False)
        )
        print("\nBottom 10 LAs by GP FTE:")
        print(
            la_summary.tail(10)[
                [c for c in ["LA_Name", "LA_Code", "GP_FTE", "GP_FTE_per_100k"]
                 if c in la_summary.columns]
            ].to_string(index=False)
        )

    return la_summary


if __name__ == "__main__":
    main()
