"""
GP Workforce by Local Authority

Calculates the number of GPs working in each local authority area by:
1. Downloading practice-level GP workforce data from NHS Digital
2. Mapping GP practices to local authorities using GP providers.xlsx
3. Matching with ONS mid-year population estimates

Data sources:
- NHS Digital General Practice Workforce (practice-level CSV)
- CQC GP Providers register (GP providers.xlsx) for practice-to-LA mapping
- ONS Mid-Year Population Estimates by local authority
"""

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

# ONS mid-year population estimates – England & Wales by local authority.
# Multiple URLs to try, as ONS filenames change between releases.
POPULATION_URLS = [
    # England & Wales dataset (mid-2024)
    "https://www.ons.gov.uk/file?uri=/peoplepopulationandcommunity/"
    "populationandmigration/populationestimates/datasets/"
    "estimatesofthepopulationforenglandandwales/"
    "mid2024/estimatesofthepopulationforenglandandwalesmid2024.xlsx",
    # UK-wide dataset (mid-2024)
    "https://www.ons.gov.uk/file?uri=/peoplepopulationandcommunity/"
    "populationandmigration/populationestimates/datasets/"
    "populationestimatesforukenglandandwalesscotlandandnorthernireland/"
    "mid2024/ukpopulationestimates18382024.xlsx",
]

# GP providers register – maps practice ODS codes to local authorities.
# This file should be in the repo root (GP providers.xlsx).
GP_PROVIDERS_PATH = Path("GP providers.xlsx")


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

    # Extract the CSV from the ZIP (or read directly if not zipped)
    print("  Reading GP workforce data ...")
    try:
        with zipfile.ZipFile(zip_path) as zf:
            csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
            if not csv_names:
                raise RuntimeError(f"No CSV found inside {zip_path}")
            csv_name = csv_names[0]
            print(f"  Reading {csv_name} ...")
            with zf.open(csv_name) as f:
                df = pd.read_csv(f, encoding="utf-8-sig", low_memory=False)
    except zipfile.BadZipFile:
        # Not a ZIP — treat as raw CSV
        print("  File is not a ZIP — reading as CSV directly ...")
        df = pd.read_csv(zip_path, encoding="utf-8-sig", low_memory=False)

    # Normalise column names to uppercase with underscores
    df.columns = [c.strip().upper().replace(" ", "_") for c in df.columns]
    print(f"  Loaded {len(df):,} practice records")
    print(f"  Columns: {list(df.columns[:10])} ...")
    return df


# ---------------------------------------------------------------------------
# Step 2 – Practice-to-LA mapping from GP providers.xlsx
# ---------------------------------------------------------------------------


def load_gp_providers() -> pd.DataFrame:
    """Load the GP providers register to map ODS codes to local authorities."""
    if not GP_PROVIDERS_PATH.exists():
        print(f"\n  ERROR: {GP_PROVIDERS_PATH} not found.")
        print("  Please place the CQC GP providers XLSX in the repo root.")
        sys.exit(1)

    print(f"  Reading {GP_PROVIDERS_PATH} ...")
    df = pd.read_excel(GP_PROVIDERS_PATH)

    # Keep only rows with an ODS code
    df = df[df["Location ODS Code"].notna()].copy()

    # Keep only non-dormant locations
    df = df[df["Dormant (Y/N)"] != "Y"].copy()

    result = df[["Location ODS Code", "Location Local Authority"]].copy()
    result.columns = ["Practice_Code", "LA_Name"]
    result["Practice_Code"] = result["Practice_Code"].astype(str).str.strip()
    result["LA_Name"] = result["LA_Name"].astype(str).str.strip()

    # De-duplicate (some practices may appear more than once)
    result = result.drop_duplicates(subset="Practice_Code")

    print(f"  Loaded {len(result):,} GP practices with LA mappings")
    return result


# ---------------------------------------------------------------------------
# Step 3 – Population data
# ---------------------------------------------------------------------------


def load_population() -> pd.DataFrame:
    """Download and load ONS mid-year population estimates by LA."""
    ensure_data_dir()
    xlsx_path = DATA_DIR / "population_estimates.xlsx"

    # Try downloading from multiple URLs; ONS filenames change between releases
    if not xlsx_path.exists():
        downloaded = False
        for url in POPULATION_URLS:
            try:
                download_file(url, xlsx_path, "ONS population estimates")
                downloaded = True
                break
            except Exception:
                print(f"  URL not found, trying next ...")
                continue
        if not downloaded:
            print(
                "\n  *** Could not download population data ***\n"
                "  Please download the mid-year population estimates XLSX from:\n"
                "  https://www.ons.gov.uk/peoplepopulationandcommunity/"
                "populationandmigration/populationestimates/datasets/"
                "estimatesofthepopulationforenglandandwales\n\n"
                "  Save it as data/population_estimates.xlsx\n"
            )
            return pd.DataFrame(columns=["LA_Name", "Population"])

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
            for c in df.columns:
                if str(c).strip().lower() in ("all ages", "all_ages", "total"):
                    total_col = c
                    break

        if total_col is None:
            print("  Warning: Could not identify total population column.")
            print(f"  Available columns: {list(df.columns[:20])}")
            return pd.DataFrame(columns=["LA_Name", "Population"])

        cols_to_keep = [total_col]
        if name_col:
            cols_to_keep = [name_col] + cols_to_keep
        if code_col:
            cols_to_keep = [code_col] + cols_to_keep

        result = df[cols_to_keep].copy()

        if name_col and code_col:
            result.columns = ["LA_Code", "LA_Name_ONS", "Population"]
        elif code_col:
            result.columns = ["LA_Code", "Population"]
        else:
            result.columns = ["Population"]

        if "LA_Code" in result.columns:
            result["LA_Code"] = result["LA_Code"].astype(str).str.strip()
        if "LA_Name_ONS" in result.columns:
            result["LA_Name_ONS"] = result["LA_Name_ONS"].astype(str).str.strip()

        result["Population"] = pd.to_numeric(result["Population"], errors="coerce")
        result = result.dropna(subset=["Population"])

        # Keep only LA-level codes (E06, E07, E08, E09, W06, S12, N09)
        if "LA_Code" in result.columns:
            la_pattern = r"^(E0[6-9]|W06|S12|N09)"
            result = result[result["LA_Code"].str.match(la_pattern, na=False)]

        print(f"  Loaded population data for {len(result):,} local authorities")
        return result

    except Exception as exc:
        print(f"  Error reading population file: {exc}")
        return pd.DataFrame(columns=["LA_Name", "Population"])


# ---------------------------------------------------------------------------
# Step 4 – Identify the GP headcount / FTE columns
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
        for c in cols:
            if "PRAC" in c and "CODE" in c:
                result["practice_code"] = c
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
    print("\n[1/4] Loading GP workforce practice-level data ...")
    workforce = load_gp_workforce()
    col_map = identify_gp_columns(workforce)
    print(f"  Identified columns: {col_map}")

    if "practice_code" not in col_map:
        print("\n  ERROR: Could not find practice code column.")
        print(f"  Available columns: {list(workforce.columns)}")
        sys.exit(1)

    # --- 2. Load practice-to-LA mapping ---
    print("\n[2/4] Loading practice-to-LA mapping (GP providers.xlsx) ...")
    providers = load_gp_providers()

    # --- 3. Map practices to LAs ---
    print("\n[3/4] Mapping practices to local authorities ...")

    prac_code_col = col_map["practice_code"]
    workforce["Practice_Code"] = workforce[prac_code_col].astype(str).str.strip()

    # Join workforce -> LA via ODS code
    workforce = workforce.merge(providers, on="Practice_Code", how="left")

    unmatched = workforce["LA_Name"].isna().sum()
    total = len(workforce)
    print(f"  Matched {total - unmatched:,} / {total:,} practices to a local authority")
    if unmatched > 0:
        print(f"  ({unmatched:,} practices could not be matched)")

    # --- 4. Aggregate to LA level ---
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
        workforce[workforce["LA_Name"].notna()]
        .groupby("LA_Name")
        .agg(agg_dict)
        .rename(columns=agg_labels)
        .reset_index()
    )

    # --- 5. Add population data ---
    print("\n[4/4] Adding population data ...")
    population = load_population()

    if not population.empty and "LA_Name_ONS" in population.columns:
        # Match on LA name (GP providers uses names, ONS uses names + codes)
        la_summary = la_summary.merge(
            population, left_on="LA_Name", right_on="LA_Name_ONS", how="left"
        )
        la_summary = la_summary.drop(columns=["LA_Name_ONS"], errors="ignore")

        # Calculate GPs per 100,000 population
        if "GP_FTE" in la_summary.columns:
            la_summary["GP_FTE_per_100k"] = (
                la_summary["GP_FTE"] / la_summary["Population"] * 100_000
            ).round(1)
        if "GP_Headcount" in la_summary.columns:
            la_summary["GP_HC_per_100k"] = (
                la_summary["GP_Headcount"] / la_summary["Population"] * 100_000
            ).round(1)
    elif not population.empty:
        print("  Warning: Could not match population data (no name column found).")

    # --- 6. Sort and output ---
    sort_col = "GP_FTE" if "GP_FTE" in la_summary.columns else "GP_Headcount"
    if sort_col in la_summary.columns:
        la_summary = la_summary.sort_values(sort_col, ascending=False)

    # Reorder columns for readability
    col_order = ["LA_Name"]
    if "LA_Code" in la_summary.columns:
        col_order.append("LA_Code")
    for c in ["Num_Practices", "GP_Headcount", "GP_FTE", "Population",
              "GP_HC_per_100k", "GP_FTE_per_100k"]:
        if c in la_summary.columns:
            col_order.append(c)
    la_summary = la_summary[[c for c in col_order if c in la_summary.columns]]

    # Save output
    ensure_data_dir()
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
    if sort_col in la_summary.columns:
        print(f"\nTop 10 LAs by {sort_col}:")
        display_cols = ["LA_Name"]
        if "LA_Code" in la_summary.columns:
            display_cols.append("LA_Code")
        display_cols.append(sort_col)
        if f"{sort_col}_per_100k" in la_summary.columns:
            display_cols.append(f"{sort_col}_per_100k")
        elif "GP_FTE_per_100k" in la_summary.columns:
            display_cols.append("GP_FTE_per_100k")
        print(
            la_summary.head(10)[
                [c for c in display_cols if c in la_summary.columns]
            ].to_string(index=False)
        )
        print(f"\nBottom 10 LAs by {sort_col}:")
        print(
            la_summary.tail(10)[
                [c for c in display_cols if c in la_summary.columns]
            ].to_string(index=False)
        )

    return la_summary


if __name__ == "__main__":
    main()
