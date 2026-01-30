# GP Workforce by Local Authority

Calculates the number of GPs working in each local authority area in England by mapping GP practice postcodes to local authorities and combining with population data.

## Data Sources

| Dataset | Source | Notes |
|---------|--------|-------|
| GP Workforce (practice-level) | [NHS Digital](https://digital.nhs.uk/data-and-information/publications/statistical/general-and-personal-medical-services) | Monthly publication with FTE and headcount by practice |
| Practice postcodes (epraccur) | [NHS ODS](https://digital.nhs.uk/services/organisation-data-service/data-search-and-export/csv-downloads/gp-and-gp-practice-related-data) | Downloaded automatically |
| Postcode-to-LA lookup (NSPL) | [ONS Open Geography Portal](https://geoportal.statistics.gov.uk/search?q=NSPL) | ~178 MB ZIP — must be downloaded manually |
| Population estimates | [ONS](https://www.ons.gov.uk/peoplepopulationandcommunity/populationandmigration/populationestimates/datasets/populationestimatesforukenglandandwalesscotlandandnorthernireland) | Mid-year estimates by local authority |

## Setup

```bash
pip install -r requirements.txt
```

## Manual Data Download

The NSPL file is too large for automated download. Before running the script:

1. Go to https://geoportal.statistics.gov.uk/search?q=NSPL
2. Download the latest NSPL ZIP file (e.g. "National Statistics Postcode Lookup (November 2025)")
3. Place the ZIP in the `data/` directory

## Usage

```bash
python gp_workforce_by_la.py
```

The script will:

1. **Download** the practice-level GP workforce CSV from NHS Digital (scrapes the publication page to find the download link)
2. **Download** the epraccur dataset to get practice postcodes
3. **Load** the NSPL postcode-to-LA lookup from `data/`
4. **Map** each GP practice to a local authority via its postcode
5. **Aggregate** GP FTE and headcount to local authority level
6. **Merge** with ONS mid-year population estimates
7. **Output** `data/gp_workforce_by_local_authority.csv`

## Output

The output CSV contains one row per local authority with:

| Column | Description |
|--------|-------------|
| `LA_Code` | ONS local authority district code (e.g. E08000003) |
| `LA_Name` | Local authority name |
| `Num_Practices` | Number of GP practices in the LA |
| `GP_Headcount` | Total GP headcount |
| `GP_FTE` | Total GP full-time equivalent |
| `Population` | Mid-year population estimate |
| `GP_HC_per_100k` | GP headcount per 100,000 population |
| `GP_FTE_per_100k` | GP FTE per 100,000 population |

## Configuration

Edit the URLs at the top of `gp_workforce_by_la.py` to change the data snapshot month or population year.

## Notes

- The practice-level CSV does not include fully estimated records. For sub-ICB and above aggregations, NHS Digital recommends using the individual-level CSV instead.
- FTE: 1.0 = 37.5 hours/week.
- GP figures exclude ad-hoc locums.
- Postcode-to-LA mapping uses ONS "best-fit" allocation from 2021 Census Output Areas.
