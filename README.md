# GP Workforce by Local Authority

Calculates the number of GPs working in each local authority area in England by mapping GP practice ODS codes to local authorities and combining with population data.

## Data Sources

| Dataset | Source | Notes |
|---------|--------|-------|
| GP Workforce (practice-level) | [NHS Digital](https://digital.nhs.uk/data-and-information/publications/statistical/general-and-personal-medical-services) | Monthly publication with FTE and headcount by practice |
| GP Providers register | CQC | `GP providers.xlsx` — maps practice ODS codes to local authorities |
| Population estimates | [ONS](https://www.ons.gov.uk/peoplepopulationandcommunity/populationandmigration/populationestimates/datasets/populationestimatesforukenglandandwalesscotlandandnorthernireland) | Mid-year estimates by local authority |

## Setup

```bash
pip install -r requirements.txt
```

## Usage

```bash
python gp_workforce_by_la.py
```

The script will:

1. **Download** the practice-level GP workforce CSV from NHS Digital (scrapes the publication page to find the download link)
2. **Load** `GP providers.xlsx` to map each practice ODS code to a local authority
3. **Aggregate** GP FTE and headcount to local authority level
4. **Download** ONS mid-year population estimates and merge
5. **Output** `data/gp_workforce_by_local_authority.csv`

## Output

The output CSV contains one row per local authority with:

| Column | Description |
|--------|-------------|
| `LA_Name` | Local authority name |
| `LA_Code` | ONS local authority district code (e.g. E08000003) |
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
