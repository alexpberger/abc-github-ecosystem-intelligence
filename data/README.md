# Raw Dataset

This directory contains the original, unmodified source data used by the ABC Retail Intelligence demo project.

## Dataset

**Online Retail II**

- **Source:** [UCI Machine Learning Repository](https://archive.ics.uci.edu/dataset/502/online+retail+ii)
- **Format:** Excel (.xlsx)
- **Filename:** `online_retail_II.xlsx`
- **Description:** Transaction data from a UK-based online retailer, covering December 2009 to December 2011.

## Download

1. Visit the [UCI dataset page](https://archive.ics.uci.edu/dataset/502/online+retail+ii).
2. Download the dataset.
3. Place the original Excel file in `data/raw/`.
4. Ensure the filename is `online_retail_II.xlsx`.

## Directory Structure

```text
data/
├── README.md
└── raw/
    └── online_retail_II.xlsx
```

## Usage

The original dataset serves as the source for the Fabric ingestion pipeline.

- Keep the source file unchanged.
- Do not commit raw dataset files to Git.
- All subsequent data processing takes place in Microsoft Fabric, following the Bronze, Silver and Gold Medallion architecture.

## Licence & Attribution

The dataset is provided by the UCI Machine Learning Repository. Refer to the original dataset page for licensing terms and attribution requirements.

When using or referencing the dataset, credit the original source.