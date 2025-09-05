# cs-confs-br-data
Raw data for cs-confs-br website

Data dumps are organized in CSV format, mostly extracted from public academic databases like:

- database1

Expected file format is `data/EVENT/EVENT_YEAR_X_YYYY_MM_DD.csv`, where the date corresponds to the extraction date and `X` is some informational prefix.

Events are encoded in acronyms to be systematically used.

Initially, the data/ folder will be ignored, until V0 is properly released.

Workflow is:

1. Extract data and add it in raw format
2. Perform Manual fixes, whenever possible
   - Check if papers with most citations are correct
3. Run calculation scripts and update the public csv

IMPORTANT: this repo should not store PDF versions of papers, only important metadata for calculation of metrics.

All computing conferences are welcome, specially those according to Google Scholar indexation metrics, with periodic publishing. This is focused to conferences in Brazil, but not restricted to it.
Feel free to request fixes or additions through Pull Request in this repo (periodically, changes will be put online in web version).

## How to run scripts?

There are many scripts here...

To calculate h5, first you need to properly prepare the CSV data, and select 5 different years or files.
Update target CSV files directly on `./scripts/calc_h5.py` script.
Then, just invoke `cd scripts && python3 calc_h5.py`

## What is this work?

This is a Personal project, aiming to help younger students and researchers to understand current metrics for popular Brazilian CS conferences.
Important: it is not intended to be used as an Individual Rank for researchers or to demonstrate the "Quality" of conferences, since the researchers must do this by themselves.
The numbers presented here are just a short demonstration of what can be achieved by using more integrated systems, so they certainly can vary across databases of publishers.

## Who are we?

We are professors and researchers from computer science field in Brazil, with the goal of providing an easier access to academic data, specially academic publishing in computer science conferences, in order to facilitate the decision-making of our colleagues.

This is all free software, so feel welcome to contribute and participate!

Prof. Igor Machado Coelho, Augusto Mendonça & Filipe Pessoa
