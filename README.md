# cs-confs-br-data
Raw data for cs-confs-br website

Data dumps are organized in CSV format, mostly extracted from public academic databases like:

- database1

Expected file format is `data/EVENT/EVENT_YEAR_YYYY_MM_DD.csv`, where the date corresponds to the extraction date.

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



