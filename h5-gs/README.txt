========= H5 from Google Scholar =========

GENERAL WORKFLOW

We initially use input from CAPES Computer Science committee that lists 
important CS conferences for CS Graduate Programs in 2017-2020:
- See file: CAPES-CC-2017-2020-Qualis.csv

Then, we crawl h5 data from Google Scholar (database 2025 July) for these 
confereces, that covers around 50% of the conferences:
- See file: out-h5-gs-2025-09.csv

Some conferences have name conflicts and other problems that need to be 
resolved with our manual list hosted on data/ folder:
- See file: data/confs-list.csv

Finally, h5 index is also calculated for other conferences on data/ using 
extracted data from other academic databases, covering GS-unlisted events:
- See file: scripts/calc_h5.py

