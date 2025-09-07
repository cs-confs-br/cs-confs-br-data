# cs-confs-br-data
Raw data for cs-confs-br website: [CS-Confs-BR - Brazilian Computer Science Conference Metrics](https://cs-confs-br.github.io/)

Data dumps are organized in CSV format, mostly extracted from public academic databases like:

- openalex (OA)
- crossref (CR)
- google scholar (GS)

Expected file format is `data/EVENT/YYYY_MM/EVENT_BEGIN_END_X_YYYY_MM.csv`, where: data period is `[BEGIN,END]` in years; date `YYYY_MM` corresponds to the extraction date; and `X#` is some informational data prefix with incremental version `#`.
Data prefixes: `OA` (openalex), `PP` (publish or perish), `CR` (crossref); and note that they can be composed, such as, `PPCR` (data extracted from crossref using publish or perish).

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

### How to run scripts?

There are many scripts here...

To calculate h5, first you need to properly prepare the CSV data, and select 5 different years or files.
Update target CSV files directly on `./scripts/calc_h5.py` script.
Then, just invoke `cd scripts && python3 calc_h5.py`


## FAQ
### What is this work?

This began as a personal project, aiming to help younger students and researchers to understand current metrics for popular Brazilian CS conferences, which are not easily found elsewhere on the web.
Important: this project is not intended to be used as an Individual Rank for researchers, students, professors or to "demonstrate the quality" of conferences, since the researchers must do this judgement by themselves regarding their own research communities and personal experiences.
The numbers presented here are just a short demonstration of what can be achieved by using even more integrated systems, so they certainly can vary across databases of publishers.

### Why focusing on Computer Science conferences?

In Brazil, we used to have a "quality-ranking" for journals named Qualis, computed by CAPES in order to rank the Brazilian Graduate Programs. This Qualis metrics was also extended for conferences, which is largely used by Computer Science community in general. Recently, the Qualis system is now being unified, standardized and sometimes replaced by some other global academic metrics, such as Impact Factor for journals and H5 metrics, typically from Google Scholar, for conferences. Since not all conferences are listed in Google Scholar, we extract data from open academic databases in order to provide colleagues and students with such missing and fundamental decision-making data.

### Is this limited to Brazil?

Not necessarily. 
We live in Brazil and know the Brazilian research system, that is why we do efforts to "list" all these conferences, even smaller ones.
But this can be also extended to other countries, fields and research communities, 
as long as we have some help doing it!


### Who are we?

We are professors and researchers from computer science field in Brazil, with the goal of providing an easier access to academic data, specially academic publishing in computer science conferences, in order to facilitate the decision-making of our colleagues.

We do this in our free time because we love it!
This is all free software, so feel welcome to contribute and participate!

- Prof. Igor Machado Coelho
   * Adjunct professor at Instituto de Computação, Universidade Federal Fluminense (UFF), Niterói-RJ, Brazil

And my PhD students...
- Augusto Mendonça & Filipe Pessoa

Disclaimer: the university UFF is NOT involved in this project and does not endorse any information put here, all being our sole individual responsibility.

#### How to collaborate?

This project is open and collaborative, so if you find any problem, please Open an Issue.

## License

All scripts and data under MIT License

2025