# glab-groups-kali

Thin GitHub Actions wrapper for the Kali namespace mirror.

## Scope

- Loads `gh-actions-cfg/glab-groups-kali`
- Calls the reusable workflow in `glab-groups-shared@main`
- Runs 25-repository batches across five shared mirror lanes
- Publishes plan, report, CSV, JSON, and Parquet artifacts for each run

## Validation

```sh
python3 -m unittest discover -s tests
```
