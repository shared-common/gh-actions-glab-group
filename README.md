# glab-groups-kali

Thin GitHub Actions wrapper for the Kali namespace mirror.

## Scope

- Loads `gh-actions-cfg/glab-groups-kali`
- Calls the reusable workflow in `glab-groups-shared@mcr/main`
- Uses the BWS target PAT secret `GL_PAT_GROUP_KALI_SVC`
- Runs deterministic mirror batch shards with five jobs max in parallel
- Schedules at minute 5 of hours 0, 6, 12, and 18 UTC
- Publishes plan, report, CSV, JSON, and Parquet artifacts for each run

## Validation

```sh
python3 -m unittest discover -s tests
```
