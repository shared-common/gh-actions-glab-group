# gh-actions-glab-group

GitHub Actions workflows and Python helpers for syncing every project discovered
under configured GitLab source groups into managed target groups, with optional
mirror-group configuration and batched reconcile jobs.

## Layout

- `.github/workflows/reconcile-target.yml` plans and reconciles the group sync.
- `.github/workflows/configure-target-mirrors.yml` provisions optional target mirror projects and push mirrors.
- `.github/scripts/` contains the Python entrypoints and shared sync logic.
- `configs/branch-policy.json` defines the managed branch-policy branches created for every target.

## Shared config checkout

The workflows check out the shared config repository `shared-common/gh-actions-cfg`
into the local runner path `gh-actions-cfg/`. The active group sync consumes:

- `gh-actions-cfg/gh-actions-glab-group/gl_forks_group.json`
- `gh-actions-cfg/gh-actions-glab-group/gl_forks_project.json`
- `gh-actions-cfg/gh-actions-glab-group/gl_forks_branch_exclusion.json`

`gl_forks_branch_exclusion.json` is optional. When present next to
`gl_forks_group.json`, the loader automatically excludes the listed target
projects from syncing the extra `gl_forks_group.json` branch set while still
applying `configs/branch-policy.json`, default-branch handling, and the rest of
the normal target reconciliation flow.

## Validation

Run these checks after changes:

```sh
python3 -m unittest discover -s tests
python3 -m compileall -q .github/scripts
yamllint .github/workflows/configure-target-mirrors.yml .github/workflows/reconcile-target.yml
```
