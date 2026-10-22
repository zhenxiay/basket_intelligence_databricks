# Basket Intelligence Databricks bundle

This repository contains a Databricks asset bundle for the data-loading jobs in `resources/`. The bundle builds a Python wheel with `uv build --wheel` during bundle operations.

## Local development

Install Databricks CLI **0.289.1**, uv **0.8.24**, and Python **3.12**. Authenticate to your workspace with a local Databricks CLI profile (for example, `databricks auth login`) or set `DATABRICKS_HOST` and `DATABRICKS_TOKEN` in your shell. Do not add workspace URLs or credentials to tracked files.

```sh
databricks bundle validate --target dev
databricks bundle deploy --target dev
```

The development target uses your authenticated workspace and a separate development deployment. To validate, plan, or deploy the **existing production** deployment locally, set `DATABRICKS_HOST`, authenticate as the existing deployment owner, and set `BUNDLE_VAR_prod_user_name` privately to that owner's username. The bundle derives the existing root path as `/Workspace/Users/${var.prod_user_name}/.bundle/${bundle.name}/${bundle.target}`. Confirm this matches your current deployment before deploying; a different owner or path would create a separate deployment.

Then run `databricks bundle validate --target prod`, `databricks bundle plan --target prod`, or (only if authorized) `databricks bundle deploy --target prod`. No production-specific identity, host, or root path value is stored in `databricks.yml`.

## GitHub Actions

Configure these three **repository Actions secrets** under Settings → Secrets and variables → Actions before enabling the workflow:

| Secret | Value to supply privately |
| --- | --- |
| `DATABRICKS_HOST` | Workspace host URL used by the existing deployment. |
| `DATABRICKS_TOKEN` | Databricks access token belonging to the existing workspace/deployment owner. |
| `DATABRICKS_PROD_USER_NAME` | Existing production deployment owner username (`BUNDLE_VAR_prod_user_name` in the bundle). The production root path is derived from this username and the bundle name/target; verify that it matches the current deployment. |

The [bundle workflow](.github/workflows/bundle.yml) validates and plans production changes for pull requests targeting `main` **only when the PR branch belongs to this repository**. Fork PRs skip the authenticated job and receive no Databricks secrets. No PR deploys. A manual `workflow_dispatch` run on `main` validates and plans that commit, then deploys it; production runs are serialized. Dispatching from another branch skips the job. The workflow reports only pass/fail for bundle commands, since raw CLI output could expose private values. If a step fails, reproduce it privately with the local CLI to inspect diagnostics.

**Trust boundary:** same-repository PR authors can run authenticated bundle commands through CI. Restrict repository write access accordingly, and revisit this model before granting new contributors branch access. Never include credentials, real workspace URLs, usernames, or deployment paths in committed files, PR descriptions, or public logs.
