# Databricks API and token setup

How each teammate connects the Databricks CLI (and optionally the Python SDK) to our workspace. Notebooks running inside Databricks don't need any of this; they can query the tables directly. For syncing this repo into Databricks, see [databricks_github_practice.md](databricks_github_practice.md).

## Rules

- **Each person uses their own token.** Never share one, paste one into chat, or put one in a notebook, script, `.env` committed to git, or this repo.
- Tokens live only in your own `~/.databrickscfg`, which sits outside the repo.
- Give tokens a short lifetime (3 days covers the event) and revoke them afterwards under **User Settings → Developer → Access tokens**.

## 1. Install the CLI

Check whether it's already installed:

```powershell
databricks --version
```

We tested with v0.292.0. If it's missing, follow the install steps at <https://docs.databricks.com/aws/en/dev-tools/cli/install>.

## 2. Find the workspace address

Open the workspace in your browser and copy the address bar up to and including `.cloud.databricks.com`, for example:

```
https://dbc-xxxxxxxx-xxxx.cloud.databricks.com
```

Ask in the team chat if you don't have access yet.

**Don't use the workspace ID.** The long number (e.g. `7474…`) is the workspace ID, not the address. Using it fails with `no such host`.

## 3. Create a personal access token

In the workspace: **User Settings → Developer → Access tokens → Generate new token**.

| Field | Value |
|---|---|
| Name | `datathon-local-<yourname>` |
| Lifetime (days) | 3 |
| Auto-scope tokens | Off (it applies `all-apis` for 30 days) |
| Scope tab | **Other APIs** (BI Tools covers SQL only) |

Scopes to add (type each word into **Search scopes**; exact names may differ slightly):

| Scope | Needed for |
|---|---|
| `sql` | Running SQL through the SQL warehouse from the CLI or SDK |
| Unity Catalog (search "unity" or "catalog") | Listing catalogs, schemas, tables, volumes |
| `files` | Uploading or reading files in volumes |
| `workspace` | Only if you import notebooks from the repo into Databricks |
| `dashboards` | Creating, updating and publishing AI/BI dashboards from the CLI (`databricks lakeview ...`). Without it the CLI says the token "does not have required scopes: dashboards" |

Scopes are fixed when the token is created. If a command later fails because a scope is missing, generate a new token with it and repeat step 4.

## 4. Save the token in a CLI profile

```powershell
databricks configure --profile datathon --host https://dbc-xxxxxxxx-xxxx.cloud.databricks.com
```

Paste the token at the `Personal access token:` prompt (input is hidden). This writes a `[datathon]` section to `~/.databrickscfg`.

To change only the host later without re-entering the token:

```powershell
notepad $HOME\.databrickscfg
```

## 5. Check that it works

```powershell
databricks auth profiles
```

```powershell
databricks catalogs list --profile datathon
```

```powershell
databricks tables list workspace dsmlc --profile datathon
```

Expected tables:

```
workspace.dsmlc.bronze_daily
workspace.dsmlc.bronze_failures
workspace.dsmlc.bronze_jobs
```

## Where our data lives

| Item | Location |
|---|---|
| Catalog / schema | `workspace.dsmlc` |
| Bronze tables | `bronze_daily`, `bronze_jobs`, `bronze_failures` |
| Raw Parquet files | Volume `workspace.dsmlc.dataset` (`/Volumes/workspace/dsmlc/dataset/`) |
| SQL warehouse | Serverless Starter Warehouse. Get its ID with `databricks warehouses list --profile datathon` |

Expected row counts: 5,075,590 / 15,610 / 2,873 (2,905 / 1,903 / 825 distinct wells).

## Running SQL from the command line

Put the request in a JSON file outside the repo, e.g. `q.json`:

```json
{
  "warehouse_id": "<id from databricks warehouses list>",
  "wait_timeout": "50s",
  "statement": "SELECT COUNT(*) FROM workspace.dsmlc.bronze_daily"
}
```

```powershell
databricks api post /api/2.0/sql/statements/ --profile datathon --json "@q.json"
```

**Not yet verified for our setup.** With a token created without the `sql` scope, this returned `Error: Not Found`. Re-test after creating a token that includes `sql`.

## Python SDK (optional)

The `databricks-sdk` package is a new dependency for local scripts only; notebooks inside Databricks don't need it. It reads the same profile, so no token appears in code:

```powershell
pip install databricks-sdk
```

```python
from databricks.sdk import WorkspaceClient

w = WorkspaceClient(profile="datathon")
for t in w.tables.list(catalog_name="workspace", schema_name="dsmlc"):
    print(t.full_name)
```

Or set the profile once per terminal session instead of passing it in code:

```powershell
$env:DATABRICKS_CONFIG_PROFILE = "datathon"
```

Check that your local Python version is supported by the SDK release `pip` installs. Some machines on the team run Python 3.8.

## Troubleshooting

| Error | Cause | Fix |
|---|---|---|
| `dial tcp: lookup <number>.cloud.databricks.com: no such host` | Workspace ID used instead of the address | Redo step 4 with the `dbc-…` address |
| `Valid: NO` in `databricks auth profiles` | Token expired, revoked, or for a different workspace | Create a new token in the right workspace; redo step 4 |
| `Error: Not Found` on `/api/2.0/sql/statements/` | Token likely lacks the `sql` scope | New token with `sql`; redo step 4 |
| `PERMISSION_DENIED` on `workspace.dsmlc` | Your user lacks grants on Jacob's schema | Ask Jacob to grant `USE SCHEMA` and `SELECT` (and `MODIFY` if you write tables) |
| Schema names fail with "is not a valid name" in a script | Windows line endings (`\r`) on names read from command output | Strip whitespace from names before reusing them |
