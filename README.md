# dbt-deja-vu

Content fingerprints for dbt deploys on Snowflake. A deploy that rebuilds every descendant of a changed model can
stop at the first table whose content came out identical: the hooks record whether each build changed its table,
and a guard lets a materialization skip a child whose parents all came out unchanged. Build systems call this early
cutoff.

Everything is a no-op until `fingerprint: true` is set. Skipping needs two more things:
`fingerprint_skip_unchanged_upstream` on, and a materialization that calls `fingerprint_should_skip()` before it
builds. The package ships no materialization, so wrapping or overriding yours is up to you (see the guard below).

## Installation

```yaml
# packages.yml
packages:
  - git: "https://github.com/henryupton/dbt-deja-vu.git"
    revision: 0.0.1
```

## Usage

```yaml
# dbt_project.yml
on-run-start:
  - "{{ dbt_deja_vu.fingerprint_register() }}"

models:
  +post-hook: "{{ dbt_deja_vu.fingerprint_post(this) }}"

seeds:
  +post-hook: "{{ dbt_deja_vu.fingerprint_post(this) }}"

snapshots:
  +post-hook: "{{ dbt_deja_vu.fingerprint_post(this) }}"
```

```bash
dbt build --vars '{"fingerprint": true, "deploy_id": "deploy-123"}'
```

Every resource type the register enrols needs the post-hook: an enrolled node whose post-hook never runs stays `pending` and blocks every child.

`fingerprint_register()` appends, under a building command (see `fingerprint_commands`), one `pending` row per selected model, seed and snapshot (ephemeral models excepted) to a ledger table (`fingerprint_deploy_node`), stamped with Snowflake's clock as `snapshot_at`, the node's parents, its column shape and its source checksum. `fingerprint_post(this)` then compares the table as built with the table `at(timestamp => snapshot_at)` and appends a verdict row. The ledger is append-only, so threads finishing together never queue on a table lock. Per node and deploy the latest real verdict counts; a `skipped` row stands only when the node recorded no verdict in that deploy, and `pending` only when nothing else exists. A verdict is measured against the deploy's snapshot, so it is cumulative over every step and retry of the deploy, and a later skip wrote nothing.

| Verdict | Meaning | Blocks a child's skip |
|---------|---------|-----------------------|
| `new` | Object created or replaced after the snapshot, or a view | yes |
| `unchanged` | Nothing written and the row count is equal, or the rewritten months hash equal and nothing sits above the watermark | no |
| `appended` | Rows sit above the pre-build watermark (`max(_loaded_at)`) and nothing at or below it changed: nothing was written there, or the rewritten months hash equal | no |
| `modified` | A column was added, removed or retyped (a retype only when the shape was registered at run start), a month at or below the watermark counts or hashes differently, or rows other than pure appends were written to a table with no watermark at the snapshot | yes |
| `unhashable` | No loaded-at column on one side, a loaded-at column that is not a timestamp or date, or a table without row timestamps (`ROW_TIMESTAMP` off, which includes Iceberg) | yes |
| `error` | Relation missing, no Time Travel (retention 0), or a snapshot older than the table's Time Travel retention | yes |
| `skipped` | The guard skipped the build | no |

The comparison is cheap until it has to hash. Object state comes from `show tables like`, shape from `show columns`, and the counts are filtered scalar subqueries on `METADATA$ROW_LAST_COMMIT_TIME` and the watermark, which prune to the partitions the build wrote. Only then are the touched `_loaded_at` months hashed with `hash_agg` on both sides (every month when every row was rewritten), and per-month row counts catch a deletion in a month the build never touched. Rows with a null loaded-at value form their own bucket and count as old rows, so they cannot pass as appended. The watermark is compared in the column's own type, so `timestamp_ntz` columns are safe under any session timezone. `GEOGRAPHY` and `GEOMETRY` columns cannot be hashed and are left out; an `unchanged` or `appended` verdict names them in its `detail`. Per-month detail lands in `fingerprint_deploy_segment`.

`fingerprint_should_skip()` is for a materialization to call before building. It returns true only when all of these hold:

- `fingerprint_skip_unchanged_upstream` is on.
- At least one parent is registered in this deploy, and none is `pending`, `new`, `modified`, `unhashable` or `error`. Ephemeral parents are looked through to their own parents (an ephemeral model has no relation and no hooks, so it never carries a verdict).
- The node's own source checksum matches the one recorded at its last fingerprinted build of the same relation. A node whose own SQL changed builds however its parents came out, and a model newly guarded or under a new alias builds once first.
- Nothing the ledger recorded since that build (or skip) says a parent moved after the node read it. A parent's verdict is measured from its deploy's snapshot, so on its own it cannot vouch for a parent rebuilt after the node. The node builds on a parent verdict recorded since then whose snapshot predates the node's build (a later step of one deploy rebuilt the parent), on a blocking verdict or `pending` row for a parent in another deploy, and on a registration of the node itself under other SQL that recorded no verdict (a build whose hooks failed after the table was written).

Two blind spots. The checksum is the node's own source file, so a change that alters its output only through a macro it calls, a var or YAML-only config leaves it equal. And a run with the fingerprint off never reaches the ledger, so a parent it rebuilt without its children goes unseen. Deploy after either with `fingerprint_skip_unchanged_upstream` off.

`fingerprint_mark_skipped()` records the skip so the node's own children can read it. `integration_tests/macros/guarded_table.sql` shows the shape:

```jinja
{% set existing_relation = load_relation(this) %}
{% if existing_relation is not none and existing_relation.is_table and dbt_deja_vu.fingerprint_should_skip() %}
  {% do dbt_deja_vu.fingerprint_mark_skipped(detail='no blocking parent verdict') %}
  {% call statement('main') %}select 'skipped' as outcome{% endcall %}
  {{ return({'relations': [existing_relation]}) }}
{% endif %}
```

| Var | Default | Purpose |
|-----|---------|---------|
| `fingerprint` | `false` | Master switch; every macro is a no-op without it |
| `fingerprint_skip_unchanged_upstream` | `false` | Lets `fingerprint_should_skip()` return true |
| `fingerprint_commands` | `build`, `run`, `seed`, `snapshot`, `retry` | The commands that register, compared with `flags.WHICH`. `compile`, `docs generate`, `test` and (on Fusion) `show` fire `on-run-start` too, and a registration there would sit `pending` and block every child, so any command not listed registers nothing. A building command left off the list is as invisible to the guard as a run with the fingerprint off. An engine that does not expose `flags.WHICH` registers anyway |
| `deploy_id` | `invocation_id` | Shared by the steps of one deploy; registration is idempotent per deploy id |
| `fingerprint_schema` | `target.schema` | Schema holding the two ledger tables; point it at a dedicated schema so model-schema housekeeping (orphan drops, schema rebuilds) never sees the ledger |
| `fingerprint_loaded_at_column` | `_loaded_at` | Watermark and month-bucket column; per model via `meta.fingerprint_loaded_at` |
| `fingerprint_exclude` | `_batched_at`, `_written_at`, `_synthesised_at` | Columns left out of the hash; per model via `meta.fingerprint_exclude` |
| `fingerprint_exclude_scd_columns` | `true` | Also leaves the scd validity columns out of every model's hash: `_is_current` and `_valid_to`, or the names dbt-scd2-utils resolves for the model when its `dbt_scd2_utils` var or the model's config sets them. A new version closing an old row then reads `appended` rather than `modified`. The cost: a change that only moves validity (a `valid_to` fix, a `default_valid_to` change) reads `unchanged`, so children that read those columns can skip. Set `false` where that matters |
| `fingerprint_select_tag` | none | Fallback selection for runtimes without `selected_resources` |

Requirements: dbt Fusion, or dbt-core 1.10 or later (for `config.meta_get`); only Fusion has run the integration suite. Snowflake with Time Travel on the fingerprinted tables (transient tables cap at one day, which is enough for a deploy) and row timestamps (`ROW_TIMESTAMP_DEFAULT` on the account or schema, or per-table `ROW_TIMESTAMP`); a table without them reads `unhashable` rather than failing. A build has to keep the object for the snapshot to survive: truncate + insert, `insert overwrite`, `merge` and `insert` all do; `create or replace` reads `new`, which is the right verdict for a first build or a column-set change. The hooks run inside the node, so a SQL error they do not anticipate fails that node's build; the known cases (missing relation, no Time Travel, a snapshot older than the table's Time Travel retention, no row timestamps, unsupported column types) are recorded as verdicts instead.

## Integration tests

`integration_tests/` holds a stateful scenario suite that runs against a Snowflake dev schema. See
`integration_tests/README.md`.
