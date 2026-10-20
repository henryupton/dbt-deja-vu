#!/usr/bin/env python3
"""Content fingerprint scenario suite.

Drives the fingerprint fixtures (models/fingerprint*, seeds/fingerprint) through 102 ordered
scenarios. After each, the matches_expected_seed test on fp_ledger asserts the verdict recorded
for every selected fixture against seeds/fingerprint/fp_expected_<n>.csv. Each scenario is its
own deploy id, so the ledger accumulates and reruns never collide. Scenarios are stateful and
run in order; --only / --from re-run a slice against whatever state the tables are in. fp_ledger
and the singular tests are enabled only with the fingerprint var on, which the runner sets on
every step except scenario 50's first (built with the fingerprint off on purpose) and the prune
group's rebuilds of its frozen inputs, which no deploy sees.

The suite is weighted towards false negatives: every way a genuine change could be waved
through as unchanged or appended, and every way a guarded child could be skipped when it must
build. Scenario 12 deletes fixture rows inside the build with a pre-hook (fp_delete_customer);
a change made before the build's snapshot belongs to no deploy and is correctly invisible.
Scenarios 33 to 38 are common warehouse shapes: a versioned dim, a batch-stamped staging merge
that starts empty, a registry seed with no loaded-at column, and a seed whose CSV the runner
rewrites mid-sequence (FILE_VARIANTS); the originals are restored when the runner exits, and a
variant a killed run left behind is replaced by the committed copy from git.
Scenarios 39 to 57 are the guard's remaining branches (ephemeral, multi-parent, transitive,
checksum, no baseline, pending, retry) and the hooks' edge cases (no row timestamps, numeric,
geography and variant columns, quoted identifiers, null and non-tz loaded-at, the scd validity
blind spot). A step marked expect_fail must exit non-zero: it leaves a parent pending on purpose.
Scenario 54 builds a guarded child in one step and skips it in the next; its new verdict still counts.
Scenario 55 skips a guarded child in one step and rebuilds it in the next, once a full refresh has
replaced a parent; the rebuild records new.
Scenarios 56 and 57 replace a parent in a deploy that builds none of its guarded children; the next
deploy builds them on that verdict, though the parent reads unchanged there.
Scenarios 58 to 63 are the package knobs. A step may name its command (default build): scenario 59
compiles with the fingerprint on, which must register nothing. Scenario 60 excludes a hookless
materialization, which is never registered and whose guarded child always builds. Scenario 61 opts a
guarded child out of skipping with meta fingerprint_skip false. Scenario 62 closes an scd row, which
reads modified under the default; scenarios 4 and 48 set fingerprint_exclude_scd_columns true to pin
the opt-in. Scenario 63 registers one row per statement across two steps of one deploy.
Scenarios 64 to 69 are the compiled-SQL check: a rewritten macro (66) and a var (67) move a
guarded child's compiled SQL with its checksum equal, and it builds; a hashless 0.0.1 ledger row
as the latest build never lets a child skip (68); with nothing changed every child skips (65, 69).
Scenarios 70 to 77 are faults inside the fingerprint, which must never fail a node or become a skip:
a ledger schema the role cannot create in, contained (71), the segment ledger renamed away while a
post-hook hashes (72), and a parent built in a run that registered nothing (73). Uncontained, that
registration failure stops the run before a parent's change builds (75), so the next deploy still
sees the change and builds its child (76). Nothing is recorded when the ledger is unreachable, so a
step may also assert on its output: expect_log patterns must appear, forbid_log patterns must not.
Scenarios 78 to 86 lose a verdict on purpose (the node ledger renamed away for the length of one
node's fingerprint post-hook): the registration that run never settled keeps blocking, though an
earlier step's verdict outranks pending. A parent's builds its child in that run (79) and in a later
deploy (81); a guarded child's own builds it at its next selection, with its SQL back as it was, in
another deploy (84) or a later step of its own (85).
Scenarios 87 to 90 mask a run-start audit stamp out of the compiled SQL hash: the stamped child skips
once masked, the child reading the clock in logic still builds.
Scenarios 91 to 102 demonstrate pruning on the EDP trial line (models/fingerprint_prune). Each builds
its selection twice from the same frozen inputs (models/fingerprint_prune_input, rebuilt between
scenarios at the scenario's stage): a control arm with the guard off, then a treatment arm with it
on, each its own deploy on its own schema and ledger (macros/fp_prune.sql), asserted against
fp_expected_<n>_control and fp_expected_<n>_treatment. A third step then tests
fp_prune_arms_agree, a content oracle independent of the package's hash: every fixture must match
across the arms, bar the staleness scenarios 97, 99 and 100 declare (rows behind, or rows
changed). --write-matrix regenerates the matrix in README.md from those seeds, and --check-matrix
fails when it is out of date; neither runs dbt. A run that includes a prune scenario makes the
same check before any dbt runs.

Usage:
    ./test_fingerprint_sequence.py [--profile default] [--target dev] [--dbt /path/to/dbt]
                                   [--only 1,2,3] [--from 15] [--show] [--no-seed]
    ./test_fingerprint_sequence.py --write-matrix | --check-matrix
"""

from __future__ import annotations

import argparse
import atexit
import datetime as dt
import json
import re
import subprocess
import sys
from pathlib import Path

CORE = "tag:fingerprint"
OVER = "tag:fp_overwrite"
EVOLVE = "tag:fp_evolve"
GUARD = "tag:fp_guard"
EDP = "tag:fp_edp"
EDGE = "tag:fp_edge"
KNOB = "tag:fp_knob"
SQL = "tag:fp_sql"
FAULT = "tag:fp_fault"
SETTLE = "tag:fp_settle"
FAULT_FIXTURES = "fp_fault_parent fp_fault_child fp_fault_parked fp_fault_child_of_parked"

# Files the runner rewrites mid-sequence: seeds so a reload carries changed content, one guarded model so
# its checksum moves while its parents stay put, and two macros guarded models render so their compiled SQL
# moves while their checksums stay put. A variant stays in place for the scenarios that follow;
# the committed originals are restored when the runner exits. A variant left behind by a killed run is
# recognised at start and the committed copy is taken from git instead.
SEED_LIVE = "seeds/fingerprint/fp_seed_live.csv"
REGISTRY = "seeds/fingerprint/fp_registry.csv"
CHILD_MULTI = "models/fingerprint/fp_edge_child_multi.sql"
CHILD_MULTI_HEAD = (
    "{{\n"
    "    config(\n"
    "        materialized='guarded_table',\n"
    "        tags=['fp_edge']\n"
    "    )\n"
    "}}\n"
    "\n"
    "{# Two parents: one blocking verdict on either side builds it. The runner rewrites this file in two #}\n"
    "{# scenarios so its checksum moves while both parents stay unchanged. #}\n"
)
CHILD_MULTI_SELECT = (
    "select a.customer_id, a.email, b.status\n"
    "from {{ ref('fp_edge_parent_a') }} a\n"
    "join {{ ref('fp_edge_parent_b') }} b on a.customer_id = b.customer_id\n"
)
CHILD_MULTI_CHANGE = "{# Rewritten by the runner: this comment is the change. #}\n"
# A macro a guarded child renders: rewriting it moves the child's compiled SQL and output, not its checksum.
SQL_MACRO = "macros/fp_sql_expr.sql"
SQL_MACRO_HEAD = (
    "{#\n"
    "  Test-only: the status expression fp_sql_child_of_macro renders. The runner rewrites this file in scenario 66,\n"
    "  so the child's compiled SQL and output move while its own file, and so its checksum, stay put.\n"
    "#}\n"
    "{% macro fp_sql_status_expr(column) %}\n"
)
SQL_MACRO_TAIL = "{% endmacro %}\n"
# The as-of join only fp_prune_fct_daily renders: the variant writes the same predicates the other way round, so the
# fact's compiled SQL moves while its output and its checksum stay put.
PRUNE_AS_OF = "macros/fp_prune_as_of_join.sql"
PRUNE_AS_OF_HEAD = (
    "{#\n"
    "  Test-only: EDP's as_of_join shape, which fp_prune_fct_daily renders and no other model does. A left join onto the\n"
    "  scd dim on a timestamp cursor inside each version's validity, falling back to the entity's first version for a\n"
    "  row dated before it. The runner rewrites this file from scenario 93 with the same predicates the other way round,\n"
    "  so the fact's compiled SQL moves while its output and its own checksum stay put.\n"
    "#}\n"
    "{% macro fp_prune_as_of_join(left, right, join_key, left_updated_at) %}\n"
    "left join (\n"
    "    select *, min(_valid_from) over (partition by {{ join_key }}) as _as_of_first_valid_from\n"
    "    from {{ right }}\n"
    ") as {{ right }}\n"
)
FILE_VARIANTS = {
    SEED_LIVE: {
        # customer 3 flips INACTIVE -> ACTIVE with the same timestamps
        "changed": (
            "customer_id,customer_name,email,status,_updated_at,_loaded_at\n"
            "2,Grace Hopper,grace@example.com,ACTIVE,2026-06-15 09:00:00+0000,2026-06-15 09:05:00+0000\n"
            "3,Alan Turing,alan@example.com,ACTIVE,2026-06-16 09:00:00+0000,2026-06-16 09:05:00+0000\n"
            "5,Margaret Hamilton,margaret@example.com,ACTIVE,2026-07-11 09:00:00+0000,2026-07-11 09:05:00+0000\n"
            "6,Barbara Liskov,barbara@example.com,ACTIVE,2026-08-05 09:00:00+0000,2026-08-05 09:05:00+0000\n"
            "7,Frances Allen,frances@example.com,ACTIVE,2026-08-06 09:00:00+0000,2026-08-06 09:05:00+0000\n"
            "2,Grace Hopper,grace@example.com,INACTIVE,2026-08-20 09:00:00+0000,2026-08-20 09:05:00+0000\n"
            "8,Radia Perlman,radia@example.com,ACTIVE,2026-06-20 09:00:00+0000,2026-06-20 09:05:00+0000\n"
        ),
    },
    REGISTRY: {
        # one label changes; with no loaded-at column the fingerprint cannot see it
        "changed": (
            "event_type,label,is_active\n"
            "gen_impression,Generation impression (AI),true\n"
            "item_download,Item download,true\n"
            "subscription_started,Subscription started,true\n"
        ),
    },
    CHILD_MULTI: {
        # the same model with one more comment line: same output, different checksum
        "changed": CHILD_MULTI_HEAD + CHILD_MULTI_CHANGE + CHILD_MULTI_SELECT,
        # and with one more again, so the checksum moves a second time
        "changed_again": (
            CHILD_MULTI_HEAD + CHILD_MULTI_CHANGE
            + "{# Rewritten again: a second checksum for the same output. #}\n"
            + CHILD_MULTI_SELECT
        ),
    },
    SQL_MACRO: {
        # upper(status) becomes lower(status): every status value changes
        "changed": SQL_MACRO_HEAD + "  {{ return('lower(' ~ column ~ ')') }}\n" + SQL_MACRO_TAIL,
    },
    PRUNE_AS_OF: {
        "changed": (
            PRUNE_AS_OF_HEAD
            + "    on {{ right }}.{{ join_key }} = {{ left }}.{{ join_key }}\n"
            "    and (\n"
            "        ({{ right }}._valid_from <= {{ left }}.{{ left_updated_at }}"
            " and {{ right }}._valid_to > {{ left }}.{{ left_updated_at }})\n"
            "        or ({{ right }}._valid_from > {{ left }}.{{ left_updated_at }} and {{ right }}._change_type = 'I'\n"
            "            and {{ right }}._as_of_first_valid_from = {{ right }}._valid_from)\n"
            "    )\n"
            + SQL_MACRO_TAIL
        ),
    },
}

# Fixture state after scenario 14, reused by the later groups so they start from a known shape.
STEADY = {"fp_source": "deleted_b", "fp_upper_email": True, "fp_extra_column": True, "fp_null_email_customer": 3}

# The edge group's knobs accumulate; each scenario carries the state the previous one left.
EDGE_41 = {**STEADY, "fp_edge_b_flip": True, "fp_edge_null_ids": [2], "fp_edge_geo_tier": "gold"}
EDGE_42 = {**EDGE_41, "fp_edge_a_flip": True, "fp_edge_quoted_flip": True, "fp_edge_ntz_extra": True,
           "fp_edge_date_extra": ["2026-08-20"]}
EDGE_43 = {**EDGE_42, "fp_edge_null_extra": "ACTIVE", "fp_edge_date_extra": ["2026-08-20", "2026-08-21"]}
EDGE_44 = {**EDGE_43, "fp_edge_null_extra": "INACTIVE"}
EDGE_45 = {k: v for k, v in EDGE_44.items() if k != "fp_edge_null_extra"}
SCD2_PAIR = "fp_edge_scd2 fp_edge_child_of_scd2 fp_ledger"

# The package-knob group: the vars a project sets to fit the fingerprint to its own deploy.
KNOB_VARS = {**STEADY, "fingerprint_exclude_materializations": ["hookless_table"]}
KNOB_62 = {**KNOB_VARS, "fp_knob_scd_source": "versioned"}

# The compiled-SQL group: the var change of 67 carries into the scenarios after it, as the macro variant of 66 does.
SQL_67 = {**STEADY, "fp_sql_label": "changed"}

# The fault group: a ledger the role cannot create, and log lines that show each failure was contained. Nothing is
# recorded when the ledger is unreachable, so the step's output is the evidence that the guards built.
UNREACHABLE = {**STEADY, "fingerprint_schema": "INFORMATION_SCHEMA"}
CONTAINED = {**UNREACHABLE, "fingerprint_contain_register_failure": True}
REGISTER_FAILED = (r"registration for deploy \S+ failed creating the ledger in DEV_ANALYTICS\.INFORMATION_SCHEMA "
                   r"\(3001: ")
GUARD_FAILED = r"fingerprint: WARNING: {} guard failed reading its parents' verdicts \(2003: "
NO_VERDICT = r"fingerprint: WARNING: \S+\.{} recorded no verdict for deploy "
UNREAD = NO_VERDICT + r".*\)\. Its registration could not be read either"
NO_SKIP = r"guarded_table: skipping"
BUILT = r"Succeeded \[[^\]]*\] model \S+\.{} "

# The settle group: a registration whose run never recorded a verdict (the node ledger taken away for the length of
# one node's fingerprint post-hook) must keep blocking, in that run and in later ones.
SETTLE_FLIP = {**STEADY, "fp_settle_flip": True}
LOSE = "fp_settle_lose"
UNRECORDED_PARENT = (r"fingerprint: fp_settle_child has 1 parent\(s\) built in this run that recorded no verdict or "
                     r"skip in this run; building")
UNSETTLED_PARENT = (r"fingerprint: fp_settle_child has 1 parent registration\(s\) since its last build that never "
                    r"recorded a verdict or skip; building")
UNSETTLED_SELF = (r"fingerprint: fp_settle_child was registered since its last build by 1 run\(s\) that recorded no "
                  r"verdict or skip for it; building")
SETTLE_LABEL = {**STEADY, "fp_settle_label": "changed"}

# The mask group: a run-start audit stamp rendered into compiled SQL, masked by the template that renders it.
MASK = "tag:fp_mask"
MASKED = {**STEADY, "fingerprint_sql_mask_run_started_at": ["'%Y-%m-%dT%H:%M:%S.%fZ'::timestamp_tz as _batched_at"]}

# The prune group: the EDP trial line (models/fingerprint_prune) built twice per scenario from the same frozen inputs
# (models/fingerprint_prune_input, rebuilt between scenarios at the scenario's stage), a control arm with the guard
# off and a treatment arm with it on, in their own schemas with their own ledgers. Every fixture renders the EDP audit
# stamp, and the group masks its template. Then fp_prune_arms_agree, the content oracle, is tested.
PRUNE = "tag:fp_prune"
PRUNE_INPUT = "tag:fp_prune_input"
PRUNE_ARMS = ("control", "treatment")
PRUNE_VARS = {"fingerprint_sql_mask_run_started_at": MASKED["fingerprint_sql_mask_run_started_at"],
              "fingerprint_exclude_materializations": ["hookless_table"]}
PRUNE_UNMASKED = {k: v for k, v in PRUNE_VARS.items() if k != "fingerprint_sql_mask_run_started_at"}
FULL_REFRESH = ["--full-refresh"]
# Matrix rows, in lineage order. The excluded leaf is never registered, so no expected seed names it.
PRUNE_NODES = ["fp_prune_evt_report", "fp_prune_int_adh", "fp_prune_dim_agent", "fp_prune_fct_daily", "fp_prune_dm_fct",
               "fp_prune_dm_dim_agent", "fp_prune_dim_agent_current", "fp_prune_rpt", "fp_prune_dim_country",
               "fp_prune_dm_country", "fp_prune_dashboard", "fp_prune_striking"]
PRUNE_UNREGISTERED = {"fp_prune_dashboard"}
PRUNE_SCHEMA_LINE = r"fp_prune: (input|control|treatment) schema (\S+)"
# A test that is disabled or selects nothing exits 0, so the oracle step must show it ran and passed.
ORACLE_PASSED = r"Passed \[[^\]]*\] test +\S+\.fp_prune_arms_agree\b"
NOTHING_SELECTED = r"NoNodesForSelectionCriteria|NoNodesSelected"
MATRIX_START = "<!-- fp_prune matrix: generated by ./test_fingerprint_sequence.py --write-matrix; do not edit by hand -->"
MATRIX_END = "<!-- fp_prune matrix end -->"

SCENARIOS = [
    # --- core: initial load and the plain shapes ---------------------------------------------
    dict(id=1, desc="initial load, create or replace everywhere", select=CORE, flags=["--full-refresh"],
         vars={"fp_source": "base", "fp_full_refresh_strategy": "replace", "fp_allow_stg_full_refresh": True}),
    dict(id=2, desc="same data, incremental", select=CORE, vars={"fp_source": "base"}),
    dict(id=3, desc="two rows appended", select=CORE, vars={"fp_source": "appended"}),
    # Pins the opt-in: with the scd columns excluded the closed row is invisible and the scd2 dim reads appended.
    dict(id=4, desc="later version of an old key (scd columns excluded)", select=CORE,
         vars={"fp_source": "versioned", "fingerprint_exclude_scd_columns": True}),
    dict(id=5, desc="logic change (upper email), full refresh", select=CORE, flags=["--full-refresh"],
         vars={"fp_source": "versioned", "fp_upper_email": True}),
    dict(id=6, desc="full refresh, nothing changed", select=CORE, flags=["--full-refresh"],
         vars={"fp_source": "versioned", "fp_upper_email": True}),
    dict(id=7, desc="column added, full refresh", select=CORE, flags=["--full-refresh"],
         vars={"fp_source": "versioned", "fp_upper_email": True, "fp_extra_column": True}),
    dict(id=8, desc="same again, incremental", select=CORE,
         vars={"fp_source": "versioned", "fp_upper_email": True, "fp_extra_column": True}),
    # --- genuine changes that must not be waved through ----------------------------------------
    dict(id=9, desc="in-place value change, same timestamps (merge rewrites the row)", select=CORE,
         vars={"fp_source": "inplace", "fp_upper_email": True, "fp_extra_column": True}),
    dict(id=10, desc="backdated row inserted below the watermark", select=CORE,
         vars={"fp_source": "backdated", "fp_upper_email": True, "fp_extra_column": True}),
    dict(id=11, desc="row removed at source, full refresh", select=CORE, flags=["--full-refresh"],
         vars={"fp_source": "deleted", "fp_upper_email": True, "fp_extra_column": True}),
    dict(id=12, desc="row deleted by a pre-hook in a month the build never touches (append and merge)", select=CORE,
         vars={"fp_source": "deleted_b", "fp_upper_email": True, "fp_extra_column": True, "fp_delete_customer": 4}),
    dict(id=13, desc="value flipped to null for one old row, full refresh", select=CORE, flags=["--full-refresh"],
         vars=STEADY),
    dict(id=14, desc="only excluded columns change (sysdate), full refresh", select=CORE, flags=["--full-refresh"],
         vars=STEADY),
    # --- insert overwrite shape ----------------------------------------------------------------
    dict(id=15, desc="overwrite: initial", select=OVER, flags=["--full-refresh"], vars=STEADY),
    dict(id=16, desc="overwrite: same rows", select=OVER, vars=STEADY),
    dict(id=17, desc="overwrite: same rows, random order", select=OVER, vars={**STEADY, "fp_shuffle": True}),
    dict(id=18, desc="overwrite: every row duplicated", select=OVER, vars={**STEADY, "fp_duplicate_rows": True}),
    dict(id=19, desc="overwrite: every _loaded_at bumped to now", select=OVER, vars={**STEADY, "fp_bump_loaded_at": True}),
    dict(id=20, desc="overwrite: back to real _loaded_at", select=OVER, vars=STEADY),
    dict(id=21, desc="overwrite: same rows again", select=OVER, vars=STEADY),
    # --- schema evolution without replace ------------------------------------------------------
    dict(id=22, desc="evolve: initial without the extra column", select=EVOLVE, flags=["--full-refresh"],
         vars={**STEADY, "fp_extra_column": False}),
    dict(id=23, desc="evolve: column added by ALTER (append_new_columns / sync_all_columns)", select=EVOLVE, vars=STEADY),
    dict(id=24, desc="evolve: column dropped by ALTER (sync_all_columns)", select="fp_evt_sync fp_child_of_sync fp_ledger",
         vars={**STEADY, "fp_drop_column": True}),
    # --- guard must build --------------------------------------------------------------------------
    dict(id=25, desc="guard: initial", select=GUARD, flags=["--full-refresh"], vars=STEADY),
    dict(id=26, desc="guard: unhashable, view, unregistered seed, no Time Travel", select=GUARD, vars=STEADY),
    dict(id=27, desc="guard: seed selected, reloaded by truncate + insert with the same content (child skips)",
         select=f"{GUARD} fp_raw_deleted_b", vars=STEADY),
    dict(id=28, desc="guard switched off: child builds though parents unchanged", select=CORE,
         vars={**STEADY, "fingerprint_skip_unchanged_upstream": False}),
    dict(id=29, desc="late child with no table builds; existing child skips", select=f"{CORE} tag:fp_late",
         vars={**STEADY, "fp_enable_late_child": True}),
    # --- two-step deploy shape: parents in step A, children in step B, one deploy id ---------------
    dict(id=30, desc="two-step: parent unchanged in A, child skipped in B", steps=[
        dict(select="fp_dim_scd2", vars=STEADY),
        dict(select="fp_child_guarded fp_ledger", vars=STEADY),
    ]),
    dict(id=31, desc="two-step: parent modified in A (email case reverted), child built in B", steps=[
        dict(select="fp_dim_scd2", flags=["--full-refresh"], vars={**STEADY, "fp_upper_email": False}),
        dict(select="fp_child_guarded fp_ledger", vars={**STEADY, "fp_upper_email": False}),
    ]),
    # --- seed replaced -------------------------------------------------------------------------------
    dict(id=32, desc="guard: seed selected with --full-refresh, create or replace (child builds)",
         select=f"{GUARD} fp_raw_deleted_b", flags=["--full-refresh"], vars=STEADY),
    # --- common warehouse shapes: versioned dim, batch-stamped staging merge, registry seed, seed change ---
    # The scd dim is replaced on the initial so a rerun over an existing table reads new, not modified.
    dict(id=33, desc="edp: initial (versioned dim, empty batch stage, registry and live seeds)", select=EDP,
         flags=["--full-refresh"], vars={**STEADY, "fp_batch_stage": 0, "fp_full_refresh_strategy": "replace"}),
    dict(id=34, desc="edp: rows land in the empty stage; seeds reloaded unchanged (registry unhashable)", select=EDP,
         vars={**STEADY, "fp_batch_stage": 1}),
    dict(id=35, desc="edp: late chunk of the current batch lands exactly at the watermark", select=EDP,
         vars={**STEADY, "fp_batch_stage": 2}),
    dict(id=36, desc="edp: next batch", select=EDP, vars={**STEADY, "fp_batch_stage": 3}),
    dict(id=37, desc="edp: seed content changed (live seed row status, registry label)", select=EDP,
         vars={**STEADY, "fp_batch_stage": 3}, files={SEED_LIVE: "changed", REGISTRY: "changed"}),
    dict(id=38, desc="edp: versioned dim gets backdated keys, child builds", select=EDP,
         vars={**STEADY, "fp_source": "inplace", "fp_batch_stage": 3}),
    # --- the guard's remaining branches and the hooks' edge cases ---------------------------------------
    dict(id=39, desc="edge: initial, create or replace everywhere", select=EDGE, flags=["--full-refresh"],
         vars={**STEADY, "fp_full_refresh_strategy": "replace"}),
    dict(id=40, desc="edge: nothing changed (ephemeral looked through, grandchild skips, unhashable children build, "
                     "geography left out, quoted exclusion holds)", select=EDGE, vars=STEADY),
    dict(id=41, desc="edge: one of two parents modified; a row's stamp moves to null; a value inside an object changes",
         select=EDGE, vars=EDGE_41),
    dict(id=42, desc="edge: the ephemeral's parent modified; quoted value flips; ntz row appended one hour up; "
                     "date row on the watermark day", select=EDGE, vars=EDGE_42),
    dict(id=43, desc="edge: a null-stamped row is inserted (not appended); a later date row is appended", select=EDGE,
         vars=EDGE_43),
    dict(id=44, desc="edge: the null-stamped row changes", select=EDGE, vars=EDGE_44),
    dict(id=45, desc="edge: the null-stamped row is removed", select=EDGE, vars=EDGE_45),
    dict(id=46, desc="edge: a guarded child's own SQL changes while its parents stay unchanged", select=EDGE,
         vars=EDGE_45, files={CHILD_MULTI: "changed"}),
    dict(id=47, desc="edge: the changed SQL is now the baseline; it skips again", select=EDGE, vars=EDGE_45),
    dict(id=48, desc="edge: scd2 full refresh moving only _valid_to reads unchanged with the scd columns excluded (pinned)",
         select=SCD2_PAIR, flags=["--full-refresh"],
         vars={**EDGE_45, "default_valid_to": "2999-01-01 00:00:00", "fingerprint_exclude_scd_columns": True}),
    dict(id=49, desc="edge: the same move with the scd columns included reads modified", select=SCD2_PAIR,
         flags=["--full-refresh"], vars={**EDGE_45, "fingerprint_exclude_scd_columns": False}),
    dict(id=50, desc="edge: a table built with the fingerprint off, then guarded: no baseline, builds once", steps=[
        dict(select="fp_edge_adopted", vars={**EDGE_45, "fingerprint": False, "fp_enable_adopted": True}),
        dict(select=EDGE, vars={**EDGE_45, "fp_enable_adopted": True}),
    ]),
    dict(id=51, desc="edge: the adopted table skips now it has a baseline", select=EDGE,
         vars={**EDGE_45, "fp_enable_adopted": True}),
    dict(id=52, desc="two-step: a parent fails in A and stays pending; its children build in B, the other's skips", steps=[
        dict(select="fp_edge_parent_a fp_edge_parent_b", vars={**EDGE_45, "fp_edge_fail_b": True}, expect_fail=True),
        dict(select="fp_edge_child_of_eph fp_edge_child_multi fp_edge_grandchild fp_ledger", vars=EDGE_45),
    ]),
    dict(id=53, desc="retry: the failed parent is rerun under the same deploy id and settles against the original snapshot",
         steps=[
             dict(select="fp_edge_parent_a fp_edge_parent_b", vars={**EDGE_45, "fp_edge_fail_b": True}, expect_fail=True),
             dict(select="fp_edge_parent_a fp_edge_parent_b", vars=EDGE_45),
             dict(select=EDGE, vars=EDGE_45),
         ]),
    # One guarded child decided twice in a deploy: its real verdict counts over the skip, whichever came first.
    dict(id=54, desc="two-step: a guarded child built in A (own SQL changed) and skipped in B keeps its verdict; "
                     "its child builds", files={CHILD_MULTI: "changed_again"}, steps=[
        dict(select="fp_edge_parent_a fp_edge_parent_b fp_edge_child_multi", vars=EDGE_45),
        dict(select="fp_edge_child_multi fp_edge_grandchild fp_ledger", vars=EDGE_45),
    ]),
    # B's full refresh replaces fp_edge_parent_a with the same rows: it reads new and blocks without leaving the data
    # out of step with EDGE_45. Its other child rebuilds alongside, or the guard would build it at its next selection
    # on that blocking verdict and a rerun from 46 would fail.
    dict(id=55, desc="two-step: a guarded child skipped in A and rebuilt in B (a parent replaced) records its verdict; "
                     "its child builds", steps=[
        dict(select="fp_edge_parent_a fp_edge_parent_b fp_edge_child_multi fp_edge_grandchild", vars=EDGE_45),
        dict(select="fp_edge_parent_a fp_edge_child_of_eph fp_edge_child_multi fp_edge_grandchild fp_ledger",
             flags=["--full-refresh"], vars=EDGE_45),
    ]),
    # A parent's verdict in another deploy, recorded after a child last built, still reaches the child: the parent is
    # replaced in a deploy that selects none of its guarded children, and reads unchanged in the next one.
    dict(id=56, desc="a parent replaced (new) in a deploy that builds none of its guarded children",
         select="fp_edge_parent_a fp_ledger", flags=["--full-refresh"], vars=EDGE_45),
    dict(id=57, desc="the next deploy builds the children that replace never reached, though the parent reads unchanged",
         select=EDGE, vars=EDGE_45),
    # --- package knobs: the command gate and the vars after it --------------------------------------------
    dict(id=58, desc="knob: initial, create or replace everywhere", select=KNOB, flags=["--full-refresh"],
         vars={**KNOB_VARS, "fp_full_refresh_strategy": "replace"}),
    # The compile fires on-run-start under the build's deploy id; anything it registered would sit pending here.
    dict(id=59, desc="knob: a compile with the fingerprint on registers nothing; the next step registers only its own",
         steps=[
             dict(command="compile", select=KNOB, flags=["--write-catalog"], vars=KNOB_VARS),
             dict(select="fp_knob_parent fp_ledger", vars=KNOB_VARS),
         ]),
    dict(id=60, desc="knob: a hookless table excluded by materialization is never registered; its guarded child builds "
                     "though its other parent is unchanged", select=KNOB, vars=KNOB_VARS),
    # fp_knob_wallclock skipped in 60 though it reads the wall clock; the opt-out comes in by var, so its checksum holds.
    dict(id=61, desc="knob: a guarded child opted out with meta fingerprint_skip false builds though nothing changed",
         select=KNOB, vars={**KNOB_VARS, "fp_knob_wallclock_skip": False}),
    dict(id=62, desc="knob: a new scd version closes an old row; under the default it reads modified and its guarded "
                     "child builds", select=KNOB, vars=KNOB_62),
    # One row per register statement; B's batches for A's nodes insert a reselected row, never a second pending.
    dict(id=63, desc="knob: registration in batches of one row; one pending row per node across two steps",
         steps=[
             dict(select="fp_knob_parent fp_knob_scd2", vars={**KNOB_62, "fingerprint_register_batch_size": 1}),
             dict(select=KNOB, vars={**KNOB_62, "fingerprint_register_batch_size": 1}),
         ]),
    # --- compiled SQL: what moves a guarded child's output without moving its checksum ---------------------
    dict(id=64, desc="sql: initial, create or replace everywhere", select=SQL, flags=["--full-refresh"], vars=STEADY),
    # Also proves the post-hook and the guard hash the same text: a mismatch would build every child here.
    dict(id=65, desc="sql: nothing changed; every guarded child skips on an equal compiled SQL hash", select=SQL,
         vars=STEADY),
    dict(id=66, desc="sql: a macro a guarded child renders changes; its checksum holds, its compiled SQL moves, "
                     "it builds", select=SQL, vars=STEADY, files={SQL_MACRO: "changed"}),
    # The macro variant is named again from here on, so each scenario states the file it expects.
    dict(id=67, desc="sql: a var rendered into a guarded child's SQL changes; it builds", select=SQL, vars=SQL_67,
         files={SQL_MACRO: "changed"}),
    dict(id=68, desc="sql: a 0.0.1 ledger row with no compiled SQL hash as a child's latest build; it builds",
         select=SQL, vars={**SQL_67, "fp_sql_legacy_child": "fp_sql_child"}, files={SQL_MACRO: "changed"}),
    dict(id=69, desc="sql: every change is now behind a recorded hash; every guarded child skips again", select=SQL,
         vars=SQL_67, files={SQL_MACRO: "changed"}),
    # --- faults: statements that fail inside the fingerprint must never fail the node or skip a build ----------
    dict(id=70, desc="fault: initial, create or replace everywhere", select=FAULT, flags=["--full-refresh"],
         vars=STEADY),
    # INFORMATION_SCHEMA takes no table, so registration fails at its first statement; contained, every post-hook and
    # guard then reads a ledger that is not there. The second step, on the real ledger, is the recovery.
    dict(id=71, desc="fault: a ledger schema the role cannot create in, contained; nothing registers, every node builds "
                     "and succeeds, every guard builds; the next step skips as before", steps=[
        dict(select=FAULT_FIXTURES, vars=CONTAINED,
             expect_log=[r"fingerprint: WARNING: " + REGISTER_FAILED,
                         GUARD_FAILED.format("fp_fault_child"), GUARD_FAILED.format("fp_fault_child_of_parked"),
                         *(UNREAD.format(n) for n in FAULT_FIXTURES.split())],
             forbid_log=[NO_SKIP]),
        dict(select=FAULT, vars=STEADY),
    ]),
    # fp_fault_parked's hooks rename the segment table away and back around its fingerprint post-hook.
    dict(id=72, desc="fault: the segment ledger goes missing as a post-hook hashes; that node records error with the "
                     "message, and its guarded child builds", select=FAULT,
         vars={**STEADY, "fp_fault_park_segment": True},
         expect_log=[r"fp_fault_parked => error \(fingerprint query failed \(month hashes\): 2003: "]),
    # A run whose registration wrote nothing (here a command left off fingerprint_commands, the state a contained
    # registration failure leaves) builds a parent the ledger cannot see; its guarded child in that run must build,
    # though its other parent is registered and unchanged.
    dict(id=73, desc="fault: a parent built in a run that registered nothing builds its guarded child in that run",
         steps=[
             dict(select="fp_fault_parent", vars=STEADY),
             dict(command="run", select="fp_fault_parked fp_fault_child_of_parked",
                  vars={**STEADY, "fingerprint_commands": ["build"]},
                  expect_log=[r"fingerprint: run is not in fingerprint_commands; nothing registered",
                              r"fingerprint: fp_fault_child_of_parked has 1 parent\(s\) built in this run with no "
                              r"registration in this deploy; building"],
                  forbid_log=[NO_SKIP]),
             dict(select="fp_ledger", vars=STEADY),
         ]),
    dict(id=74, desc="fault: nothing changed; the segment ledger is back and every guarded child skips", select=FAULT,
         vars=STEADY),
    # Uncontained, the failed registration stops the run, so the parent's change never lands where no later deploy
    # could see it. Had it landed, 76 would find the parent unchanged against its own snapshot and skip the child.
    dict(id=75, desc="fault: registration fails under the default; the run stops before the parent's change builds",
         select="fp_fault_parent", vars={**UNREACHABLE, "fp_upper_email": False}, expect_fail=True,
         expect_log=[REGISTER_FAILED], forbid_log=[BUILT.format("fp_fault_parent")]),
    dict(id=76, desc="fault: the next deploy builds the change, which reads modified, and its guarded child builds",
         select="fp_fault_parent fp_fault_child fp_ledger", vars={**STEADY, "fp_upper_email": False}),
    dict(id=77, desc="fault: back to the steady state; every fault fixture builds on its parent's change", select=FAULT,
         vars=STEADY),
    # --- settle: a registration that recorded nothing keeps blocking ---------------------------------------
    dict(id=78, desc="settle: initial, create or replace everywhere", select=SETTLE, flags=["--full-refresh"],
         vars=STEADY),
    # Step A's unchanged verdict outranks the lost one, so only this run's own record can tell the child.
    dict(id=79, desc="settle: a parent rebuilt with a change in a later step loses its verdict; its guarded child in "
                     "that step builds", steps=[
        dict(select="fp_settle_parent", vars=STEADY),
        dict(select="fp_settle_parent fp_settle_child fp_ledger", vars={**SETTLE_FLIP, LOSE: "fp_settle_parent"},
             expect_log=[NO_VERDICT.format("fp_settle_parent"), UNRECORDED_PARENT], forbid_log=[NO_SKIP]),
    ]),
    dict(id=80, desc="settle: the same loss in a deploy that builds no child", steps=[
        dict(select="fp_settle_parent", vars=SETTLE_FLIP),
        dict(select="fp_settle_parent fp_ledger", vars={**STEADY, LOSE: "fp_settle_parent"},
             expect_log=[NO_VERDICT.format("fp_settle_parent")]),
    ]),
    dict(id=81, desc="settle: the next deploy reads the parent unchanged and builds the child on 80's unsettled "
                     "registration", select="fp_settle_parent fp_settle_child fp_ledger", vars=STEADY,
         expect_log=[UNSETTLED_PARENT], forbid_log=[NO_SKIP]),
    dict(id=82, desc="settle: nothing changed; the child, built since, skips", select=SETTLE, vars=STEADY),
    # The child's own build under other compiled SQL loses its verdict; reverting the var makes its SQL match its last
    # recorded build again, with its checksum equal throughout, so only the unsettled registration can build it.
    dict(id=83, desc="settle: the guarded child builds under a changed var and loses its own verdict",
         select="fp_settle_parent fp_settle_child fp_ledger", vars={**SETTLE_LABEL, LOSE: "fp_settle_child"},
         expect_log=[NO_VERDICT.format("fp_settle_child")]),
    dict(id=84, desc="settle: the var reverted, a new deploy builds the child on 83's unsettled registration",
         select="fp_settle_parent fp_settle_child fp_ledger", vars=STEADY,
         expect_log=[UNSETTLED_SELF], forbid_log=[NO_SKIP]),
    dict(id=85, desc="settle: the same within one deploy; step A's unsettled registration builds the child in step B",
         steps=[
             dict(select="fp_settle_parent fp_settle_child", vars={**SETTLE_LABEL, LOSE: "fp_settle_child"},
                  expect_log=[NO_VERDICT.format("fp_settle_child")]),
             dict(select="fp_settle_parent fp_settle_child fp_ledger", vars=STEADY,
                  expect_log=[UNSETTLED_SELF], forbid_log=[NO_SKIP]),
         ]),
    dict(id=86, desc="settle: nothing changed; the child skips again", select=SETTLE, vars=STEADY),
    # --- mask: a run-start stamp in the compiled SQL, the shape of an audit column -------------------------------
    dict(id=87, desc="mask: initial, create or replace everywhere", select=MASK, flags=["--full-refresh"], vars=STEADY),
    dict(id=88, desc="mask: unmasked, both children's compiled SQL moves with the run start, so both build",
         select=MASK, vars=STEADY),
    # The baseline hash was taken unmasked, so the first masked run still builds the stamped child once.
    dict(id=89, desc="mask: masked for the first time; the stamped child builds once against its unmasked baseline",
         select=MASK, vars=MASKED),
    dict(id=90, desc="mask: masked again; the stamped child skips, the child reading the clock in logic still builds",
         select=MASK, vars=MASKED),
    # --- prune: the EDP trial line, built by a control arm (guard off) and a treatment arm (guard on) ------------------
    # arms=True runs the selection once per arm; stage is the frozen inputs' state, guard=False keeps the treatment's
    # guard off too, and stale names the relations the treatment leaves behind on purpose: rows the control has above
    # the treatment's watermark (behind), and rows at or below it that differ (changed).
    dict(id=91, label="S1 baseline (guard off)", desc="prune: baseline with the guard off in both arms; every object "
                                                     "created or replaced", arms=True, guard=False, stage=0,
         select=PRUNE, flags=FULL_REFRESH,
         vars={**PRUNE_VARS, "force_full_refresh": True, "fp_full_refresh_strategy": "replace"}),
    dict(id=92, label="S2 no-op", desc="prune: a no-op deploy; the replaced event builds its child, every other guarded "
                                       "node with unchanged parents prunes", arms=True, stage=0, select=PRUNE,
         flags=FULL_REFRESH, vars=PRUNE_VARS),
    dict(id=93, label="S3 macro only", desc="prune: the as-of macro is rewritten; the fact builds on its compiled SQL, "
                                            "reads unchanged, and its mart prunes", arms=True, stage=0, select=PRUNE,
         flags=FULL_REFRESH, vars=PRUNE_VARS, files={PRUNE_AS_OF: "changed"}),
    # The macro variant is named again from here on, so each scenario states the file it expects.
    dict(id=94, label="S4 backdated version", desc="prune: a backdated agent version re-resolves an old report; the change "
                                                   "propagates to the marts", arms=True, stage=4, select=PRUNE,
         flags=FULL_REFRESH, vars=PRUNE_VARS, files={PRUNE_AS_OF: "changed"}),
    dict(id=95, label="S5 forward-dated version", desc="prune: a forward-dated version closes a row; the fact rebuilds "
                                                       "unchanged and its mart prunes (early cutoff)", arms=True, stage=5,
         select=PRUNE, flags=FULL_REFRESH, vars=PRUNE_VARS, files={PRUNE_AS_OF: "changed"}),
    dict(id=96, label="S6 daily (no --full-refresh)", desc="prune: the same without --full-refresh; the incremental event "
                                                           "writes nothing, so the intermediate prunes too", arms=True,
         stage=6, select=PRUNE, vars=PRUNE_VARS, files={PRUNE_AS_OF: "changed"}),
    # Appended never blocks: the children of the appended dim and intermediate prune on content that lacks the new rows.
    dict(id=97, label="S7 appended", desc="prune: rows appended to the dim and the intermediate; their children prune and "
                                          "are stale by exactly those rows", arms=True, stage=7, select=PRUNE,
         flags=FULL_REFRESH, vars=PRUNE_VARS, files={PRUNE_AS_OF: "changed"},
         stale={"fp_prune_fct_daily": dict(behind=4), "fp_prune_dm_fct": dict(behind=4),
                "fp_prune_dm_dim_agent": dict(behind=1)}),
    dict(id=98, label="S8 resync (guard off)", desc="prune: the treatment rebuilt with the guard off; the arms are "
                                                    "identical again", arms=True, guard=False, stage=7, select=PRUNE,
         flags=FULL_REFRESH, vars=PRUNE_VARS, files={PRUNE_AS_OF: "changed"}),
    # A late-arriving dim under the left as-of join. The forward-dated version that lands with the report blocks, so the
    # fact builds and carries the report with a null team; the mart behind it prunes on the appended fact.
    dict(id=99, label="late report", desc="prune: a report lands before its agent's first version; a forward-dated "
                                          "version builds the fact, which carries it with a null team", arms=True,
         stage=8, select=PRUNE, flags=FULL_REFRESH, vars=PRUNE_VARS, files={PRUNE_AS_OF: "changed"},
         stale={"fp_prune_dm_fct": dict(behind=1)}),
    # The first version is dated after the report, so only the pre-history fallback resolves it. Appended never blocks:
    # the fact prunes with the report's team still null, a changed row at its watermark rather than a missing one.
    dict(id=100, label="late agent", desc="prune: the agent's first version is appended; the fact prunes and keeps the "
                                          "report's null team", arms=True, stage=9, select=PRUNE, flags=FULL_REFRESH,
         vars=PRUNE_VARS, files={PRUNE_AS_OF: "changed"},
         stale={"fp_prune_fct_daily": dict(changed=1), "fp_prune_dm_fct": dict(behind=1),
                "fp_prune_dm_dim_agent": dict(behind=1)}),
    dict(id=101, label="resync again (guard off)", desc="prune: the treatment rebuilt with the guard off after the late "
                                                        "agent; the arms are identical again", arms=True, guard=False,
         stage=9, select=PRUNE, flags=FULL_REFRESH, vars=PRUNE_VARS, files={PRUNE_AS_OF: "changed"}),
    # Every guarded fixture renders the run-start stamp, so without the mask none of them can prune. Last, because the
    # unmasked hashes it records would build every guarded node once more at the next masked deploy.
    dict(id=102, label="mask off", desc="prune: a no-op deploy without fingerprint_sql_mask_run_started_at; every guarded "
                                        "node builds on its moved compiled SQL", arms=True, stage=9, select=PRUNE,
         flags=FULL_REFRESH, vars=PRUNE_UNMASKED, files={PRUNE_AS_OF: "changed"}),
]


def run(cmd: list[str], *, capture: bool = False) -> subprocess.CompletedProcess:
    print("+", " ".join(cmd), flush=True)
    return subprocess.run(cmd, text=True, capture_output=capture)


def _original_text(here: Path, path: str) -> str:
    """The text to restore: the committed copy when the working tree holds a variant a killed run left behind."""
    text = (here / path).read_text()
    if text not in FILE_VARIANTS[path].values():
        return text
    try:
        committed = subprocess.run(["git", "show", f"HEAD:./{path}"], cwd=here, text=True, capture_output=True,
                                   check=True).stdout
    except (OSError, subprocess.CalledProcessError):
        print(f"[fingerprint] {path} holds a runner variant and git cannot supply the committed copy; left as is",
              flush=True)
        return text
    print(f"[fingerprint] {path} holds a variant a previous run left behind; restoring the committed copy", flush=True)
    return committed


def prune_steps(sc: dict, run_tag: str, schemas: dict[str, str], frozen_stage: int | None) -> list[dict]:
    """A prune scenario's steps: the frozen inputs rebuilt at its stage unless they are there already, one build per
    arm, each its own deploy on its own schema and ledger, asserted against fp_expected_<id>_<arm>, then the content
    oracle under the treatment's vars, which runs even when the treatment's build failed."""
    sid = sc["id"]
    steps = []
    if frozen_stage != sc["stage"]:
        steps.append(dict(select=PRUNE_INPUT, flags=FULL_REFRESH, freeze=sc["stage"],
                          vars={"fingerprint": False, "fp_prune_stage": sc["stage"]}))
    for arm in PRUNE_ARMS:
        arm_vars = {**sc.get("vars", {}), "fp_prune_arm": arm, "fp_prune_stage": sc["stage"],
                    "fingerprint_schema": schemas[arm], "deploy_id": f"fp_{run_tag}_{sid}_{arm}",
                    "fp_iteration": f"{sid}_{arm}",
                    "fingerprint_skip_unchanged_upstream": arm == "treatment" and sc.get("guard", True)}
        steps.append(dict(select=sc["select"], flags=sc.get("flags", []), vars=arm_vars))
    steps.append(dict(command="test", select="fp_prune_arms_agree", after_failure=True,
                      vars={**steps[-1]["vars"], "fp_prune_oracle": True, "fp_prune_stale": sc.get("stale", {})},
                      expect_log=[ORACLE_PASSED], forbid_log=[NOTHING_SELECTED]))
    return steps


def _expected_verdicts(here: Path, sid: int, arm: str) -> dict[str, str]:
    lines = (here / f"seeds/fingerprint/fp_expected_{sid}_{arm}.csv").read_text().splitlines()[1:]
    return dict(line.split(",", 1) for line in lines if line)


def prune_matrix(here: Path) -> str:
    """The treatment arm's outcome for every node and prune scenario, read from the expected seeds the tests assert."""
    scenarios = [s for s in SCENARIOS if s.get("arms")]
    registered = set(PRUNE_NODES) - PRUNE_UNREGISTERED
    expected = {}
    for s in scenarios:
        for arm in PRUNE_ARMS:
            verdicts = _expected_verdicts(here, s["id"], arm)
            if set(verdicts) != registered:
                raise SystemExit(f"[fingerprint] fp_expected_{s['id']}_{arm}.csv does not list exactly the registered "
                                 f"fp_prune nodes: {sorted(set(verdicts) ^ registered)}")
            expected[s["id"], arm] = verdicts
    lines = ["| Node | " + " | ".join(f"{s['id']} {s['label']}" for s in scenarios) + " |",
             "|---" * (len(scenarios) + 1) + "|"]
    for node in PRUNE_NODES:
        cells = []
        for s in scenarios:
            if node in PRUNE_UNREGISTERED:
                cells.append("built, not registered")
                continue
            treatment = expected[s["id"], "treatment"][node]
            if treatment != "skipped":
                cells.append(f"built: {treatment}")
                continue
            stale = s.get("stale", {}).get(node, {})
            lag = [f"{stale[k]} row(s) {k}" for k in ("behind", "changed") if stale.get(k)]
            control = expected[s["id"], "control"][node]
            cells.append(f"**pruned** (control: {control}" + (f"; stale, {', '.join(lag)})" if lag else ")"))
        lines.append(f"| `{node}` | " + " | ".join(cells) + " |")
    return "\n".join(lines)


def sync_matrix(here: Path, write: bool) -> int:
    """Checks, or with write regenerates, the matrix between the markers in README.md."""
    readme = here / "README.md"
    text = readme.read_text()
    start, end = text.find(MATRIX_START), text.find(MATRIX_END)
    if start < 0 or end < start:
        print("[fingerprint] FAIL: README.md has no fp_prune matrix markers")
        return 1
    start += len(MATRIX_START)
    want = "\n" + prune_matrix(here) + "\n"
    if text[start:end] == want:
        print("[fingerprint] the fp_prune matrix in README.md is up to date")
        return 0
    if write:
        readme.write_text(text[:start] + want + text[end:])
        print("[fingerprint] wrote the fp_prune matrix into README.md")
        return 0
    print("[fingerprint] FAIL: the fp_prune matrix in README.md is out of date; regenerate it with --write-matrix")
    return 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--profile", default="default")
    parser.add_argument("--target", default="dev")
    parser.add_argument("--dbt", default="dbt")
    parser.add_argument("--only", help="comma-separated scenario ids")
    parser.add_argument("--from", dest="from_id", type=int, help="first scenario id to run")
    parser.add_argument("--show", action="store_true", help="dump the ledger after every scenario")
    parser.add_argument("--no-seed", action="store_true", help="skip reseeding the fixtures")
    parser.add_argument("--write-matrix", action="store_true",
                        help="regenerate the fp_prune matrix in README.md from the expected seeds, then exit")
    parser.add_argument("--check-matrix", action="store_true",
                        help="exit non-zero when the fp_prune matrix in README.md is out of date")
    args = parser.parse_args()

    here = Path(__file__).resolve().parent
    if args.write_matrix or args.check_matrix:
        return sync_matrix(here, write=args.write_matrix)
    common = ["--profile", args.profile, "--target", args.target, "--project-dir", str(here)]
    run_tag = dt.datetime.now().strftime("%Y%m%d%H%M%S")

    chosen = SCENARIOS
    if args.only:
        wanted = {int(x) for x in args.only.split(",")}
        chosen = [s for s in SCENARIOS if s["id"] in wanted]
    if args.from_id:
        chosen = [s for s in chosen if s["id"] >= args.from_id]
    # The matrix claims to come from the expectations this run asserts; one out of date fails before any dbt runs.
    if any(sc.get("arms") for sc in chosen) and sync_matrix(here, write=False) != 0:
        return 1

    originals ={path: _original_text(here, path) for path in FILE_VARIANTS}

    def restore_files() -> None:
        for path, text in originals.items():
            if (here / path).read_text() != text:
                (here / path).write_text(text)
                print(f"[fingerprint] restored {path}", flush=True)

    # Put back a variant a killed run left behind before the seed load reads it, and again on exit.
    restore_files()
    atexit.register(restore_files)

    if not args.no_seed:
        print(f"[fingerprint] run tag {run_tag}; seeding fixtures")
        if run([args.dbt, "seed", "--select", "path:seeds/fingerprint", "--full-refresh", *common]).returncode != 0:
            print("[fingerprint] FAIL: seeding")
            return 1

    # The prune group's schemas, created with Time Travel on; the arms' names come back as their ledger schemas.
    prune_schemas: dict[str, str] = {}
    if any(sc.get("arms") for sc in chosen):
        proc = run([args.dbt, "run-operation", "fp_prune_setup", *common], capture=True)
        output = (proc.stdout or "") + (proc.stderr or "")
        print(output, end="", flush=True)
        prune_schemas = dict(re.findall(PRUNE_SCHEMA_LINE, output))
        if proc.returncode != 0 or set(prune_schemas) != {"input", *PRUNE_ARMS}:
            print("[fingerprint] FAIL: setting up the fp_prune schemas")
            return 1
    frozen_stage = None

    results: list[tuple[int, str, bool]] = []
    for sc in chosen:
        sid = sc["id"]
        deploy_id = f"fp_{run_tag}_{sid}"
        print(f"\n[fingerprint] ===== {sid}: {sc['desc']} =====", flush=True)

        for path, variant in sc.get("files", {}).items():
            (here / path).write_text(FILE_VARIANTS[path][variant])
            print(f"[fingerprint] wrote {path} variant {variant!r}", flush=True)

        if sc.get("arms"):
            steps = prune_steps(sc, run_tag, prune_schemas, frozen_stage)
        else:
            steps = sc.get("steps") or [dict(select=sc["select"], flags=sc.get("flags", []), vars=sc.get("vars", {}),
                                             expect_fail=sc.get("expect_fail", False),
                                             expect_log=sc.get("expect_log", []),
                                             forbid_log=sc.get("forbid_log", []))]
        ok = True
        last_vars = None
        ran_previous = False
        for step in steps:
            # After a failure only an after_failure step runs, and only when the step before it ran.
            if not ok and not (step.get("after_failure") and ran_previous):
                break
            ran_previous = True
            if "freeze" in step:
                frozen_stage = None
            step_vars = {
                "fp_iteration": sid,
                "fingerprint": True,
                "fingerprint_skip_unchanged_upstream": True,
                "deploy_id": deploy_id,
                # fp_child_late and fp_edge_adopted must never pre-exist, so they get a fresh alias every run.
                "fp_late_alias": f"fp_child_late_{run_tag}",
                "fp_adopted_alias": f"fp_edge_adopted_{run_tag}",
                **step.get("vars", {}),
            }
            last_vars = step_vars
            cmd = [args.dbt, step.get("command", "build"), "--select", *step["select"].split(),
                   "--vars", json.dumps(step_vars), *step.get("flags", []), *common]
            proc = run(cmd, capture=True)
            output = (proc.stdout or "") + (proc.stderr or "")
            print(output, end="", flush=True)
            failed = proc.returncode != 0
            if failed != bool(step.get("expect_fail")):
                print(f"[fingerprint] step {'succeeded but was expected to fail' if not failed else 'failed'}", flush=True)
                ok = False
                continue
            # Log assertions, for outcomes the ledger cannot hold (a guard that built while the ledger was unreachable).
            missing = [p for p in step.get("expect_log", []) if not re.search(p, output)]
            present = [p for p in step.get("forbid_log", []) if re.search(p, output)]
            for p in missing:
                print(f"[fingerprint] step output has no line matching {p!r}", flush=True)
            for p in present:
                print(f"[fingerprint] step output has a forbidden line matching {p!r}", flush=True)
            if missing or present:
                ok = False
                continue
            if "freeze" in step:
                frozen_stage = step["freeze"]

        print(f"[fingerprint] {'PASS' if ok else 'FAIL'} {sid}", flush=True)
        results.append((sid, sc["desc"], ok))

        if (args.show or not ok) and last_vars is not None:
            # fp_ledger is enabled only with the fingerprint var on, and a step may have switched it off.
            run([args.dbt, "show", "--inline",
                 "select node_name, verdict, detail, pre_rows, post_rows, appended_rows, touched_rows, "
                 "touched_old_rows, buckets_hashed, buckets_changed from {{ ref('fp_ledger') }} order by node_name",
                 "--vars", json.dumps({**last_vars, "fingerprint": True}), "--limit", "40", *common])

    print("\n[fingerprint] summary")
    failed = 0
    for sid, desc, ok in results:
        print(f"  {'PASS' if ok else 'FAIL'}  {sid:>2}  {desc}")
        failed += 0 if ok else 1
    print(f"[fingerprint] {len(results) - failed} passed, {failed} failed (run tag {run_tag})")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
