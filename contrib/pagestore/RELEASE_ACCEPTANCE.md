# Release acceptance

This document describes `.github/workflows/pagestore-release-acceptance.yml`,
the CI workflow that runs the V4 "candidate and delivery validation" gate
(`RELEASE_VALIDATION.md`, `MVP_COMPLETION_PLAN.md` V4/P4) against one frozen
candidate commit, and what its evidence bundle contains.

## When it runs

- Automatically on `push` of a tag matching `pagestore-candidate-*`.
- On demand via `workflow_dispatch` with a `tag` input naming an existing
  `pagestore-candidate-*` tag.

It refuses anything that is not an actual tag in the repository (a
`workflow_dispatch` input that names a branch or a bare SHA is rejected in the
`resolve` job) so a run's result always names one immutable commit, never a
branch that could move under it mid-run.

## Why a separate workflow

`pagestore-test.yml` runs on every push and pull request and, on a release
branch whose PostgreSQL major has no matching fixture yet, WARNs instead of
failing. `pagestore-nightly.yml` soaks a moving branch head on a schedule.
Neither is "the candidate passed acceptance": that claim needs an exact,
reproducible commit and an unconditional requirement that the store and
pgdata fixture checks find a fixture this build actually loads
(`--require-build-match`, always on here, not gated by ref).

## What it runs

- **`standalone`** -- the same no-PostgreSQL daemon/unit-test lane as
  `pagestore-test.yml`'s `standalone-test` job (envelope-only fixture checks;
  no PostgreSQL build to check the payload against).
- **`in-engine`** -- the same PostgreSQL-build lane as `pagestore-test.yml`'s
  `integration-test` job: every harness scenario, the integration test, the
  three WAL-redo demos, the composed MVP golden scenario, the branch-boot
  test, the `pagestore_branch_prepare` Meson test, and the persisted-format
  and pgdata fixture checks with `--require-build-match` unconditional.
- **`soak`** -- the nightly's long configuration (3 seeds x 8000 rounds) at
  the candidate's exact commit.
- **`bundle`** -- collects every job's evidence into `evidence-<tag>.tar.gz`
  and uploads it with 90-day retention (the maximum GitHub Actions allows).

## What the evidence bundle contains

```
evidence-<tag>/
  manifest.txt                                  tag, SHA, workflow run URL, timestamp
  sha256sums.txt                                checksums of every file below
  standalone/
    standalone-test.log                         ./pagestore_test output
    standalone-store-fixture-check.log           envelope-only fixture check
    standalone-pgdata-fixture-check.log           pgdata identity check
  in-engine/
    in-engine-store-fixture-check.log            --require-build-match store check
    in-engine-pgdata-fixture-check.log            --require-build-match pgdata check
    evidence-integration-logs/*.log               integration_test.sh daemon/server logs
    pagestore-control-restore-payload-identity.json   `pagestore_control_restore --payload-identity`
    pagestore-format-versions.json                `pagestore_format_versions`
    git-describe.txt                              `git describe --tags --long`
  soak/
    soak-report-20260909.json
    soak-report-7.json
    soak-report-4242.json
```

Each seed's soak report is also uploaded on its own
(`release-acceptance-soak-<tag>-seed-<seed>`), and the standalone/in-engine
logs on their own (`release-acceptance-standalone-<tag>`,
`release-acceptance-in-engine-<tag>`), so a failing job's evidence is
available even if a later job in the run never reaches `bundle`.

## Relationship to the freeze/promote procedure (P6)

Cutting a `pagestore-candidate-*` tag is what starts this workflow; the P6
freeze/acceptance/publish/promote procedure records this run's result (and
the bundle) in `contrib/pagestore/releases/<tag>.md` alongside the V1-V3
endurance/correctness/recovery evidence gathered separately. This workflow
does not itself decide "restricted developer preview" vs. "production
release" -- see `RELEASE_VALIDATION.md`'s evidence-required-for-sign-off
section for that.
