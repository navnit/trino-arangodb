# Static analysis & pre-commit tooling: design

**Status:** design (approved in-session 2026-07-22).
**Goal:** Add an industry-standard, high-signal static-analysis and pre-commit stack to the repo without a disruptive one-time reformat and without touching the deliberately-pinned dependency versions in `pom.xml`.

---

## 1. Context & constraints

- Standalone `pom.xml` (no parent POM), `packaging: trino-plugin`, `maven.compiler.release=24`. Every dependency version is pinned explicitly with a comment explaining a real mediation bug (`CLAUDE.md`). **Do not touch those pins.**
- Today the build has **no** static-analysis plugins (only `maven-surefire-plugin` and `maven-failsafe-plugin`), no git hooks, no `.editorconfig`. CI is a single `mvn verify` job that needs Docker (Testcontainers).
- `CLAUDE.md` records "There is no linter/formatter plugin configured" as a deliberate state; the source and its comments are hand-tuned. A full auto-reformat would produce a large, noisy diff and disturb intentional formatting — so formatting is enforced on a **ratchet** (changed files only), not applied wholesale.
- Tests require Docker; static analysis must **not** require Docker, so it can run as a fast, separate CI job that gives feedback without waiting on containers.

---

## 2. Tool selection (and what's excluded)

High-signal set, each tool with a distinct job and minimal overlap:

| Tool | Job | Gating |
|---|---|---|
| **Spotless** + `google-java-format` | Formatting (whitespace, line length, import order). `ratchetFrom origin/master` — enforces only on files changed vs master, so existing code is untouched. | `spotless:check` fails build; `spotless:apply` auto-fixes. |
| **Checkstyle** (Trino-derived ruleset) | Semantic conventions a formatter can't check: import restrictions (no star/unused/redundant imports), naming, required `@Override`, `equals`/`hashCode` pairing, empty blocks, missing braces, etc. **Pure-formatting checks disabled** (LineLength, indentation, whitespace) so it never fights google-java-format. | `checkstyle:check` fails build on `error` severity. |
| **SpotBugs** + **FindSecBugs** | Bytecode bug patterns + security findings (injection, crypto, etc.). Scans all main code. | `spotbugs:check` fails build on reported bugs; triaged false positives live in `spotbugs-exclude.xml`. |
| **Dependabot** (`.github/dependabot.yml`) | Dependency + GitHub-Actions CVE / update PRs. GitHub-native, zero build cost. | Opens PRs; not a build gate. |
| **.editorconfig** | Cross-editor indent/charset/EOL consistency. | Advisory (editor-enforced). |
| **pre-commit** (`.pre-commit-config.yaml`) | Fast local git hooks: generic hygiene (trailing-whitespace, end-of-file-fixer, check-yaml, check-merge-conflict, check-added-large-files, mixed-line-ending) + `spotless:check` on the commit. Heavy analysis (SpotBugs/Checkstyle) stays in CI, not the commit hook, to keep commits fast. | Blocks the commit locally; opt-in via `pre-commit install`. |

**Excluded, on purpose:**
- **PMD** — overlaps SpotBugs (bugs) and Checkstyle (style); adds noise, not signal.
- **Error Prone** — its Java 24 support lags; wiring it into a `release=24` build risks breaking compilation for little marginal benefit over SpotBugs.

---

## 3. Maven wiring

A new `<pluginManagement>`/`<plugins>` block in `pom.xml`, versions pinned (consistent with the repo's explicit-pin convention), each with a one-line comment on why it's there. Java-24-compatible floors:

- `spotless-maven-plugin` (≥ 2.44) with `googleJavaFormat` (≥ 1.24, which parses Java 24), `ratchetFrom = origin/master`, import-order + `removeUnusedImports` + trailing-whitespace/EOF steps.
- `maven-checkstyle-plugin` (≥ 3.6) driving `checkstyle` core (≥ 10.21, Java-24-aware), config at `config/checkstyle/checkstyle.xml` + `config/checkstyle/suppressions.xml`.
- `spotbugs-maven-plugin` (≥ 4.8) with `findsecbugs-plugin` dependency, `effort=Max`, `threshold=Medium`, `spotbugs-exclude.xml`.

**Binding choice:** the `check` goals bind to the `verify` phase, so a local `mvn verify` catches issues too — the checks are fast and Docker-free, so this adds negligible cost. They are also runnable standalone (`mvn spotless:check`, `mvn checkstyle:check`, `mvn spotbugs:check`), which is exactly what the fast CI job invokes to get feedback without the Docker test phases. No separate profile — one wiring, usable both ways.

---

## 4. CI wiring

Add a second job to `.github/workflows/ci.yml`, running in parallel with the existing `Build and test` job:

```
static-analysis:
  runs-on: ubuntu-latest
  steps: checkout → set up JDK 24 → mvn -B spotless:check checkstyle:check spotbugs:check
```

No Docker, no Testcontainers — finishes in ~1–2 min and gives fast feedback while the container test job runs. Both jobs must pass for the PR check to be green.

---

## 5. Existing-code triage (the real work)

Spotless is ratcheted, so it contributes **zero** findings on unchanged code. But **Checkstyle and SpotBugs scan the existing M1–M3 codebase** and will surface findings on first run. The plan handles this explicitly:

1. Run each tool, capture the full finding list.
2. For each finding: **fix** if it's a genuine issue (a real bug, an unused import, a missing `@Override`); **suppress with a documented reason** in `suppressions.xml` / `spotbugs-exclude.xml` if it's a false positive or an intentional pattern (e.g. the `UnknownType` relocation, the deliberate `catch (Exception ignored)` in `ArangoPageSource.close()`).
3. The gate is only turned on (build fails on findings) **after** the codebase is clean or fully triaged — never landing a red build.

This triage is bounded: it's a read-through of a small codebase's findings, not open-ended refactoring. No behavior changes; fixes are limited to import hygiene, annotations, and genuine bug patterns SpotBugs flags.

---

## 6. Files added / changed

- `pom.xml` — new plugin declarations (no dep-pin changes).
- `config/checkstyle/checkstyle.xml`, `config/checkstyle/suppressions.xml` — Trino-derived, formatting-checks-off ruleset + suppressions.
- `config/spotbugs/spotbugs-exclude.xml` — triaged exclusions.
- `.editorconfig` — root.
- `.pre-commit-config.yaml` — hooks.
- `.github/dependabot.yml` — Maven + Actions ecosystems, weekly.
- `.github/workflows/ci.yml` — new `static-analysis` job.
- `CLAUDE.md` / `README.md` — document the tooling, how to run it, and how to auto-fix (`mvn spotless:apply`), replacing the "no linter/formatter configured" note.
- Targeted source fixes from §5 triage (import hygiene, annotations, genuine SpotBugs findings).

---

## 7. Success criteria

- `mvn spotless:check checkstyle:check spotbugs:check` passes clean on the branch.
- The full existing suite (`mvn verify`) still passes — no behavior change.
- The new CI `static-analysis` job runs Docker-free and gates the PR alongside the test job.
- `pre-commit run --all-files` passes; `pre-commit install` wires the commit hook.
- A deliberately-introduced format violation, unused import, and trivial bug pattern are each caught by the respective tool (proof the gates are live).

---

## 8. Out of scope

- Reformatting existing code (ratchet avoids it).
- PMD, Error Prone (§2).
- JaCoCo / coverage gating — separate concern, not requested.
- Touching the pinned dependency versions or the Surefire/Failsafe workarounds.
