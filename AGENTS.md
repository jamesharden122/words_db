# Repository Guidelines

## Project Structure & Module Organization
- Source: `src/` (Rust library crate).
  - `src/lib.rs`: entry; DB starters and helpers.
  - `src/finance_data_structs/`: domain models (`crsp.rs`, `world_indices.rs`, `compustat.rs`).
  - `src/schema.rs`: SurrealDB namespaces/tables.
  - `src/error.rs`: unified error type.
- Tests: `tests/` (integration tests: DuckDB + SurrealDB flows).
- Scripts: `scripts/` (R/Python utilities for data prep). Keep reproducible steps here; do not commit raw data.

## Build, Test, and Development Commands
- Build: `cargo build` — compile the crate.
- Test: `cargo test` — run all tests. For heavy I/O: `RAYON_NUM_THREADS=14 POLARS_MAX_THREADS=14 cargo test --release`.
- Lint: `cargo clippy -- -D warnings` — enforce a clean lint baseline.
- Format: `cargo fmt --all` (check: `cargo fmt --all -- --check`).
- Docs: `cargo doc --no-deps` (open locally: `cargo doc --open`).

## Coding Style & Naming Conventions
- Indentation: rustfmt defaults (4 spaces). Run `cargo fmt` before pushing.
- Naming: `snake_case` for functions/modules; `CamelCase` for types; `SCREAMING_SNAKE_CASE` for consts.
- Modules: keep index/returns structs in `src/finance_data_structs/`; re‑export in `lib.rs` only when needed.

## Testing Guidelines
- Framework: Rust built‑in test runner; integration tests under `tests/`.
- Determinism: avoid network and external files in CI; tests needing local Parquet should early‑return if missing (see `tests/duck_global.rs`).
- Naming: prefer descriptive files (e.g., `duck_global.rs`).
- Running: `cargo test -q`; add `-- --nocapture` to view logs.

## Commit & Pull Request Guidelines
- Commits: use Conventional Commits where practical.
  - Examples: `feat: add DuckDB ingest for world indices`, `fix: handle null indexcat in Arrow reader`.
- PRs: include purpose, scope, and linked issues. Add usage notes (commands, paths) and update tests/docs. Keep scope focused; avoid unrelated refactors.

## Security & Configuration Tips
- Secrets: never commit credentials (e.g., WRDS). Use environment variables or ignored local config.
- Data: don’t commit large extracts; reference input paths (e.g., `../data/raw_files/...`) and keep prep scripts in `scripts/`.
- Errors: normalize parsing/IO errors into `AppError`; avoid panics in library code.

