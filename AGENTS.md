# Repository Guidelines

## Project Structure & Module Organization
- Source: `src/` (Rust library crate).
  - `src/lib.rs`: entry; DB starters and helpers.
  - `src/finance_data_structs/`: domain models (`crsp.rs`, `world_indices.rs`, `compustat.rs`).
  - `src/schema.rs`: SurrealDB namespaces/tables.
  - `src/error.rs`: unified error type.
- Tests: `tests/` (integration tests: DuckDB + SurrealDB flows).
- Scripts: `scripts/` (R/Python utilities for data prep).

## Build, Test, and Development Commands
- Build: `cargo build` — compile the crate.
- Test: `cargo test` — run all tests. Use env tuning when heavy I/O: `RAYON_NUM_THREADS=14 POLARS_MAX_THREADS=14 cargo test --release`.
- Lint: `cargo clippy -- -D warnings` — keep a clean lint baseline.
- Format: `cargo fmt --all` (check: `cargo fmt --all -- --check`).
- Docs: `cargo doc --no-deps` (open: `cargo doc --open`).

## Coding Style & Naming Conventions
- Formatting: rustfmt defaults (4‑space indent). Run before pushing.
- Naming: `snake_case` for functions/modules; `CamelCase` for types; `SCREAMING_SNAKE_CASE` for consts.
- Modules: keep index/returns structs in `src/finance_data_structs/`; re‑export in `lib.rs` only when needed.

## Testing Guidelines
- Harness: Rust built‑in test runner; integration tests under `tests/`.
- Determinism: avoid network and external files in CI; tests that require local Parquet should early‑return if the file is missing (see `tests/duck_global.rs`).
- Names: prefer descriptive file names (e.g., `duck_global.rs`).
- Run: `cargo test -q`; use `-- --nocapture` for logs while debugging.

## Commit & Pull Request Guidelines
- Commit style: use Conventional Commits where practical.
  - Examples: `feat: add DuckDB ingest for world indices`, `fix: handle null indexcat in Arrow reader`.
- PRs: include purpose, scope, and linked issues. Add usage notes (commands, paths) and update tests/docs.
- Scope: keep PRs focused; avoid unrelated refactors.

## Security & Configuration Tips
- Secrets: never commit credentials (e.g., WRDS). Use environment variables or local config files ignored by git.
- Data: don’t commit large data extracts; keep reproducible steps in `scripts/` and reference input paths (e.g., `../data/raw_files/...`).
- Errors: normalize parsing/IO errors into `AppError`; avoid panics in library code.

