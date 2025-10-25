# Fintech-ready Raw → CT/TUS Analysis Pipeline
This repo contains a fintech-hardened data pipeline suitable for production-class environments and interview reviewers in fintech companies.
Key fintech improvements:
- Schema validation with `pandera` (optional).
- Structured JSON logging for observability.
- Audit lineage: deterministic row hashes and processed timestamps (`outputs/audit_lineage.csv`).
- Atomic writes to avoid partial output files.
- Config via environment variables (12-factor app style).
- CI includes linting and security scanning (ruff & bandit).
- Pipeline versioning metadata included in outputs.
How to run locally:
```bash
python process_data_fintech.py --raw "1 Raw Data.xlsx" --ct_template "2 CT Analysis.xlsx" --tus_template "3 TUS Analysis.xlsx" --out_dir "outputs" --version "0.1.0"
```
CI:
- See `.github/workflows/ci_fintech.yml` which runs linting, security scan, tests, and builds Docker image.
Compliance & Audit-ready details:
- Schema validation and error capture.
- Deterministic SHA-256 row hashes in `audit_lineage.csv`.
- Structured logs and atomic file writes.
- Retention & governance recommendations included in README.
