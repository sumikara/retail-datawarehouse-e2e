# Python Normalization Alternative (Optional Pre-Staging)

## Why consider it
When SQL-only environments are constrained or source quality is very inconsistent, a Python pre-standardization step can reduce SQL complexity.

## Suggested responsibilities for Python
- Unicode normalization, robust text cleaning, regex-heavy parsing
- Date parsing with strict fallback handling
- Controlled dictionaries for categorical normalization
- Optional hash/signature generation before DB load

## Boundary rule
Python should not replace warehouse integration logic. It should only pre-clean source payloads, while canonical keying and entity integration remain in SQL layers.

## Minimal artifact expectations
- Reproducible notebook/script
- Versioned transformation rules
- Before/after quality metrics report
