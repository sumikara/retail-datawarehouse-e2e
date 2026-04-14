# General Profiling Interpretation Guide

## Purpose
Translate profiling outputs into actionable modeling and quality decisions.

## What to inspect first
1. Null-heavy columns affecting keys or joins.
2. Cardinality patterns (high/low uniqueness).
3. Temporal ranges and outlier dates.
4. Measure distributions (negative/zero-heavy anomalies).
5. Text domain entropy (normalization candidates).

## Decision outputs
- Confirm grain assumptions.
- Confirm entity boundaries.
- Refine composite key logic.
- Define DQ rules and thresholds.
