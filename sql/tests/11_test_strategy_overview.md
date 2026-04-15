# Test Strategy Overview

## Test categories
1. **Unit-like SQL tests**: per-procedure expected behavior checks
2. **Data quality tests**: completeness/uniqueness/validity/consistency/timeliness/accuracy
3. **Reconciliation tests**: source vs target counts and measure parity
4. **Security tests**: role grants and RLS visibility behavior
5. **Performance tests**: explain plans on critical joins and fact loads

## Release gate proposal
- Block release on critical DQ/security failures.
- Allow warning-level drift only with documented exception.

## Evidence policy
Each run should preserve:
- SQL executed
- test result status
- row counts and sampled failing keys
- batch/step ids for traceability
