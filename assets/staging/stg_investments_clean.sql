/* @bruin

name: staging.investments_clean
type: duckdb.sql
materialization:
  type: table

depends:
  - raw.investments

description: |
  Clean investments data:
  1. Remove investments missing org_uuid or investor_uuid
  2. Remove investments with invalid (negative or zero) funding amounts

columns:
  - name: org_uuid
    type: string
    checks:
      - name: not_null
  - name: investor_uuid
    type: string
    checks:
      - name: not_null

@bruin */

SELECT *
FROM raw.investments
WHERE org_uuid IS NOT NULL
  AND investor_uuid IS NOT NULL
  AND total_funding_usd > 0
