/* @bruin

name: staging.investments_funded
type: duckdb.sql
materialization:
  type: table

depends:
  - staging.investments_clean
  - staging.companies_clean

description: |
  Apply paper methodology filters:
  1. Keep only investments for cleaned companies
  2. Apply $150,000 funding threshold per company
  3. Exclude companies that only received accelerator funding (endogeneity)
  4. Ensure all remaining companies have at least one investment

columns:
  - name: org_uuid
    type: string
    checks:
      - name: not_null
  - name: investor_uuid
    type: string
    checks:
      - name: not_null

custom_checks:
  - name: has_rows
    description: Funded investments table is not empty
    query: SELECT count(*) > 0 FROM staging.investments_funded
    value: 1

@bruin */

WITH company_total_funding AS (
    -- Total funding per company
    SELECT org_uuid, SUM(total_funding_usd) AS total_funding
    FROM staging.investments_clean
    WHERE org_uuid IN (SELECT uuid FROM staging.companies_clean)
    GROUP BY org_uuid
),
qualified_companies AS (
    -- Companies meeting $150,000 threshold
    SELECT org_uuid
    FROM company_total_funding
    WHERE total_funding >= 150000
),
accelerator_only AS (
    -- Companies where ALL investments are from accelerators/incubators
    SELECT org_uuid
    FROM staging.investments_clean
    WHERE org_uuid IN (SELECT org_uuid FROM qualified_companies)
    GROUP BY org_uuid
    HAVING COUNT(*) = COUNT(
        CASE WHEN LOWER(COALESCE(investor_types, '')) LIKE '%accelerator%'
              OR LOWER(COALESCE(investor_types, '')) LIKE '%incubator%'
        THEN 1 END
    )
)
SELECT i.*
FROM staging.investments_clean i
WHERE i.org_uuid IN (SELECT org_uuid FROM qualified_companies)
  AND i.org_uuid NOT IN (SELECT org_uuid FROM accelerator_only)
  AND i.org_uuid IN (SELECT uuid FROM staging.companies_clean)
