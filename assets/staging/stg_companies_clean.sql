/* @bruin

name: staging.companies_clean
type: duckdb.sql
materialization:
  type: table

depends:
  - raw.companies

description: |
  Clean companies data following Dalle et al. paper methodology:
  1. Exclude companies with missing essential info (uuid, name, founded_year)
  2. Exclude companies founded after 2017
  3. Exclude companies with exit status (closed, acquired, ipo)

columns:
  - name: uuid
    type: string
    checks:
      - name: not_null
      - name: unique
  - name: name
    type: string
    checks:
      - name: not_null

@bruin */

SELECT *
FROM raw.companies
WHERE uuid IS NOT NULL
  AND name IS NOT NULL
  AND founded_year IS NOT NULL
  AND founded_year <= 2017
  AND (status IS NULL OR status NOT IN ('closed', 'acquired', 'ipo'))
