/* @bruin

name: core.investment_pairs
type: duckdb.sql
materialization:
  type: table

depends:
  - core.vc_investments

description: |
  Create investment pairs: late-stage (left/set 0) <=> early-stage (right/set 1).
  This forms the edge list for the bipartite syndication network.
  
  Early stages: angel, pre_seed, seed, series_a
  Late stages: series_b, series_c, series_d, series_e, series_f, series_g, series_h, series_i
  
  Filters:
  - Remove self-pairs (same investor on both sides)
  - Remove pairs where investor names share first 5 chars (likely related entities)
  - Deduplicate: each (company, investor_pair) appears only once

columns:
  - name: investor_name_left
    type: string
    checks:
      - name: not_null
  - name: investor_name_right
    type: string
    checks:
      - name: not_null

custom_checks:
  - name: has_pairs
    description: Investment pairs table is not empty
    query: SELECT count(*) > 0 FROM core.investment_pairs
    value: 1

@bruin */

WITH early_stages AS (
    SELECT * FROM core.vc_investments
    WHERE investment_type IN ('angel', 'pre_seed', 'seed', 'series_a')
),
late_stages AS (
    SELECT * FROM core.vc_investments
    WHERE investment_type IN ('series_b', 'series_c', 'series_d', 'series_e',
                              'series_f', 'series_g', 'series_h', 'series_i')
),
paired AS (
    SELECT
        l.investor_node_name  AS investor_name_left,
        r.investor_node_name  AS investor_name_right,
        l.announced_year      AS announced_year_left,
        r.announced_year      AS announced_year_right,
        l.org_uuid,
        l.total_funding_usd   AS total_funding_usd_left,
        r.total_funding_usd   AS total_funding_usd_right,
        GREATEST(l.announced_year, r.announced_year) AS year,
        l.company_country     AS company_country,
        l.investor_country    AS investor_country_left,
        r.investor_country    AS investor_country_right,
        l.investor_region     AS investor_region_left,
        r.investor_region     AS investor_region_right,
        l.investment_type     AS investment_type_left,
        r.investment_type     AS investment_type_right,
        l.category            AS category_left,
        r.category            AS category_right
    FROM late_stages l
    JOIN early_stages r
      ON l.org_uuid = r.org_uuid
    WHERE l.investor_name != r.investor_name
),
deduplicated AS (
    SELECT *,
        -- Create a canonical pair key for dedup
        CASE WHEN investor_name_left < investor_name_right
             THEN investor_name_left || '||' || investor_name_right
             ELSE investor_name_right || '||' || investor_name_left
        END AS investor_pair_key,
        org_uuid || '||' ||
        CASE WHEN investor_name_left < investor_name_right
             THEN investor_name_left || '||' || investor_name_right
             ELSE investor_name_right || '||' || investor_name_left
        END AS dedup_key
    FROM paired
    WHERE LEFT(investor_name_left, 5) != LEFT(investor_name_right, 5)
)
SELECT
    investor_name_left,
    investor_name_right,
    announced_year_left,
    announced_year_right,
    org_uuid,
    total_funding_usd_left,
    total_funding_usd_right,
    year,
    company_country,
    investor_country_left,
    investor_country_right,
    investor_region_left,
    investor_region_right,
    investment_type_left,
    investment_type_right,
    category_left,
    category_right
FROM deduplicated
QUALIFY ROW_NUMBER() OVER (PARTITION BY dedup_key ORDER BY year) = 1
