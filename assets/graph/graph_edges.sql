/* @bruin

name: graph.edges
type: duckdb.sql
materialization:
  type: table

depends:
  - core.investment_pairs
  - graph.network

description: |
  Build the canonical edges table: each row is an investment pair (Source ↔ Target)
  with community assignment, duplicated once per clustering method stored in graph.network.
  Only keeps edges where both investors belong to the same community.

columns:
  - name: clustering_method
    type: string
    checks:
      - name: not_null
  - name: Source
    type: string
    checks:
      - name: not_null
  - name: Target
    type: string
    checks:
      - name: not_null
  - name: community
    type: integer

custom_checks:
  - name: has_edges
    description: Edges table is not empty
    query: SELECT count(*) > 0 FROM graph.edges
    value: 1

@bruin */

WITH methods AS (
    SELECT DISTINCT clustering_method FROM graph.network
),
edges_with_community AS (
    SELECT
        m.clustering_method,
        p.investor_name_left,
        p.investor_name_right,
        p.announced_year_left,
        p.announced_year_right,
        p.org_uuid,
        p.total_funding_usd_left,
        p.total_funding_usd_right,
        p.year,
        p.company_country,
        p.investor_country_left,
        p.investor_country_right,
        p.investor_region_left,
        p.investor_region_right,
        p.investment_type_left,
        p.investment_type_right,
        p.category_left,
        p.category_right,
        nl.community_id AS community_left,
        nr.community_id AS community_right
    FROM core.investment_pairs p
    CROSS JOIN methods m
    LEFT JOIN graph.network nl
        ON nl.node = p.investor_name_left
        AND nl.clustering_method = m.clustering_method
    LEFT JOIN graph.network nr
        ON nr.node = p.investor_name_right
        AND nr.clustering_method = m.clustering_method
)
SELECT
    clustering_method,
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
    category_right,
    community_left,
    community_right,
    CASE
        WHEN community_left = community_right AND community_left IS NOT NULL
        THEN community_left
        ELSE -1
    END AS community,
    investor_name_left  AS "Source",
    investor_name_right AS "Target"
FROM edges_with_community
