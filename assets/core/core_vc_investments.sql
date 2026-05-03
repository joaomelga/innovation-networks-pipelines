/* @bruin

name: core.vc_investments
type: duckdb.sql
materialization:
  type: table

depends:
  - staging.investments_funded

description: |
  Extract venture capital investments (investor_types contains 'venture').
  investor_node_name is an intermediate field carrying the round-type suffix
  (investor_name + '-' + investment_type). Downstream, core.investment_pairs
  overrides this with a binary stage label ('-late' / '-early') so each investor
  appears at most once per bipartite side.

columns:
  - name: investor_node_name
    type: string
    checks:
      - name: not_null

custom_checks:
  - name: has_vc_investments
    description: VC investments table is not empty
    query: SELECT count(*) > 0 FROM core.vc_investments
    value: 1

@bruin */

SELECT
    *,
    investor_name || '-' || investment_type AS investor_node_name
FROM staging.investments_funded
WHERE LOWER(COALESCE(investor_types, '')) LIKE '%venture%'
