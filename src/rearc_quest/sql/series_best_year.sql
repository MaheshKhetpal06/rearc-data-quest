-- Best year per series: the largest sum across quarters. Q05 is an annual average, so it is excluded.
-- Ties go to the latest year. Series with only annual data stay in the result with a NULL best year.
WITH yearly AS (
  SELECT
    series_id,
    year,
    SUM(value) AS summed_value,
    COUNT(*) AS quarters_in_best_year
  FROM transform_bls_data
  WHERE period IN ('Q01', 'Q02', 'Q03', 'Q04')
  GROUP BY series_id, year
),
ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY series_id ORDER BY summed_value DESC, year DESC) AS year_rank
  FROM yearly
)
SELECT
  s.series_id,
  s.series_label,
  r.year AS best_year,
  r.summed_value,
  r.quarters_in_best_year,
  r.year IS NOT NULL AS has_quarterly_data
FROM transform_bls_series AS s
LEFT JOIN ranked AS r
  ON s.series_id = r.series_id AND r.year_rank = 1
