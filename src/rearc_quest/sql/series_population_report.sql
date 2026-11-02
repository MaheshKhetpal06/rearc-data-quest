-- One series and quarter per year, with population where available. Years without population stay.
SELECT
  d.series_id,
  d.year,
  d.period,
  d.value,
  p.population
FROM transform_bls_data AS d
LEFT JOIN transform_population AS p
  ON d.year = p.year
WHERE d.series_id = 'PRS30006032'
  AND d.period = 'Q01'
