-- Mean and sample standard deviation of annual US population, 2013-2018 inclusive.
SELECT
  MIN(year) AS min_year,
  MAX(year) AS max_year,
  COUNT(*) AS years_included,
  ROUND(AVG(population), 2) AS mean_population,
  ROUND(STDDEV_SAMP(population), 2) AS stddev_population
FROM transform_population
WHERE year BETWEEN 2013 AND 2018
