CREATE EXTERNAL TABLE `hca-usr-hin-landing-datalake.hca_employee_data_landing.hca_expertise_levels_metadata`
WITH CONNECTION `hca-usr-hin-landing-datalake.us-central1.employee_data_biglake_connection`
OPTIONS(
  object_metadata = 'SIMPLE',
  uris=['gs://hca_employee-data_landing_20231005/expertise_levels/*.csv']
);


WITH times AS (
    SELECT 
        CAST(DATETIME(
          CONCAT(
                REGEXP_EXTRACT(uri, r'date=(\d{4}-\d{2}-\d{2})'), ' ',
                REGEXP_EXTRACT(uri, r'time=(\d{2}:\d{2})'),  ':00'
         ) ) AS DATETIME
        ) AS st,
        CAST(updated AS DATETIME) as lt
    FROM hca-usr-hin-landing-datalake.hca_employee_data_landing.hca_expertise_levels_metadata
)
SELECT AVG(TIMESTAMP_DIFF(lt, st, SECOND)) FROM times;
