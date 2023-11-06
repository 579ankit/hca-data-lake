--hca_employee_data_landing.hca_expertise_levels
with times as (
(SELECT CAST(CONCAT(CAST(date as STRING) , ' ',time, ':00') as DATETIME) as st, CAST(updated AS DATETIME) as lt FROM hca-usr-hin-landing-datalake.hca_employee_data_landing.hca_expertise_levels)
)

select avg(TIMESTAMP_DIFF(lt,st, SECOND) ) from times;

--hca_employee_data_landing.hca_skills_inventory
with times as (
(SELECT CAST(CONCAT(CAST(date as STRING) , ' ',time, ':00') as DATETIME) as st, CAST(updated AS DATETIME) as lt FROM hca-usr-hin-landing-datalake.hca_employee_data_landing.hca_skills_inventory)
)

select avg(TIMESTAMP_DIFF(lt,st, SECOND) ) from times;


--hca_employee_data_landing.hca_floor_skill_association
with times as (
(SELECT CAST(CONCAT(CAST(date as STRING) , ' ',time, ':00') as DATETIME) as st, CAST(updated AS DATETIME) as lt FROM hca-usr-hin-landing-datalake.hca_employee_data_landing.hca_floor_skill_association)
)

select avg(TIMESTAMP_DIFF(lt,st, SECOND) ) from times;


--hca_employee_data_landing.hca_work_environment
with times as (
(SELECT CAST(CONCAT(CAST(date as STRING) , ' ',time, ':00') as DATETIME) as st, CAST(updated AS DATETIME) as lt FROM hca-usr-hin-landing-datalake.hca_employee_data_landing.hca_work_environment)
)

select avg(TIMESTAMP_DIFF(lt,st, SECOND) ) from times;


--hca_hospital_reports_landing.hca_work_environment
with times as (
(SELECT CAST(CONCAT(CAST(date as STRING) , ' ',time, ':00') as DATETIME) as st, CAST(updated AS DATETIME) as lt FROM hca-usr-hin-landing-datalake.hca_hospital_reports_landing.hca_work_environment)
)

select avg(TIMESTAMP_DIFF(lt,st, SECOND) ) from times;