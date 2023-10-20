#These are the list of dependencies used in employee_data_gen.py 

total_records= 1000
no_of_floors= 5
emp_id_prefix = "EMP"
department_prefix= "DEPT"
job_type=["Full-time", "Part-time", "Contract", "Temporary"]
job_code=["JC-01","JC-02","JC-03","JC-04","JC-05","JC-06"]
job_code_description=["Nurse","Adult Day Caregiver Support","Basic Patient Care","Certified Nurse Midwife","Certified Medical Assistant","Nurse Specialist Acute"]
employment_status_active=["Active", "On Leave"]
employment_status_inactive=["Suspended", "Terminated"]
education_highest_qualification_list=["MD", "MS", "DNB", "D.M.", "M.Ch"]
project_name="hca-usr-hin-datalake-poc"
source_bucket_prefix="test_bucket"
bucket_prefix="hca_employee-data_source_"
employee_data_folder_name="employee_usecase_related"
work_environments_folder_name="employee_usecase_related"
work_environments_file_prefix="hca_work-environments_"
destination_file_name_prefix="hca_employee-data_incremental_daily_"
destination_folder_name="employee_data"

