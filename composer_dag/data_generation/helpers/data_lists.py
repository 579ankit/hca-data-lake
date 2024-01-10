import csv, os
from google.cloud import storage
from helpers.help_funs import get_matching_file_path


AIRFLOW_HOME="/home/airflow/gcs"


storage_client=storage.Client()


department_names = ["Internal medicine", "Cardiology", "Neurology", "Urology", "General surgery", "Radiology",
                    "Orthopedics", "Ophthalmology",
                    "Nephrology", "Obstetrics and gynaecology", "Otorhinolaryngology", "Surgery", "Pediatrics",
                    "Pathology", "Dermatology", "Emergency medicine",
                    "Physiotherapy", "Hematology", "Gastroenterology", "Gynecology", "Anesthesiology", "Medicine",
                    "Intensive care medicine", "Psychiatry", "Interventional Radiography Technologist",
                    "Registered Nurse RN PCU PRN", "Registered Nurse RN Float PRN", "MRI Technologist",
                    "CT Technologist", "Medical Laboratory Technician PRN",
                    "Operating Room Tech", "Med Surg Oncology", "Sanitation Assistant",
                    "Registered Nurse Pre-Op Endocrine", "Nurse Manager PCU", "Neurosurgeon",
                    "Orthopedic and Spine Service Line Navigator", "Physical Therapist PRN",
                    "Stroke Program Coordinator", "Registered Nurse Ortho Neuro Trauma",
                    "Director Emergency Services", "Patient Safety Attendant PRN", "Registered Nurse Cardiovascular",
                    "Speech Language Pathologist PRN",
                    "Respiratory Therapist RRT PRN", "Radiographer PRN", "Ultrasound Technologist",
                    "Internal medicine", "Cardiology", "Neurology", "Urology", "General surgery", "Radiology",
                    "Orthopedics", "Ophthalmology",
                    "Nephrology", "Obstetrics and gynaecology", "Otorhinolaryngology", "Surgery", "Pediatrics",
                    "Pathology", "Dermatology", "Emergency medicine",
                    "Physiotherapy", "Hematology", "Gastroenterology", "Gynecology", "Anesthesiology", "Medicine",
                    "Intensive care medicine", "Psychiatry", "Interventional Radiography Technologist",
                    "Registered Nurse RN PCU PRN", "Registered Nurse RN Float PRN", "MRI Technologist",
                    "CT Technologist", "Medical Laboratory Technician PRN",
                    "Operating Room Tech", "Med Surg Oncology", "Sanitation Assistant",
                    "Registered Nurse Pre-Op Endocrine", "Nurse Manager PCU", "Neurosurgeon",
                    "Orthopedic and Spine Service Line Navigator", "Physical Therapist PRN",
                    "Stroke Program Coordinator", "Registered Nurse Ortho Neuro Trauma",
                    "Director Emergency Services", "Patient Safety Attendant PRN", "Registered Nurse Cardiovascular",
                    "Speech Language Pathologist PRN",
                    "Respiratory Therapist RRT PRN", "Radiographer PRN", "Ultrasound Technologist",
                    "Internal medicine", "Cardiology", "Neurology", "Urology", "General surgery", "Radiology",
                    "Orthopedics", "Ophthalmology",
                    "Nephrology", "Obstetrics and gynaecology", "Otorhinolaryngology", "Surgery", "Pediatrics",
                    "Pathology", "Dermatology", "Emergency medicine",
                    "Physiotherapy", "Hematology", "Gastroenterology", "Gynecology", "Anesthesiology", "Medicine",
                    "Intensive care medicine", "Psychiatry", "Interventional Radiography Technologist",
                    "Registered Nurse RN PCU PRN", "Registered Nurse RN Float PRN", "MRI Technologist",
                    "CT Technologist", "Medical Laboratory Technician PRN",
                    "Operating Room Tech", "Med Surg Oncology", "Sanitation Assistant",
                    "Registered Nurse Pre-Op Endocrine", "Nurse Manager PCU", "Neurosurgeon",
                    "Orthopedic and Spine Service Line Navigator", "Physical Therapist PRN",
                    "Stroke Program Coordinator", "Registered Nurse Ortho Neuro Trauma",
                    "Director Emergency Services", "Patient Safety Attendant PRN", "Registered Nurse Cardiovascular",
                    "Speech Language Pathologist PRN",
                    "Respiratory Therapist RRT PRN", "Radiographer PRN", "Ultrasound Technologist"
                    ]


hospitals=["Cleveland Clinic", "Mayo Clinic - Rochester", "Massachusetts General Hospital", "The Mount Sinai Hospital", "Northwestern Memorial Hospital",
           "Ronald Reagan UCLA Medical Center", "UCSF Medical Center", "johns hopkins hospital", "barnes jewish hospital", "RUSH University Medical Center"]
hospitals_addresses={"Cleveland Clinic": "11100 Euclid Ave, Cleveland, OH 44106",
                   "Mayo Clinic - Rochester": "200 1st St SW, Rochester, MN 55905",
                   "Massachusetts General Hospital": "55 Fruit St, Boston, MA 02114",
                   "The Mount Sinai Hospital": "1468 Madison Ave, New York, NY 10029",
                   "Northwestern Memorial Hospital": "251 E Huron St, Chicago, IL 60611",
                   "Ronald Reagan UCLA Medical Center": "757 Westwood Plaza, Los Angeles, CA 90095",
                   "UCSF Medical Center": "505 Parnassus Ave, San Francisco, CA 94143",
                   "johns hopkins hospital": "1800 Orleans St, Baltimore, MD 21287",
                   "barnes jewish hospital": "One Barnes Jewish Hospital Plaza, St. Louis, MO 63110",
                   "RUSH University Medical Center": "1620 W Harrison St, Chicago, IL 60612"}


hospital_job_types = ["Full-time", "Part-time", "Contract", "Temporary"]
hospital_job_titles = ["Nurse", "Doctor", "Surgeon", "Pharmacist", "Radiologist", "Physical Therapist",
                        "Lab Technician", "Administrative Assistant", "Medical Coder", "Health Educator"]
employment_statuses = ["Active", "Suspended", "Terminated", "On Leave"]




majors=["MD", "MS", "DNB", "D.M.", "M.Ch"]


Job_Class_Code_and_Desc={}
def get_job_class_code_and_desc_list():
    count=0
    with open(f"{AIRFLOW_HOME}/data/csvs/job_class_code_and_desc.csv", 'r') as file:
    # with open(f"./helpers/job_class_code_and_desc.csv", 'r') as file:
        csvreader = csv.reader(file)
        for row in csvreader:
            if count>50:
                break
            if count>0:
                Job_Class_Code_and_Desc[row[1]]=row[0]
            # print(type(row), row)
            count+=1


get_job_class_code_and_desc_list()
# print(Job_Class_Code)
# print(Job_Class_Desc)


# test_names=["HDL", "LDL", "Triiodothyronine (T3)", "Thyroxine (T4)", "Thyroid-stimulating hormone (TSH)",
#             "C-reactive protein (CRP)", "SGPT", "SGOT", "Alkaline Phosphatase (ALP)", "Serum Albumin", "Serum Globulin", "CT Scan"]


# test_details={"HDL":["Lipid Profile", "mg/dL"],
#               "LDL":["Lipid Profile", "mg/dL"],
#               "Triiodothyronine (T3)":["Thyroid Profile", "ng/dL"],
#               "Thyroxine (T4)":["Thyroid Profile", "ng/dL"],
#               "Thyroid-stimulating hormone (TSH)":["Thyroid Profile", "mIU/L"],
#               "C-reactive protein (CRP)":["CRP", "mg/dL"],
#               "SGPT":["LFT", "U/L"],
#               "SGOT":["LFT", "U/L"],
#               "Alkaline Phosphatase (ALP)":["LFT", "U/L"],
#               "Serum Albumin":["LFT", "g/dL"],
#               "Serum Globulin":["LFT", "gm/dL"],
#               "CT Scan":["CT Scan", "HU"]}




# test_values={"HDL":[35, 150], "LDL":[80, 250], "Triiodothyronine (T3)":[60, 250], "Thyroxine (T4)":[0.5, 2.5],
#              "Thyroid-stimulating hormone (TSH)":[0.5, 7.0], "C-reactive protein (CRP)":[0.5, 20.0],
#              "SGPT":[3, 200], "SGOT":[3, 200], "Alkaline Phosphatase (ALP)":[3, 200], "Serum Albumin":[2.0, 6.0], "Serum Globulin":[2.0, 6.0], "CT Scan":[1, 25]}


def get_lab_test_details():
    # # Get latest hospital_data file
    # file_path, bucket_, matching_file=get_matching_file_path("hca-usr-hin-datalake-poc", "hca_hospital-reports_source_20231005", "test_specs", "hca_test_specs_full_")
    # bucket=storage_client.bucket(bucket_)
    # blob=bucket.blob(matching_file)
    # blob.download_to_filename("./test_specs_temp.csv")
    test_ids=[]
    test_details={}
    with open(f"{AIRFLOW_HOME}/data/csvs/test_specs.csv", 'r') as file:
    # with open("./test_specs_temp.csv", 'r') as file:
        csvreader = csv.reader(file)
        for row in csvreader:
            details=[]
            # print(type(row[0]), row[0])
            if row[0]!='test_id':
                test_ids.append(row[0])
                details.append(row[1])
                details.append(row[2])
                # x=row[3]
                # lst=x.split()
                # details.append(lst[0])
                # details.append(lst[2])
                details.append(row[3])
                details.append(row[4])
                details.append(row[5])
                details.append(row[6])
                details.append(row[7])
                details.append(row[8])
                details.append(row[9])
                details.append(row[10])
                details.append(row[11])
                details.append(row[12])
                test_details[row[0]]=details
    return test_ids, test_details
test_ids, test_details=get_lab_test_details()






def get_hospital_details():
    # # Get latest hospital_data file
    # file_path, bucket_, matching_file=get_matching_file_path("hca-usr-hin-datalake-poc", "hca_hospital-reports_source_20231005", "hospital_data", "hca_hospital_data_full_")
    # bucket=storage_client.bucket(bucket_)
    # blob=bucket.blob(matching_file)
    # blob.download_to_filename("./hospital_data_temp.csv")
    hospital_ids=[]
    hospital_details={}
    with open(f"{AIRFLOW_HOME}/data/csvs/hospital_data.csv", 'r') as file:
    # with open("./hospital_data_temp.csv", 'r') as file:
    # with open(f"./helpers/hospital_data.csv", 'r') as file:
        csvreader = csv.reader(file)
        for row in csvreader:
            details=[]
            # print(type(row[0]), row[0])
            if row[0]!='hospital_id':
                hospital_ids.append(row[0])
                details.append(row[1])
                details.append(row[2])
                # x=row[3]
                # lst=x.split()
                # details.append(lst[0])
                # details.append(lst[2])
                details.append(row[3])
                details.append(row[4])
                hospital_details[row[0]]=details
    return hospital_ids, hospital_details
hospital_ids, hospital_details=get_hospital_details()




def get_test_units_and_ids_set():
    # # Get latest hospital_data file
    # file_path, bucket_, matching_file=get_matching_file_path("hca-usr-hin-datalake-poc", "hca_hospital-reports_source_20231005", "test_specs", "hca_test_specs_full_")
    # bucket=storage_client.bucket(bucket_)
    # blob=bucket.blob(matching_file)
    # blob.download_to_filename("./test_specs_temp.csv")
    test_ids_set=set()
    test_units={}
    with open(f"{AIRFLOW_HOME}/data/csvs/test_specs.csv", 'r') as file:
    # with open("./test_specs_temp.csv", 'r') as file:
        csvreader = csv.reader(file)
        for row in csvreader:
            if row[0]!="test_id":
                test_id=row[0]
                source_unit_of_measurement=row[1]
                standard_unit_of_measurement=row[7]
                conversion_factor=row[8]
                if row[0] not in test_ids_set:
                    test_ids_set.add(test_id)
                    test_units[test_id]={}
                    test_units[test_id][source_unit_of_measurement]=conversion_factor
                else:
                    test_units[test_id][source_unit_of_measurement]=conversion_factor
    return test_ids_set, test_units
test_ids_set, test_units=get_test_units_and_ids_set()



