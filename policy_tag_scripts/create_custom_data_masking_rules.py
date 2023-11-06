from google.cloud import bigquery


def create_custom_masking_rules(project_id,dataset,routines):
    client = bigquery.Client()

    for routine in routines:
        sql = routine.get("query")

        query_job = client.query(sql)
        query_job.result() 
    print("Data masking routines created.")


if __name__ == "__main__":
    project_id = input("Enter Project Id: ")
    dataset = input("Enter Dataset Name: ")

    routines = [
        {
            "query": f"""CREATE OR REPLACE FUNCTION `{project_id}.{dataset}.Alphanumeric_Mask`(input_string STRING) RETURNS STRING
                OPTIONS (data_governance_type="DATA_MASKING") AS (
                SAFE.REGEXP_REPLACE(input_string, '[a-zA-Z0-9]', 'X')
                );"""
        },
        {
            "query": f"""CREATE OR REPLACE FUNCTION `{project_id}.{dataset}.emailMask`(email STRING) RETURNS STRING
                OPTIONS (data_governance_type="DATA_MASKING") AS (
                SAFE.REGEXP_REPLACE(email, '([A-Za-z0-9._%+-]+)@([A-Za-z0-9.-]+//.[A-Za-z]{2,4})', '*****@//2')
                );
                """   
        }
    ]

    create_custom_masking_rules(project_id,dataset,routines)
