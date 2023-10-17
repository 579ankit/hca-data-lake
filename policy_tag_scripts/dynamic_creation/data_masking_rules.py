from google.cloud import bigquery_datapolicies_v1
import time

# Lists all default and custom Data Masking Rules
def list_masking_rules(project_id,location):

    client = bigquery_datapolicies_v1.DataPolicyServiceClient()

    request = bigquery_datapolicies_v1.ListDataPoliciesRequest(
        parent=f"projects/{project_id}/locations/{location}",
    )

    page_result = client.list_data_policies(request=request)

    print("Available Data Masking Rules are:")

    for response in page_result:
        print(response.data_masking_policy)



# Adds given Data Masking Rule for given Policy tags
def add_masking_rules_to_policy_tags(project_id, location, policy_tag_name, policy_tag_id, masking_rule):
    
    client = bigquery_datapolicies_v1.DataPolicyServiceClient()

    if "routines" in masking_rule:
        # Create a data policy that uses a custom masking rule.
        data_policy = bigquery_datapolicies_v1.DataPolicy(
            policy_tag=policy_tag_id,
            # principal=principal,
            data_masking_policy=bigquery_datapolicies_v1.DataMaskingPolicy(
                routine = masking_rule 
            ),
            data_policy_id=policy_tag_name+"_"+str(int(time.time())),
            data_policy_type=bigquery_datapolicies_v1.DataPolicy.DataPolicyType.DATA_MASKING_POLICY,
            # name=f"projects/hca-data-lake-poc/locations/us-central1/dataPolicies/{policy_tag_name+str(time.time())}"
        )

    else:
        # Create a data policy that uses a predefined masking rule.
        data_policy = bigquery_datapolicies_v1.DataPolicy(
            policy_tag=policy_tag_id,
            # principal=principal,
            data_masking_policy=bigquery_datapolicies_v1.DataMaskingPolicy(
            predefined_expression=masking_rule
            ),
            data_policy_id=policy_tag_name+"_"+str(int(time.time())),
            data_policy_type=bigquery_datapolicies_v1.DataPolicy.DataPolicyType.DATA_MASKING_POLICY,
            # name=f"projects/hca-data-lake-poc/locations/us-central1/dataPolicies/{policy_tag_name+str(time.time())}"
        )


    response = client.create_data_policy(parent=f"projects/{project_id}/locations/{location}", data_policy=data_policy)

    print(f"Created data policy: {response.name}")











# list_masking_rules()