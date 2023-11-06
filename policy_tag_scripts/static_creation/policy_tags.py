from google.cloud import datacatalog_v1
from data_masking_rules import list_masking_rules
from data_masking_rules import add_masking_rules_to_policy_tags


def get_taxonomy_id(location,taxonomy_name):

    client = datacatalog_v1.PolicyTagManagerClient()
    parent = f"projects/hca-usr-hin-proc-datalake/locations/{location}"
    taxonomies = client.list_taxonomies(request={"parent": parent})

    for taxonomy in taxonomies:
        if taxonomy.display_name == taxonomy_name:
            print("taxonomy id: ",taxonomy.name)
            return taxonomy.name
    return 0


def list_taxonomies(location):

    client = datacatalog_v1.PolicyTagManagerClient()
    parent = f"projects/hca-usr-hin-proc-datalake/locations/{location}"
    taxonomies = client.list_taxonomies(request={"parent": parent})

    print("Available Taxonomy List:  ")
    for taxonomy in taxonomies:
        print(taxonomy.display_name)


def get_policy_tag_id(policy_tag_name,taxonomy_id):
    client = datacatalog_v1.PolicyTagManagerClient()

    taxonomy_name = taxonomy_id

    policy_tags = client.list_policy_tags(parent=taxonomy_name)
    for policy_tag in policy_tags:
        if policy_tag.display_name == policy_tag_name:
            policy_tag_id = policy_tag.name.split("/")[-1]
            return policy_tag_id
    else:
        print(f"No policy tag found with {policy_tag_name}")
        return 0



def list_policy_tags(taxonomy_id):

    client = datacatalog_v1.PolicyTagManagerClient()
    parent = taxonomy_id
    policy_tags = client.list_policy_tags(request={"parent": parent})

    print("Available Policy Tags List:  ")
    for policy_tag in policy_tags:
        print(policy_tag.display_name)



def create_taxonomy(taxonomy_name, project_id, location):

    client = datacatalog_v1.PolicyTagManagerClient()
    parent = datacatalog_v1.PolicyTagManagerClient.common_location_path(
        project_id, location
    )

    taxonomy = datacatalog_v1.Taxonomy()
    taxonomy.display_name = taxonomy_name
    taxonomy.description = "A policy taxonomy for Sensitive Data"

    taxonomy = client.create_taxonomy(parent=parent, taxonomy=taxonomy)
    print(f"Created taxonomy {taxonomy.name}")
    return taxonomy.name





def create_policy_tags(taxonomy_id,project_id, location, policy_tags: list = None):

    client = datacatalog_v1.PolicyTagManagerClient()
    
    for policy_tag_data in policy_tags:
        # print("policy tag name >>>>>>>>>",policy_tag_data.get('name'))
        policy_tag = datacatalog_v1.PolicyTag()
        policy_tag.display_name = policy_tag_data.get('name')
        policy_tag.description = policy_tag_data.get('description')
        policy_tag = client.create_policy_tag(parent=taxonomy_id, policy_tag=policy_tag)
        print(f"Created policy tag {policy_tag.name}")

        # add masking rules calling
        add_masking_rules_to_policy_tags(project_id,location,policy_tag_data.get('name'),policy_tag.name,policy_tag_data.get('masking_rule'))




if __name__ == "__main__":
    
    project_id = input("Enter Project Id: ")
    location = input("Enter Location: ")
    taxonomy_name = input("Enter Taxonomy name to create: ")

    policy_tags = [
        {
            "name": "email",
            "description": "",
            "masking_rule": f"projects/{project_id}/datasets/test_custom_data_masking_rules/routines/emailMask"
        },
        {
            "name": "phone",
            "description": "",
            "masking_rule": "FIRST_FOUR_CHARACTERS"
        },
        {
            "name": "number",
            "description": "",
            "masking_rule": "FIRST_FOUR_CHARACTERS"
        },
        {
            "name": "name",
            "description": "",
            "masking_rule": f"projects/{project_id}/datasets/test_custom_data_masking_rules/routines/Alphanumeric_Mask"
        },
        {
            "name": "date",
            "description": "",
            "masking_rule": "DATE_YEAR_MASK"
        },
        {
            "name": "address",
            "description": "",
            "masking_rule": f"projects/{project_id}/datasets/test_custom_data_masking_rules/routines/Alphanumeric_Mask"
        },
        {
            "name": "results",
            "description": "",
            "masking_rule": "DEFAULT_MASKING_VALUE"
        },
        {
            "name": "alphanumeric",
            "description": "",
            "masking_rule": f"projects/{project_id}/datasets/test_custom_data_masking_rules/routines/Alphanumeric_Mask"
        },
        {
            "name": "hash_value",
            "description": "",
            "masking_rule": "SHA256"
        }    

    ]
    # print(policy_tags)

    taxonomy_id = create_taxonomy(taxonomy_name,project_id,location)
    # print(taxonomy_id)

    create_policy_tags(taxonomy_id,project_id,location, policy_tags=policy_tags)