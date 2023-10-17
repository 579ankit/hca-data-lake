from google.cloud import bigquery
from google.cloud import datacatalog_v1beta1
from policy_tags import list_taxonomies,list_policy_tags,get_taxonomy_id,get_policy_tag_id


def list_bq_table_columns(project_id, dataset_name, table_name):
    client = bigquery.Client()
    table_id = f"{project_id}.{dataset_name}.{table_name}"

    table = client.get_table(table_id)
    columns = [col.name for col in table.schema]

    print("The list of columns in that table are: ")
    for number, column in enumerate(columns):
        print(f"{number+1}.{column}")



def apply_policy_tags_to_bq_table(project_id, dataset_name, table_name, columns_policy_tags, taxonomy_id):
    # Create a client object
    client = bigquery.Client()

    # Get the table object
    table_id = f"{project_id}.{dataset_name}.{table_name}"
    table = client.get_table(table_id)

    # Create a new schema list
    new_schema = []

    # Iterate over the original schema
    for column in table.schema:
        if column.name in columns_policy_tags:
            policy_tag = columns_policy_tags[column.name]
            policy_tag_id = get_policy_tag_id(policy_tag,taxonomy_id)
            if policy_tag_id:
                new_column = bigquery.SchemaField(
                    name=column.name,
                    field_type=column.field_type,
                    mode=column.mode,
                    description=column.description,
                    # Specify the policy tag name
                    policy_tags=bigquery.PolicyTagList(
                        [f"{taxonomy_id}/policyTags/{policy_tag_id}"]
                    )
                )
                # Append the new column to the new schema list
                new_schema.append(new_column)
            else:
                new_schema.append(column)
                
        else:
            new_schema.append(column)

    table.schema = new_schema
    client.update_table(table, ["schema"])
    print("Table Schema Updated Successfully...")




if __name__ == "__main__":
    project_id = input("Enter Project Id: ")
    location = input("Enter Location: ")
    dataset_name = input("Enter Dataset name: ")
    table_name = input("Enter table name: ")

    apply_columns = ["last_name","date_of_birth","gender","address","phone_number","email_address","emergency_contact","test_results"]
    list_taxonomies(project_id,location)
    taxonomy_name = input("Enter taxonomy name from above where policy tags exists: ")
    taxonomy_id = get_taxonomy_id(project_id,location,taxonomy_name)

    if taxonomy_id:
        apply_policy_tags = ["name","dob","gender","address","phone","email","phone","test_results"]
        columns_policy_tags = dict(zip(apply_columns, apply_policy_tags))
        apply_policy_tags_to_bq_table(project_id, dataset_name, table_name, columns_policy_tags, taxonomy_id)
    else:
        print("Policy Taxonomy not found")

