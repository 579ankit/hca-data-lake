from google.cloud import datacatalog_v1
from google.cloud import bigquery

# Set your Google Cloud project and BigQuery dataset/table information.
project_id = "hca-data-lake-poc"
dataset_id = "auto_test_lab_reports"
table_id = "patient_data"
taxonomy_name = "3328170092391861664"  # Replace with the taxonomy name

# Create a Data Catalog client.
datacatalog_client = datacatalog_v1.PolicyTagManagerClient()

# Create a BigQuery client.
bigquery_client = bigquery.Client(project=project_id)

# Define a dictionary to map policy tag names to BigQuery columns.
# Replace with your column names and the corresponding policy tag names.
column_to_policy_tags = {
    "phone_number": " Lab_data.phone"
    # Add more columns and associated policy tags as needed.
}

# Fetch the BigQuery table reference.
dataset_ref = bigquery_client.dataset(dataset_id)
table_ref = dataset_ref.table(table_id)

# Fetch the list of existing policy tags.
policy_tags = datacatalog_client.list_policy_tags(parent=f"projects/{project_id}/locations/us-central1/taxonomies/{taxonomy_name}")

# Create a mapping of policy tag names to their corresponding IDs.
policy_tag_name_to_id = {policy_tag.display_name: policy_tag.name for policy_tag in policy_tags}

# Fetch the BigQuery table's schema.
schema = bigquery_client.get_table(table_ref).schema

# Attach policy tags to columns in the BigQuery table.
for column_name, tag_name in column_to_policy_tags.items():
    # Verify that the policy tag exists before attaching.
    if tag_name in policy_tag_name_to_id:
        column = next(col for col in schema if col.name == column_name)
        tag_id = policy_tag_name_to_id[tag_name]
        column.policy_tags = [tag_id]

# Update the table schema with policy tags.
bigquery_client.update_table(table_ref, ["schema"])

print("Policy tags attached to BigQuery table columns.")
