from google.cloud import datacatalog_v1

def create_taxonomy_with_policy_tags(
    project_id: str = "hca-data-lake-poc",
    location_id: str = "us-central1",
    display_name: str = "test-taxonomy",
    policy_tags: list = None
):
    # Create a Policy Tag Manager client.
    client = datacatalog_v1.PolicyTagManagerClient()

    # Construct a full location path to be the parent of the taxonomy.
    parent = datacatalog_v1.PolicyTagManagerClient.common_location_path(
        project_id, location_id
    )

    # Construct the taxonomy.
    taxonomy = datacatalog_v1.Taxonomy()
    taxonomy.display_name = display_name
    taxonomy.description = "This Taxonomy represents ..."

    # Create the taxonomy.
    taxonomy = client.create_taxonomy(parent=parent, taxonomy=taxonomy)
    print(f"Created taxonomy {taxonomy.name}")

    # Create policy tags and associate them with the taxonomy.
    for policy_tag_data in policy_tags:
        policy_tag = datacatalog_v1.PolicyTag()
        policy_tag.display_name = policy_tag_data.get('display_name')
        policy_tag.description = policy_tag_data.get('description')
        policy_tag = client.create_policy_tag(parent=taxonomy.name, policy_tag=policy_tag)
        print(f"Created policy tag {policy_tag.name}")

# Example usage:
policy_tags = [
    {
        "display_name": "Sensitive Data",
        "description": "Tags data with sensitive information.",
    },
    {
        "display_name": "Public Data",
        "description": "Tags data that is public and non-sensitive.",
    },
]

create_taxonomy_with_policy_tags(policy_tags=policy_tags)
