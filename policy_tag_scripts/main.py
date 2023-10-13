from google.cloud import datacatalog_v1

def create_taxonomy(
    # TODO(developer): Set project_id to the ID of the project the
    #  taxonomy will belong to.
    project_id: str = "hca-data-lake-poc",
    # TODO(developer): Specify the geographic location where the
    #  taxonomy should reside.
    location_id: str = "us-central1",
    # TODO(developer): Set the display name of the taxonomy.
    display_name: str = "test-taxonomy",
):
    # TODO(developer): Construct a Policy Tag Manager client object. To avoid
    # extra delays due to authentication, create a single client for your
    # program and share it across operations.
    client = datacatalog_v1.PolicyTagManagerClient()

    # Construct a full location path to be the parent of the taxonomy.
    parent = datacatalog_v1.PolicyTagManagerClient.common_location_path(
        project_id, location_id
    )

    # TODO(developer): Construct a full Taxonomy object to send to the API.
    taxonomy = datacatalog_v1.Taxonomy()
    taxonomy.display_name = display_name
    taxonomy.description = "This Taxonomy represents ..."

    # Send the taxonomy to the API for creation.
    taxonomy = client.create_taxonomy(parent=parent, taxonomy=taxonomy)
    print(f"Created taxonomy {taxonomy.name}")

create_taxonomy()