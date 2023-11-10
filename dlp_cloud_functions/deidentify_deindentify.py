import google.cloud.dlp

def deidentify_deterministic( 
    project: str,
    input_str: str,
) -> None:

    # Instantiate a client
    dlp = google.cloud.dlp_v2.DlpServiceClient()

    # Convert the project id into a full resource id.
    parent = f"projects/{project}/locations/global"

    # Convert string to item
    item = {"value": input_str}

    # Call the API
    response = dlp.deidentify_content(
        request={
            "parent": parent,
            "inspect_template_name": "projects/hca-data-lake-poc/locations/global/inspectTemplates/dlp_inspect_crypto_1107_01",
            "deidentify_template_name": "projects/hca-data-lake-poc/locations/global/deidentifyTemplates/dlp_de-identify_1107_01",
            "item": item,
        }
    )

    output_str=response.item.value
    return output_str