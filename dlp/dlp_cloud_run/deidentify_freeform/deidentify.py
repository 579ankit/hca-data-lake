import google.cloud.dlp
import syslog


def deidentify_deterministic(
    project: str,
    input_str: str,
) -> None:

    # Instantiate a client
    dlp = google.cloud.dlp_v2.DlpServiceClient()

    # Convert the project id into a full resource id.
    parent = f"projects/{project}/locations/us-central1"

    # Convert string to item
    item = {"value": input_str}

    # Call the API
    response = dlp.deidentify_content(
        request={
            "parent": parent,
            "inspect_template_name": "projects/hca-usr-hin-proc-datalake/locations/us-central1/inspectTemplates/dlp_inspect_crypto_1123_04",
            "deidentify_template_name": "projects/hca-usr-hin-proc-datalake/locations/us-central1/deidentifyTemplates/dlp_deidentify_1123",
            "item": item,
        }
    )

    output_str = response.item.value

    return output_str
