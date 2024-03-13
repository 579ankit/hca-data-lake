import base64
import google.cloud.dlp
from google.cloud import secretmanager

secret_name = "SECRET_NAME"
project = "PROJECT_NAME"


def access_secret_version(project_id, secret_id, version_id="latest"):
    # Create the Secret Manager client.
    client = secretmanager.SecretManagerServiceClient()

    # Build the resource name of the secret version.
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

    # Access the secret version.
    response = client.access_secret_version(name=name)

    # Return the decoded payload.
    return response.payload.data.decode('UTF-8')


def reidentify_deterministic(
    input_str: str,
    alphabet: str = None,
    key_name: str = None,
) -> None:

    # Instantiate a client
    dlp = google.cloud.dlp_v2.DlpServiceClient()

    # Convert the project id into a full resource id.
    parent = f"projects/{project}/locations/us-central1"

    # The wrapped key is base64-encoded, but the library expects a binary
    # string, so decode it here.
    wrapped_key = access_secret_version(project,secret_name)
    wrapped_key = base64.b64decode(wrapped_key)

    # Construct Deidentify Config
    reidentify_config = {
        "info_type_transformations": {
            "transformations" : [
            {
            "info_types":[
                {
                    "name":"SSN"
                }
            ],
            "primitive_transformation": {
                "crypto_deterministic_config": {
                    "crypto_key": {
                        "kms_wrapped": {
                            "wrapped_key": wrapped_key,
                            "crypto_key_name": key_name,
                        }
                    },
                    "surrogate_info_type": {"name": "SSN"},
                }
            },
        },
            {
            "info_types":[
                {
                    "name":"pat_id"
                },
            ],
            "primitive_transformation": {
                "crypto_deterministic_config": 
                {
                    "crypto_key": {
                        "kms_wrapped": {
                            "wrapped_key": wrapped_key,
                            "crypto_key_name": key_name,
                        }
                    },
                    "surrogate_info_type": {"name": "pat_id"},
                }
            },
        }
            ]
        }
    }

    inspect_config = {
    "custom_info_types":[
    {
        "info_type":{
        "name":"SSN"
        },
        "surrogate_type":{
        }
    },
    {
        "info_type":{
        "name":"pat_id"
        },
        "surrogate_type":{
        }
    }
    ]
}

    # Convert string to item
    item = {"value": input_str}

    # Call the API
    response = dlp.reidentify_content(
        request={
            "parent": parent,
            "reidentify_config": reidentify_config,
            "inspect_config": inspect_config,
            "item": item,
        }
    )

    return response.item.value