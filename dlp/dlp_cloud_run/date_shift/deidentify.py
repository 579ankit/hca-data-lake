import base64
import google.cloud.dlp
from google.cloud import secretmanager
import csv
from datetime import datetime
from typing import List
from google.cloud.dlp_v2 import types

def access_secret_version(project_id, secret_id, version_id="latest"):
    # Create the Secret Manager client.
    client = secretmanager.SecretManagerServiceClient()

    # Build the resource name of the secret version.
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

    # Access the secret version.
    response = client.access_secret_version(name=name)

    # Return the decoded payload.
    return response.payload.data.decode('UTF-8')



def deidentify_with_date_shift(
    project: str,
    schema: List[str]=None,
    data: List[List]=None,
    date_fields: List[str] = None,
    lower_bound_days: int = None,
    upper_bound_days: int = None,
    context_field_id: str = None,
    wrapped_key: str = None,
    key_name: str = None,
) -> None:
    """Uses the Data Loss Prevention API to deidentify dates in a CSV file by
        pseudorandomly shifting them.
    Args:
        project: The Google Cloud project id to use as a parent resource.
        date_fields: The list of (date) fields in the CSV file to date shift.
            Example: ['birth_date', 'register_date']
        lower_bound_days: The maximum number of days to shift a date backward
        upper_bound_days: The maximum number of days to shift a date forward
        context_field_id: (Optional) The column to determine date shift amount
            based on. If this is not specified, a random shift amount will be
            used for every row. If this is specified, then 'wrappedKey' and
            'keyName' must also be set. Example:
            contextFieldId = [{ 'name': 'user_id' }]
        key_name: (Optional) The name of the Cloud KMS key used to encrypt
            ('wrap') the AES-256 key. Example:
            key_name = 'projects/YOUR_GCLOUD_PROJECT/locations/YOUR_LOCATION/
            keyRings/YOUR_KEYRING_NAME/cryptoKeys/YOUR_KEY_NAME'
        wrapped_key: (Optional) The encrypted ('wrapped') AES-256 key to use.
            This key should be encrypted using the Cloud KMS key specified by
            key_name.
    """

    # Instantiate a client
    dlp = google.cloud.dlp_v2.DlpServiceClient()

    # Convert the project id into a full resource id.
    parent = f"projects/{project}/locations/global"

    # Convert date field list to Protobuf type
    def map_fields(field: str) -> dict:
        return {"name": field}

    if date_fields:
        date_fields = map(map_fields, date_fields)
    else:
        date_fields = []

    f = []
    f.append(schema)
    for i in data:
        f.append(i)

    #  Helper function for converting CSV rows to Protobuf types
    def map_headers(header: str) -> dict:
        return {"name": header}

    def map_data(value: str) -> dict:
        try:
            # date = datetime.strptime(value, "%m/%d/%Y")
            date = datetime.strptime(value, "%Y-%m-%d")
            return {
                "date_value": {"year": date.year, "month": date.month, "day": date.day}
            }
        except ValueError:
            return {"string_value": value}

    def map_rows(row: str) -> dict:
        return {"values": map(map_data, row)}

    # Using the helper functions, convert CSV rows to protobuf-compatible
    # dictionaries.
    print(f)
    csv_headers = map(map_headers, f[0])
    csv_rows = map(map_rows, f[1:])

    # Construct the table dict
    table_item = {"table": {"headers": csv_headers, "rows": csv_rows}}

    # Construct date shift config
    date_shift_config = {
        "lower_bound_days": lower_bound_days,
        "upper_bound_days": upper_bound_days,
    }

    # If using a Cloud KMS key, add it to the date_shift_config.
    # The wrapped key is base64-encoded, but the library expects a binary
    # string, so decode it here.
    if context_field_id and key_name and wrapped_key:
        date_shift_config["context"] = {"name": context_field_id}
        date_shift_config["crypto_key"] = {
            "kms_wrapped": {
                "wrapped_key": base64.b64decode(wrapped_key),
                "crypto_key_name": key_name,
            }
        }
    elif context_field_id or key_name or wrapped_key:
        raise ValueError(
            """You must set either ALL or NONE of
        [context_field_id, key_name, wrapped_key]!"""
        )

    # Construct Deidentify Config
    deidentify_config = {
        "record_transformations": {
            "field_transformations": [
                {
                    "fields": date_fields,
                    "primitive_transformation": {
                        "date_shift_config": date_shift_config
                    },
                }
            ]
        }
    }


    def write_data(data: types.Value) -> str:
        if data.date_value:
            return "{}-{}-{}".format(
                data.date_value.year,
                data.date_value.month,
                data.date_value.day,
            )


    # Call the API
    response = dlp.deidentify_content(
        request={
            "parent": parent,
            "deidentify_config": deidentify_config,
            "item": table_item,
        }
    )

    rest=[]
    for row in response.item.table.rows:
        lst=list(map(write_data, row.values))
        rest.append(lst[0])
    
    return rest