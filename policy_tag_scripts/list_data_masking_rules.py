
from google.cloud import bigquery

# Construct a BigQuery client object.
client = bigquery.Client()

# TODO(developer): Set dataset_id to the ID of the dataset that contains
#                  the routines you are listing.
dataset_id = 'hca-data-lake-poc.custom_rule'

routines = client.list_routines(dataset_id)  # Make an API request.

print("Routines contained in dataset {}:".format(dataset_id))
for routine in routines:
    print(routine.reference)