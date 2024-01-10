import functions_framework
import deidentify
import flask

@functions_framework.http
def hello_http(request):

    project = "hca-data-lake-poc"
    key_name= "KEY NAME PLACEHOLDER"
    secret_name = "SECRET NAME PLACEHOLDER"
    wrapped_key = deidentify.access_secret_version(project,secret_name)
 
    request_json = request.get_json(silent=True)
    calls = request_json['calls']

    print(f"Calls: {calls}")

    return_value = deidentify.deidentify_with_date_shift(project, ["date_of_visit","patient_id"],calls,["date_of_visit"],-10000,-400,"patient_id",wrapped_key,key_name)

    return flask.jsonify({"replies": return_value})

