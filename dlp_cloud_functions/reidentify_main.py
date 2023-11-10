import functions_framework
import reidentify
import flask

@functions_framework.http
def hello_http(request):

    project = "hca-data-lake-poc"
    key_name= "KMS-KEY-PATH-PLACEHOLDER"
    secret_name = "SECRET-NAME-PLACEHOLDER"
    
    wrapped_key = reidentify.access_secret_version(project,secret_name)

    return_value = []    
    request_json = request.get_json(silent=True)
    calls = request_json['calls']

    for call in calls:
        input_str = str(call)
        output_str = reidentify.reidentify_deterministic(project= project, input_str = input_str,surrogate_type = "SSN",key_name = key_name,wrapped_key = wrapped_key)
        return_value.append(output_str)

    return flask.jsonify({"replies": return_value})