import functions_framework
import reidentify
import flask

@functions_framework.http
def hello_http(request):

    project = "hca-data-lake-poc"
    key_name= "KEY NAME PLACEHOLDER"
    secret_name = "SECRET NAME PLACHOLDER"
    
    wrapped_key = reidentify.access_secret_version(project,secret_name)

    return_value = []    
    request_json = request.get_json(silent=True)
    calls = request_json['calls']

    access_list = ["ACCESS LIST EMAIL IDs"] #Add access list email IDs here. List of Strings

    if request_json['sessionUser'] in access_list:

        for call in calls:
            input_str = str(call[0])
            output_str = reidentify.reidentify_deterministic(project= project, input_str = input_str,surrogate_type = "SSN",key_name = key_name,wrapped_key = wrapped_key)
            return_value.append(output_str)

        return flask.jsonify({"replies": return_value})

    else:

        return flask.jsonify({"errorMessage": "user doesn't have enough permissions to execute this task"}), 400