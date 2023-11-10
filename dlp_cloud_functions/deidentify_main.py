import functions_framework
import deindentify
import flask

@functions_framework.http
def hello_http(request):

    request_json = request.get_json(silent=True)
    calls = request_json['calls']
    return_value = []

    project = "hca-data-lake-poc"
    
    for call in calls:

      input_str = str(call)
      output_str = deindentify.deidentify_deterministic(project, input_str)
      return_value.append(output_str)

    return flask.jsonify({ "replies":  return_value })

