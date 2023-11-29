# app.py
import reidentify
import flask
import os
from flask import request

app = flask.Flask(__name__)

@app.route("/", methods=['POST'])
def hello_world():
    project = "hca-data-lake-poc"
    key_name= "SECRET_KEY_NAME"
    secret_name = "SECRET_NAME"
    
    wrapped_key = reidentify.access_secret_version(project,secret_name)

    return_value = []    
    request_json = request.get_json(silent=True)
    calls = request_json['calls']
    len_of_calls = len(calls) if len(calls)>0 else 1

    access_list = ["amruth.chinthala@egen.ai","janeesh.jayaraj@egen.ai"]

    if request_json['sessionUser'] in access_list:

        for call in calls:
            input_str = str(call[0])
            output_str = reidentify.reidentify_deterministic(project= project, input_str = input_str,surrogate_type = "SSN",key_name = key_name,wrapped_key = wrapped_key)
            return_value.append(output_str)

        return flask.jsonify({"replies": return_value})

    else:
        return flask.jsonify({"errorMessage": "user doesn't have enough permissions to execute this task"}), 400

if __name__ == '__main__':
    app.run(debug=True)
