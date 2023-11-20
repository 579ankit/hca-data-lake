# app.py
import deidentify
import flask
import os
from flask import request

app = flask.Flask(__name__)

@app.route("/", methods=['POST'])
def hello_world():
    project = "hca-data-lake-poc"
    key_name= "KMS_KEY_PATH"
    secret_name = "SECRET_NAME"
    wrapped_key = deidentify.access_secret_version(project,secret_name)
 
    request_json = request.get_json(silent=True)
    calls = request_json['calls']

    print(f"Calls: {calls}")

    return_value = deidentify.deidentify_with_date_shift(project, ["date_of_visit","patient_id"],calls,["date_of_visit"],-10000,-400,"patient_id",wrapped_key,key_name)

    return flask.jsonify({"replies": return_value})

if __name__ == '__main__':
    app.run(debug=True)
