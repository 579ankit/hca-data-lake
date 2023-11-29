# app.py
import deidentify
import flask
import os
from flask import request

app = flask.Flask(__name__)

@app.route("/", methods=['POST'])
def hello_world():
    project = "hca-data-lake-poc"
 
    request_json = request.get_json(silent=True)
    calls = request_json['calls']
    return_value = []

    project = "hca-data-lake-poc"
    
    for call in calls:

      input_str = str(call[0])
      output_str = deidentify.deidentify_deterministic(project, input_str)
      return_value.append(output_str)

    return flask.jsonify({ "replies":  return_value })

if __name__ == '__main__':
    app.run(debug=True)
