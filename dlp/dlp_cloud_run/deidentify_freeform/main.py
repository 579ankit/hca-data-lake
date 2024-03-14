# app.py
import deidentify
import flask
import os
from flask import request
import syslog
import numpy as np
import time


app = flask.Flask(__name__)


def break_into_batches(lst):
    """
    Converts a list into ndarray of 4 elements

    Args:
        lst (list)

    Returns:
        numpy.ndarray
    """
    split_parts = np.array_split(lst, 4)
    return split_parts


def api_call(input_str):

    project = "PROJECT_NAME"
    output_str_to_return = deidentify.deidentify_deterministic(project, input_str)
    return output_str_to_return


@app.route("/", methods=["POST"])
def hello_world():

    request_json = request.get_json(silent=True)
    calls = request_json["calls"]
    len_of_calls = str(len(calls))
    syslog.syslog(len_of_calls)
    input_list = [str(call[0]) for call in calls]

    # Check if whole list has null values
    if input_list.count(None) == len_of_calls:

        syslog.syslog("All call items are NULL/ None")
        return flask.jsonify({"replies": [None] * len_of_calls})

    else:

        input_str = " || ".join(input_list)
        try:
            return_value = api_call(input_str)
            return flask.jsonify({"replies": return_value.split(" || ")})

        except Exception as e:

            if "400 Too many findings to de-identify".lower() in str(e).lower():

                try:

                    output_list_whole = []
                    syslog.syslog(
                        "Found 400 exception, so breaking down into more batches"
                    )
                    new_batches = break_into_batches(input_list)
                    for count, new_batch in enumerate(new_batches, start=1):
                        input_str_new = " || ".join(new_batch.tolist())
                        syslog.syslog("HIT " + str(count) + "/4")
                        output_str = api_call(input_str_new)
                        output_list_whole.append(output_str)

                    output_str_to_return = " || ".join(output_list_whole)
                    return_value = output_str_to_return.split(" || ")
                    return flask.jsonify({"replies": return_value})

                except Exception as e:

                    if "429 Quota exceeded".lower() in str(e).lower():
                        syslog.syslog(
                            "Encountered 429 error after breaking, waiting for a minute to send it to BQ with error code as 429"
                        )
                        # time.sleep(60)
                        syslog.syslog(
                            "Encountered 429 error, sending it back to BQ with error code as 429"
                        )
                        return (
                            flask.jsonify({"errorMessage": "429 Quota limit error"}),
                            429,
                        )
                    else:
                        syslog.syslog("Got new error after breaking: " + str(e))
                        return (
                            flask.jsonify(
                                {
                                    "errorMessage": "New error encountered, hence stopping the process"
                                }
                            ),
                            500,
                        )

            elif "429 Quota exceeded".lower() in str(e).lower():
                syslog.syslog(
                    "Encountered 429 error, waiting for a minute to send it to BQ with error code as 429"
                )
                # time.sleep(60)
                syslog.syslog(
                    "Encountered 429 error, sending it back to BQ with error code as 429"
                )
                return flask.jsonify({"errorMessage": "429 Quota limit error"}), 429

            else:
                syslog.syslog("Found other exception: " + str(e))
                return (
                    flask.jsonify(
                        {
                            "errorMessage": "New error encountered, hence stopping the process"
                        }
                    ),
                    500,
                )


if __name__ == "__main__":
    app.run(debug=True)
