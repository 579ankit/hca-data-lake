# app.py
import reidentify
import flask
import os
from flask import request
import numpy as np
import syslog

app = flask.Flask(__name__)
project = "PROJECT-NAME"
key_name = "KEY_NAME"
# access_list = ["EMAIL_ID_1","EMAIL_ID_2"]


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


@app.route("/", methods=["POST"])
def hello_world():
    return_value = []
    request_json = request.get_json(silent=True)
    calls = request_json["calls"]

    # if request_json['sessionUser'] in access_list:

    input_list = [str(call[0]) for call in calls]
    len_of_calls = str(len(calls))
    syslog.syslog(len_of_calls)

    # Check if whole list has null values
    if input_list.count(None) == len_of_calls:

        syslog.syslog("All call items are NULL/ None")
        return flask.jsonify({"replies": [None] * len_of_calls})

    else:
        input_str = " || ".join(input_list)

        try:
            syslog.syslog("HIT")
            output_str = reidentify.reidentify_deterministic(
                input_str=input_str, key_name=key_name
            )
            return_value = output_str.split(" || ")
            return flask.jsonify({"replies": return_value})

        except Exception as e:

            if "400 Too many findings to de-identify".lower() in str(e).lower():

                output_list_whole = []
                syslog.syslog("Found 400 exception, so breaking down into more batches")
                new_batches = break_into_batches(input_list)

                for count, new_batch in enumerate(new_batches, start=1):
                    input_str_new = " || ".join(new_batch.tolist())
                    syslog.syslog("HIT " + str(count) + "/4")
                    output_str = reidentify.reidentify_deterministic(
                        input_str=input_str_new, key_name=key_name
                    )
                    output_list_whole.append(output_str)

                output_str_to_return = " || ".join(output_list_whole)
                return_value = output_str_to_return.split(" || ")

                return flask.jsonify({"replies": return_value})

            else:
                syslog.syslog("Found other exception: " + str(e))

                return (
                    flask.jsonify(
                        {
                            "errorMessage": "New error encountered, hence stopping the process"
                        }
                    ),
                    400,
                )

    # else:
    #     return flask.jsonify({"errorMessage": "user doesn't have enough permissions to execute this task"}), 400


if __name__ == "__main__":
    app.run(debug=True)
