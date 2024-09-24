from flask import Flask, request, jsonify
import os
import json
from flask_cors import CORS, cross_origin
import json
from plotly.utils import PlotlyJSONEncoder
from errors.errors import bp_errors
from routes.apis import bp_apis
from dotenv import load_dotenv
load_dotenv()

application = Flask(__name__)
CORS(application, origins="*")

# Register the Blueprint
application.register_blueprint(bp_errors)
application.register_blueprint(bp_apis)

if __name__ == '__main__':
    application.run(debug=True)
