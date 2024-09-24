from flask import Blueprint, jsonify

bp_errors = Blueprint('bp_errors', __name__)

@bp_errors.errorhandler(404)
def handle_404_error(error):
    return jsonify({'error': 'Not Found', 'message': str(error)}), 404

@bp_errors.errorhandler(400)
def handle_400_error(error):
    return jsonify({'error': 'Bad Request', 'message': str(error)}), 400

@bp_errors.errorhandler(500)
def handle_500_error(error):
    return jsonify({'error': 'Internal Server Error', 'message': str(error)}), 500