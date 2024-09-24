from flask import Blueprint, jsonify, request,flash,redirect
import os
import json
from flask_cors import CORS, cross_origin
import pandas as pd
import json
from plotly.utils import PlotlyJSONEncoder
from errors.errors import handle_500_error, handle_404_error, handle_400_error
from utils.charts import *
from modules.data_manager import DataManager, insights
import re
from modules.azure_blob_storage import AzureBlobStorage
from azure.core.exceptions import ResourceExistsError


from werkzeug.utils import secure_filename


from utils.utility import get_graphs_data_wrapper

bp_apis = Blueprint('bp_apis', __name__)

@bp_apis.route('/get_question_plot_details', methods=["POST"])
def get_question_plot_details():
    try:
        if request.method == "POST":
            data = request.get_json()
            print('data',data)
            column_name = data.get('column_name')
            if column_name not in insights:
                # Insight data is not available
                return jsonify({'error': f"Insights data for '{column_name}' column is not found."}), 500
            
            df = DataManager().get_dataframe()
            analysisType = get_chart_name(column_name)
            graph = analyse_column_graph(df, column_name, analysisType)
            # graph = update_title_graph(graph, column_name)

            all_segments_graphs = []
            lvl_2_segmentation_data = get_segmentation_lvl_2(column_name).get(column_name, {})
            for segment_name, data in lvl_2_segmentation_data.items():
                graph_min = get_graph(data.get("pivot_table"), data.get("raw_minimum"), segment_name,column_name)
                graph_max = get_graph(data.get("pivot_table"),data.get("raw_maximum"), segment_name,column_name)

                lvl_2_segmentation_insight = get_segmentation_lvl_2_insight_ultra(column_name, segment_name)

                all_segments_graphs.append({
                    'title': segment_name,
                    'graph_min_lvl_2': graph_min,
                    'graph_max_lvl_2': graph_max,   
                    'sub_title': 'Insights',             
                    'insight': lvl_2_segmentation_insight,
                })

            column_insight_summary = insights[column_name]["column_insight_summary"]
            column_insight_ultra_summary = insights[column_name]["column_insight_ultra_summary"]
            
            response = {
                'graph_lvl_1': {
                    'title': column_name,
                    'data': graph,
                },
                'insights': {
                    'column_insight_short_summary': column_insight_ultra_summary,
                    'column_insight_detailed_summary': column_insight_summary
                },
                'cards': all_segments_graphs
            }

            insights_dir = "files/insights"
            # Check insights directory exists
            os.makedirs(insights_dir, exist_ok=True)
            # sanitized column name for the file name
            sanitized_column_name = re.sub(r'[^\w\s]', '', column_name)
            sanitized_column_name = sanitized_column_name.replace(' ', '_')
            insights_file_path = os.path.join(insights_dir, f"{sanitized_column_name}.json")

            with open(insights_file_path, 'w') as file:
                json.dump(response, file, indent=4, ensure_ascii=False)

            return jsonify(response)

    except Exception as e:
        return handle_500_error(e)


@bp_apis.route("/survey-insights-report", methods=["POST"])
def serve_survey_insights():
    try:
        data = request.get_json()
        file_name = data.get('file_name')
        if not file_name:
            return handle_400_error(ValueError("'file_name' parameter is required"))

        DataManager().load_survey_and_context_data(file_name)
        
        insights_file_path = f"files/Survey CS Project_insights_v2.json"

        if os.path.exists(insights_file_path):
            with open(insights_file_path, "r") as file:
                insights_data = json.load(file)
                if not insights_data: 
                    return jsonify({'error': 'Insights data is not ready. Please try again later.'}), 202                
                insights.clear()
                insights.update(insights_data)          

            executive_page_summary = insights_data.pop('page_executive_summary', None)
            # generate_graphs()
            
            graphs_data_wrapper = get_graphs_data_wrapper(insights_data)
            response = {
                'graphs_data': graphs_data_wrapper,
                'executive_page_summary': executive_page_summary
            }


            #generate_graphs()

            return jsonify(response)
        else:
            generate_graphs()
            prefix = os.path.splitext(file_name)[0]
            return handle_404_error(FileNotFoundError(f"'{prefix}' file not found"))

    except Exception as e:
        # current_app.logger.error(f"Error serving survey insights: {str(e)}")
        return handle_500_error(e)
    

@bp_apis.route("/survey-participants-page", methods=["POST"])
def serve_survey_participants():
    try:
        data = request.get_json()
        file_name = data.get('file_name')
        if not file_name:
            return handle_400_error(ValueError("'file_name' parameter is required"))
        df = DataManager().get_dataframe()
       
        graphs = generate_segment_graphs() 
        response ={
            'num_duplicated_answer':get_num_of_duplicate_records(df),
            'num_of_survey_participants':get_num_of_survey_participants(df),
            'uncompleted_answers':get_uncompleted_answers(df),
            'graphs':graphs
        }
        return jsonify(response)
        
    except Exception as e:
        return handle_500_error(e)


@bp_apis.route("/upload-file",methods=['POST'])
def upload_survey_file():
    try:
        if 'file' not in request.files:
                flash('No file part')
                return redirect(request.url)    
        file = request.files['file']
        if file.filename == '':
                flash('No selected file')
                return redirect(request.url)
        if file and allowed_file(file.filename):
                file_name = secure_filename(file.filename)
                survey_prefix, suffix = os.path.splitext(file_name.lower())
                storage = AzureBlobStorage()
                folder_path = f"{survey_prefix}/"
                if suffix.lower() == '.json':
                    file_name = folder_path + "context_" + file_name
                else: 
                    file_name = folder_path + "survey_" + file_name

                res = storage.upload_blob(file,file_name=file_name)
                
                return jsonify(res)
    except ResourceExistsError as e:
        # Handle the specific BlobAlreadyExists error
        return jsonify({"message": "BlobAlreadyExists", "details": str(e)}), 409

    except Exception as e:
        return handle_500_error(e)


def allowed_file(filename):
    ALLOWED_EXTENSIONS = {'csv', 'xlsx','json'}
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS