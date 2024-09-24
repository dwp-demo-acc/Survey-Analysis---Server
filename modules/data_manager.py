import copy
import pandas as pd
from modules.azure_blob_storage import AzureBlobStorage
import json
import io

class DataManager:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._df = pd.DataFrame()
            cls._instance._filename = None
            cls._instance._exe_summary = None
            cls._instance._context_schema_data = None
            cls._instance._count_loading = 0
            cls._instance._curr_graph_name = ""
            cls._instance._is_fully_loaded_survey_and_context_data = False
            cls._instance._survey_insight = dict()
            cls._instance._items_breadcrumb = [{"label": "Home", "href": "/", "external_link": True}]

        return cls._instance

    def get_dataframe(self):
        return self._df

    def set_dataframe(self, table):
        self._df = pd.DataFrame(table)

    def set_filename(self, filename):
        self._filename = filename

    def get_filename(self):
        return self._filename

    def set_json_data(self, _json_data):
        self._json_data = _json_data

    def get_json_data(self):
        return self._json_data

    # deprecated - delete after stabilize load_survey_and_context_data
    def set_context_schema_data(self, survey_name: str):
        """
        survey_name : survey_name stored in azure data storage "surveyanalysisdata" in container name "data"
        in the "data" container we stored each survey and his context in separate folder.
        folder name same as the survey_name 
        inside each folder we store 2 files:
        1. survey_<survey_name>.<xls/sxls/csv> - as the raw survey file.
        2. context_<survey_name>.json - as the survey context schema file.
        """

        # convert survey name into schema name as json file
        file_name = survey_name.rsplit(".", 1)[0]
        schema_name = 'context_' + file_name + '.json'

        blob_name = f'{file_name}\\{schema_name}'

        storage = AzureBlobStorage()
        file_data = storage.read_blob(blob_name)
        self._context_schema_data = json.loads(file_data.decode('utf-8'))

    def get_context_schema_data(self):
        return self._context_schema_data

    def get_exe_summary(self):
        return self._exe_summary

    def set_exe_summary(self, exe_summary):
        self._exe_summary = exe_summary

    def get_count_loading(self):
        return self._count_loading

    def set_count_loading(self, count):
        self._count_loading = count

    def get_curr_graph_name(self):
        return self._curr_graph_name
    
    def get_items_breadcrumb(self):
        return self._items_breadcrumb
    
    def set_items_breadcrumb(self,breadcrumb_list):
         self._items_breadcrumb =breadcrumb_list

    def set_curr_graph_name(self, graph_name):
        self._curr_graph_name = graph_name

    def get_survey_insight(self):
        return self._survey_insight

    def set_survey_insight(self, file):
        self._survey_insight = file

    def get_is_fully_loaded_survey_and_context_data(self):
        return self._is_fully_loaded_survey_and_context_data

    def set_is_fully_loaded_survey_and_context_data(self, flag):
        self._is_fully_loaded_survey_and_context_data = flag

    def filter_data_set(self, df):
        relevant_cols = [item["columnName"] for item in self._context_schema_data["data-table"] if
                         item.get("isRelevantAnalysis")]
        return df[relevant_cols]

    def load_file_content_into_df(self, contents, filename: str):
        content = None
        try:
            if 'csv' in filename:
                content = pd.read_csv(
                    io.StringIO(contents.decode('utf-8')))
            elif 'xls' in filename or 'xlsx' in filename: 
                content = pd.read_excel(io.BytesIO(contents))
            
            return content

        except FileNotFoundError as context_error:
            raise Exception(f"Cant load context {context_error}")

        except Exception as e:
            print(e)
            return None
    
    def load_survey_and_context_data(self, survey_file: str):
        self.set_filename(survey_file)

        folder_name = survey_file.rsplit(".", 1)[0]
        context_file = "context_" + folder_name + ".json"

        survey_blob_name = f'{folder_name}/{"survey_" + survey_file}'
        context_blob_name = f'{folder_name}/{context_file}'

        storage = AzureBlobStorage()
        survey_file_data = storage.read_blob(survey_blob_name)
        context_file_data = storage.read_blob(context_blob_name)

        # No Setters For this
        self._context_schema_data = json.loads(context_file_data.decode('utf-8'))

        survey_df = self.load_file_content_into_df(survey_file_data, survey_file)
       
        filtered_survey_df = self.filter_data_set(survey_df)
       
        operational_df = copy.deepcopy(filtered_survey_df)
        for column in filtered_survey_df.columns:
            column_type = self.get_column_item_value_from_context(column, "columnType")
            analysis_type = self.get_column_item_value_from_context(column, "analysisType")
            to_transition = self.get_column_item_value_from_context(column, "toTransition")
            if to_transition:
                answer_range = self.get_column_item_value_from_context(column, "answerRange")
                operational_df[column] = operational_df[column].map(answer_range)
            if column_type == "verbal" and "sentiment" in analysis_type:
                operational_df = self.load_sentiment_to_df_and_context(operational_df, self.get_column_object_from_context(column))

        self.set_dataframe(operational_df)

    def update_dict(self, old_dict, new_dict):
        updated_dict = old_dict.copy()

        for key, value in new_dict.items():
            updated_dict[key] = value

        return updated_dict

    def load_sentiment_to_df_and_context(self, filtered_survey_df, column_obj):
        # TODO Support multiple textual columns
        from modules.sentiment_analyzer import SentimentAnalyzer
        sentiment = SentimentAnalyzer(filtered_survey_df)
        updated_df, new_col_name = sentiment.update_local_df_with_sentiment(column_obj["columnName"])
        new_col_context = self.update_dict(column_obj, {"columnName": new_col_name})
        self._context_schema_data["data-table"].append(new_col_context)

        return updated_df

    def get_column_item_value_from_context(self, column: str, key_name: str | None) -> str | dict:
        data = self.get_context_schema_data()
        data = data.get('data-table', [])

        for item in data:
            if item.get('columnName') == column:
                return item.get(key_name)

        raise Exception(f"{column} does not exist in the data manager context")

    def get_column_object_from_context(self, column: str) -> str | dict:
        data = self.get_context_schema_data()
        data = data.get('data-table', [])

        for item in data:
            if item.get('columnName') == column:
                return item

        raise Exception(f"{column} does not exist in the data manager context")

insights = dict()
