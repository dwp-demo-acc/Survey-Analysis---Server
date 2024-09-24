import pandas as pd
from modules.data_manager import DataManager


class Prompter:

    def __init__(self) -> None:
        # self.data_manager = DataManager().get_raw_data()
        self.topic = DataManager().get_context_schema_data()['main-topic']

    # level a
    def get_exe_summary_prompt():
        pass

    # level b
    def get_question_prompt(self,question_column,statistical_summary):
       
        return f"""
                Given the survey topic '{self.topic}' and the question '{question_column}', the statistical summary of the dataset of responses is as follows:

                {statistical_summary}

                Based on this data, provide insights only on what is directly observable in the data. Do not include any speculative or inconclusive remarks. Keep the response succinct and focused on the data.
                Remember Focus on concise, clear, and direct insights. Limit the response to the most significant findings only.
                """
    
    # level c
    # title?Customer Satisfaction and Sentiment Analysis
    def get_segment_prompt(title:str,question_column:str,pivot_table:pd.DataFrame):
        anomaly_example = { 
            "type": "enum of the anomaly type if it anomaly/maximum/minimum",
            "category name": "String with the category name",
            "description": "String describing the anomaly in the founded category",
            "impact": "String explaining the impact of this anomaly",
            "value": "string value of mean"
            }

        json_example  = [anomaly_example,anomaly_example] 

        prompt = f"""
        You are a tool to analyze survey results. We need you to look into the data and find any type of anomaly or correlation that exists in the data.

        Survey title: {title}.

        Survey question: {question_column}

        Data table: 
        {pivot_table}

        Table statistics:
        {pivot_table.describe().to_string()}

        The rules are as follows:

        1. The analytics must be in reference to the survey title.
        2. You will analyze the table to find any anomaly or correlation in the data keep your response short and concise.
        3. An anomaly can be of many kinds, in the numbers, in their absence, or if they in any way look to be abnormal in reference to all of the table, the survey contact title, or in comparison with other results or tables.
        4. Every table consists of a header with ratings from 1 to 7, where 7 is the highest client satisfaction and 1 is the lowest.
        5. Every table ends with average (avg) calculations; use them in addition to pivot table and statistics provided bellow to help with anomaly detection.
        6. Every row will start with the segment name and the values.
        7. Present the response in json format with anomalies that showcase the anomalies (according the types maximum and/or minimum and/or anomaly).  
        8. Highlight these anomalies explicitly, detailing their significance and potential impact in relation to the survey's objectives.
        9. Present your findings in a JSON format with keys for 'type' (minimum /maximum / anomaly ) and 'category name'. Each key should have a structured response including a description, the impact of the finding, and the value of the calculated mean and the category name.

        Example of your Response in Array of Object (anomaly_example) in JSON structure:
        {json_example}
    
        - Remember, your response should be in JSON format only, with no additional text outside of the JSON structure. 
        - Use double quotes (") in your JSON response.

        """

        return prompt
    
    def analyze_comments_by_question(self, question, column_name, df) -> str:

        prompt = f"""
        Given the customer feedback data in the {column_name} column, please analyze the comments related to the question {question}. Summarize the key themes and sentiments expressed by the customers. Highlight any recurring feedback, both positive and negative, and identify any suggestions for improvement or areas of excellence. Consider the overall customer satisfaction and provide insights into how the services are being received, including any notable trends or patterns observed in the comments.

        Comments:
        {df.to_string()}
        """

        return prompt