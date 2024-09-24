from flask import Flask, request, jsonify
import os
import json
from flask_cors import CORS, cross_origin
import pandas as pd
import json
from plotly.utils import PlotlyJSONEncoder
from modules.data_manager import DataManager, insights
from utils.filters import *
from utils.constants import *
import plotly.express as px
import numpy as np


def update_title_graph(graph, title: str):
    graph.update_layout(
        title={
            'text': f"<b>{title}</b>",
            'x': 0.5,
            'y': 0.9,
            'xanchor': 'center',
            'yanchor': 'top',
            'font': {'size': 16, 'color': 'rgb(0,85,135)', 'family': 'Arial'},
        }
    )
    return graph

def generate_bar_chart(column_name: str, df: pd.DataFrame,with_color:bool):
    value_counts = df[column_name].value_counts()
    counts_df = value_counts.reset_index()
    counts_df.columns = ['Response', 'Count']

    color_scale = ['rgb(160,220,255)', 'rgb(98,181,229)', 'rgb(0,163,224)', 'rgb(0,118,168)', 'rgb(0,85,135)',
                 'rgb(2, 47, 151)', 'rgb(4,30,66)']     
    if column_name.startswith("sentiment_"):
        title = column_name.replace("_", " ").title()
        color_scale = {'Negative': 'rgb(194,61,61)', 'Positive': 'rgb(87,210,159)'}
        counts_df['Response'] = ['Negative', 'Positive']
        counts_df['Color'] = counts_df['Response'].apply(lambda x: color_scale.get(x, 'rgb(0,0,0)'))
        graph_data_wrapper = {
            'type': 'bar',
            'data': counts_df.to_dict('records'),
            'x': 'Response',
            'y': 'Count',
            'text': 'Count',
            "layout": {
                "title": title
            }        
        }
        graph_data_wrapper.update({
            'color': 'Color',
            'color_discrete_map': 'identity'
            })
        return graph_data_wrapper

    else:
        title = column_name.replace("_", " ").capitalize()
        if len(value_counts)==MAIN_SURVEY_LEN and with_color:
            counts_df['Color'] = counts_df['Response'].astype(int).apply(
                lambda x: color_scale[x-1] if 0 < x <= len(color_scale) else color_scale[-1]
            )
        graph_data_wrapper = {
                'type': 'bar',
                'data': counts_df.to_dict('records'),
                'x': 'Response',
                'y': 'Count',
                'text': 'Count',
                "layout": {
                    "title": title
                },
                'color': 'Color',
                'color_discrete_map': 'identity'       
            }
            
        return graph_data_wrapper

            
def get_figure(analysis_type, column_name, df,with_color):
    chart_dict = {
        BAR_CHART: generate_bar_chart,
        # PIE_CHART: generate_pie_chart,
        # HISTOGRAM_CHART: generate_histogram_chart,
        SENTIMENT: generate_bar_chart
    }
    default_func = lambda x, y: None
    analysis_func = chart_dict.get(analysis_type, default_func)
    return analysis_func(column_name, df,with_color)

def analyse_column_graph(df, column_name, analysis_type,with_color=True):
    fig = get_figure(analysis_type, column_name, df,with_color)
    return fig

# Function to generate a bar chart (mock function for the example)
def generate_graphs():
    try:
        df = DataManager().get_dataframe()
        context_schema = DataManager().get_context_schema_data()
        filtered_cols_no_seg = filter_df_columns_from_scheme_excluding_key(context_schema, "segment")
        filtered_df_no_seg = df[filtered_cols_no_seg]

        #TODO: complete from dash app from_db

        # Responsible for graphs creation
        for count, column in enumerate(filtered_df_no_seg.columns):
            # Generating graph per question type
            column_type = DataManager().get_column_item_value_from_context(column, "columnType")
            analysis_type = DataManager().get_column_item_value_from_context(column, "analysisType")

            if column.startswith("sentiment_"):
                fig = analyse_column_graph(filtered_df_no_seg, column, "sentiment")
            elif any(chart_type in analysis_type for chart_type in ["Pie Chart", "Bar Chart", "Histogram Chart"]):
                fig = analyse_column_graph(filtered_df_no_seg, column, analysis_type[0])
            else:
                # TODO: at the current state sentiment is handled externaly in in a diff column refactor in mvp
                continue

            # encoded_plot_json = json.dumps(fig, cls=PlotlyJSONEncoder)
            encoded_plot_json = fig

            insights[column]['plot_chart'] = encoded_plot_json

        prefix = os.path.splitext(DataManager().get_filename())[0]
        with open(f"files/{prefix}_insights_v2.json", 'w') as file:
            json.dump(insights, file, indent=4, ensure_ascii=False)
    except Exception as e:
        print(e)




def generate_segment_graphs():
    try:
        all_graphs = []
        df = DataManager().get_dataframe()
        context_schema = DataManager().get_context_schema_data()
        segments = get_all_segments(context_schema)
        filtered_cols_with_seg=[]
        for item in context_schema["data-table"]:
            if item['columnName'] in segments and item['isRelevantAnalysis']:
                filtered_cols_with_seg.append(item['columnName'])
            
        filtered_cols_with_seg = df[filtered_cols_with_seg]


        # Responsible for graphs creation
        for count, column in enumerate(filtered_cols_with_seg.columns):
            # Generating graph per question type
            column_type = DataManager().get_column_item_value_from_context(column, "columnType")
            analysis_type = DataManager().get_column_item_value_from_context(column, "analysisType")

            if column.startswith("sentiment_"):
                fig = analyse_column_graph(filtered_cols_with_seg, column, "sentiment",False)
            elif any(chart_type in analysis_type for chart_type in ["Pie Chart", "Bar Chart", "Histogram Chart"]):
                fig = analyse_column_graph(filtered_cols_with_seg, column, "Bar Chart",False)
            else:
                # TODO: at the current state sentiment is handled externaly in in a diff column refactor in mvp
                continue

            encoded_plot_json = fig
            graph_wrapper = {'columnName':column,
                             'graphData':encoded_plot_json
                             }
            all_graphs.append(graph_wrapper) 

        prefix = os.path.splitext(DataManager().get_filename())[0]
        with open(f"files/{prefix}_insights_v3.json", 'w') as file:
            json.dump(all_graphs, file, indent=4, ensure_ascii=False)
        return all_graphs

    except Exception as e:
        print(e)

def get_column_segmentations(column_name: str):
    data = DataManager().get_context_schema_data()
    segment_questions = [item["whichSegmentation"]
                         for item in data["data-table"] if item.get("columnName") == column_name]
    segment_questions = segment_questions[0]
    return segment_questions
#
def is_segment_date(segment_name: str) -> bool:
    json_data = DataManager().get_context_schema_data()
    date_questions = [question["columnName"] for question in json_data["data-table"] if
                      question.get("columnName") == segment_name and question.get("columnValueType") == "date"]

    return bool(date_questions)

def pivot_table_for_verbal_question_and_segmentation(df: pd.DataFrame, question_column: str, column_segment: str,
                                                     min_total_count: int):
    pivot_table = df.pivot_table(index=column_segment, columns=question_column, aggfunc={question_column: 'size'},
                                 fill_value=0)
    pivot_table = pivot_table[pivot_table.sum(axis=1) >= int(min_total_count)]
    pivot_table['score'] = (pivot_table[(question_column, 'Positive')] - pivot_table[(question_column, 'Negative')])
    max_row = pivot_table.loc[pivot_table['score'].idxmax()]
    min_row = pivot_table.loc[pivot_table['score'].idxmin()]
    pivot_table.drop('score', axis=1)

    return pivot_table, max_row, min_row

def pivot_table_for_numeric_question_and_segmentation(df: pd.DataFrame, question_column: str, column_segment: str,
                                                      min_total_count: int):
    try:
        pivot_table = df.pivot_table(index=column_segment, columns=question_column, aggfunc='size', fill_value=0)
        # Filter out rows with total count less than min_total_count
        pivot_table = pivot_table[pivot_table.sum(axis=1) >= min_total_count]

        if pivot_table.empty:
            print("Filtered pivot table is empty. No rows meet the minimum total count requirement.")
            return None, None, None

        # Calculate the mean score for each row
        pivot_table['mean'] = pivot_table.apply(lambda row: np.dot(row.index, row) / row.sum(), axis=1)

        min_row = pivot_table.loc[pivot_table['mean'].idxmin()].drop('mean') if not pivot_table.empty else None
        max_row = pivot_table.loc[pivot_table['mean'].idxmax()].drop('mean') if not pivot_table.empty else None

        return pivot_table, max_row, min_row

    except Exception as e:
        print(f"Error: {e}")
        raise Exception(
            f"Column '{question_column}' and segment '{column_segment}' encountered an issue creating pivot_table_for_numeric_question_and_segmentation")
#
def get_pivot_tables_from_question_by_segment(df: pd.DataFrame | None, question_column: str, column_segment: str, question_column_type: str) -> dict:
    df['CustomIndex'] = range(1, len(df) + 1)

    min_total_count = DataManager().get_column_item_value_from_context(column_segment,
                                                                       key_name='minSegmentationGroupToAnalysis')

    if is_segment_date(column_segment):
        df['Year'] = df[column_segment].dt.year
        pivot_table = df.pivot_table(values='CustomIndex', index=['Year'], columns=question_column,
                                     aggfunc={question_column: 'count'}, fill_value=0)
    if question_column_type == 'verbal':
        pivot_table, max_row, min_row = pivot_table_for_verbal_question_and_segmentation(df, question_column,
                                                                                         column_segment,
                                                                                         min_total_count)
    else:
        pivot_table, max_row, min_row = pivot_table_for_numeric_question_and_segmentation(df, question_column,
                                                                                          column_segment,
                                                                                          min_total_count)
    return {"pivot_table": pivot_table, "raw_minimum": min_row, "raw_maximum": max_row}

def get_segmentation_lvl_2(column_name: str):
    lvl_2_segmentation_data = dict()
    lvl_2_segmentation_data[column_name] = dict()
    segments = get_column_segmentations(column_name)
    if segments is None:
        segments = []

    df = DataManager().get_dataframe()
    for seg in segments:
        pivot_anomalies_math = get_pivot_tables_from_question_by_segment(df, column_name, seg,
                                                                         DataManager().get_column_item_value_from_context(
                                                                         column_name, "columnType"))
        if pivot_anomalies_math is None:
            continue
        lvl_2_segmentation_data[column_name][seg] = pivot_anomalies_math
    return lvl_2_segmentation_data

def get_scale_value_by_survey(len: int):
    if len == MAIN_SURVEY_LEN:
        scale = ['Very Low', 'Low', 'Moderately Low', 'Moderate', 'Moderately High', 'High', 'Very High']
        return scale
    elif len == 5:
        scale = ['Very Low', 'Low', 'Moderate', 'High', 'Maximum']
    elif len == SENTIMENT_TWO_OPTIONS:
        scale = ['Negative', 'Positive']
        return scale

def get_scale_mapping(len: int):
    if len == SENTIMENT_TWO_OPTIONS:
        scale_mapping = {
            0: 'Negative',
            1: 'Positive'
        }
    elif len == MAIN_SURVEY_LEN:
        scale_mapping = {
            1: 'Very Low',
            2: 'Low',
            3: 'Moderately Low',
            4: 'Moderate',
            5: 'Moderately High',
            6: 'High',
            7: 'Very High',
        }
    elif len == 5:
        scale_mapping = {
            1: 'Very Low',
            2: 'Low',
            3: 'Moderate',
            4: 'High',
            5: 'Very High',
        }

    return scale_mapping

def is_bar_chart(category_anomaly_name: str):
    data = DataManager().get_context_schema_data()
    result = [question["analysisType"] for question in data["data-table"] if
              question.get("columnName") == category_anomaly_name]
    result = result[0]
    return result == BAR_CHART

def get_scale_color_by_survey(len: int,anomaly_category_df):
    scale_verbal_list = {'Very Low':'rgb(160,220,255)', 'Low':'rgb(98,181,229)', 'Moderately Low':'rgb(0,163,224)', 'Moderate':'rgb(0,118,168)', 'Moderately High':'rgb(0,85,135)', 'High':'rgb(2, 47, 151)', 'Very High':'rgb(4,30,66)'}
    if len ==MAIN_SURVEY_LEN:
        scale_color = []
        for elem in anomaly_category_df['Satisfaction Level']:
            item_index = scale_verbal_list[elem]
            scale_color.append(item_index)
        return scale_color
    if len == MAIN_SURVEY_LEN:
        scale = ['rgb(160,220,255)', 'rgb(98,181,229)', 'rgb(0,163,224)', 'rgb(0,118,168)', 'rgb(0,85,135)',
                 'rgb(2, 47, 151)', 'rgb(4,30,66)']
        return scale
    elif len == 5:
        scale = ['rgb(0,163,224)', 'rgb(0,118,168)', 'rgb(0,85,135)', 'rgb(2, 47, 151)', 'rgb(4,30,66)']
    elif len == SENTIMENT_TWO_OPTIONS:
        scale = ['rgb(194,61,61)', 'rgb(87,210,159)']
        return scale
    
def get_graph(pivot_table, anomaly_df, segment_name:str,column_name:str):
    anomaly_category_name = anomaly_df.name
    anomaly_category_df = pivot_table.loc[[anomaly_category_name]]
    anomaly_category_df = pd.DataFrame(
        anomaly_category_df, index=[anomaly_category_name])

    selected_row = anomaly_category_df.loc[[anomaly_category_name]]
    row_data = selected_row.values[0].tolist()
    # Drop the last item is mean
    row_data = row_data[:-1]
    anomaly_category_df = pd.DataFrame({'Satisfaction Level': range(1, len(row_data) + 1), 'Count': row_data})

    if not column_name.startswith('sentiment_'):
        anomaly_category_df = anomaly_category_df[anomaly_category_df['Count'] != 0] 

    scale_mapping = get_scale_mapping(len(row_data))

    if column_name.startswith('sentiment_'):
        satisfaction_level_order = [scale_mapping[i] for i in range(len(row_data))]
        anomaly_category_df['Satisfaction Level'] = satisfaction_level_order
        
    else:
        satisfaction_level_order = [scale_mapping[i] for i in range(len(row_data), 0, -1)]
        anomaly_category_df['Satisfaction Level'] = anomaly_category_df['Satisfaction Level'].map(get_scale_mapping(len(row_data)))

    color_scale = get_scale_value_by_survey(len(row_data))

    if is_bar_chart(segment_name):
        # figure=px.bar(
        #     anomaly_category_df,
        #     x='Satisfaction Level',
        #     y='Count',
        #     color='Satisfaction Level',
        #     color_discrete_sequence=color_scale,
        #     category_orders={'Satisfaction Level': satisfaction_level_order},
        #     labels={'Satisfaction Level': 'Satisfaction Level','Count': 'Count'},
        #     title=anomaly_category_name,
        # )
        figure = {
            'type': 'bar',
            'data': anomaly_category_df.to_dict('records'),
            'color': 'Satisfaction Level',
            'color_discrete_sequence': color_scale,
            'category_orders': {'Satisfaction Level': satisfaction_level_order},
            'labels':{'Satisfaction Level': 'Satisfaction Level','Count': 'Count'},
            'title':anomaly_category_name,
            "layout": {
                "title": column_name
            }            
        }
        return figure
    else:
        scale_value = get_scale_value_by_survey(len(row_data))
        color_scale = get_scale_color_by_survey(len(row_data),anomaly_category_df)
        # figure=px.pie(
        #     anomaly_category_df,
        #     names='Satisfaction Level',
        #     values='Count',
        #     title=str(anomaly_category_name),
        #     labels={'Satisfaction Level': 'Satisfaction Level',
        #             'Percentage': 'Percentage'},
        #     color_discrete_sequence=color_scale,
        #     category_orders={'Satisfaction Level': scale_value},
        # )
    
        # figure.update_layout(
        #     showlegend=False
        # )

        # figure.update_traces(marker=dict(line=dict(color='white', width=2)))
        # figure.update_layout(
        #     title={
        #         'text': f"<b>{anomaly_category_name}</b>",
        #         'x': 0.5,
        #         'y': 0.95,
        #         'xanchor': 'center',
        #         'yanchor': 'top',
        #         'font': {'size': 16, 'color': 'black'},
        #     },
        #     plot_bgcolor='#ffffff',
        #     paper_bgcolor='#ffffff'
        # )
        figure = {
            'type': 'pie',
            'data': anomaly_category_df.to_dict('records'),
            'names': 'Satisfaction Level',
            'values': 'Count',
            'title': str(anomaly_category_name),
            'labels': {'Satisfaction Level': 'Satisfaction Level',
                    'Percentage': 'Percentage'},
            'color_discrete_sequence':color_scale,
            'category_orders': {'Satisfaction Level': scale_value},
            "layout": {
                "title": column_name
            }
        }
        return figure
    
def get_segmentation_lvl_2_insight_ultra(column_name: str, segment_name: str) -> str:

    lvl_2_segmentation_insight = insights[column_name].get("lvl_2_segmentations_insight_ultra_summary", {})
    
    if segment_name in lvl_2_segmentation_insight and lvl_2_segmentation_insight[segment_name] is not None:
        lvl_2_segmentation_insight = lvl_2_segmentation_insight[segment_name]
    else:
        lvl_2_segmentation_insight = "N/A"
    return lvl_2_segmentation_insight

def get_chart_name(column_name: str):
    data = DataManager().get_context_schema_data()
    chart_name = [item["analysisType"]
                  for item in data["data-table"] if item.get("columnName") == column_name]
    chart_name = chart_name[0]
    return chart_name