import pandas as pd


def get_num_of_duplicate_records(df: pd.DataFrame)->int:
    unique_df = df.drop_duplicates()
    num_duplicates = len(df) - len(unique_df)
    return num_duplicates

def get_num_of_survey_participants(df: pd.DataFrame)->int:
    return len(df) -get_num_of_duplicate_records(df)
    

def get_uncompleted_answers(df: pd.DataFrame)->int:
    empty_rows = df[df.apply(lambda row: any(
        cell == '' or pd.isnull(cell) for cell in row), axis=1)]
    if not empty_rows.empty:
        return len(empty_rows.index)
    return 0


def filter_df_columns_from_scheme_excluding_key(context_scheme, key):
    if context_scheme is None:
        return []
    relevant_cols = [item["columnName"] for item in context_scheme["data-table"]
                     if item.get("isRelevantAnalysis") and item.get("columnType") != key]
    return relevant_cols

def get_all_segments(context_scheme):
    if context_scheme is None:
        return []
    segment_questions = [item["columnName"]
                         for item in context_scheme["data-table"] if item.get("columnType") == "segment"]
    return segment_questions