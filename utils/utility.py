import copy
def get_graphs_data_wrapper(insights_data):
    graphs_data_wrapper = []
    is_contain_plot_chart = False

    for key, inner_dict in insights_data.items():
        question_data = {'question': key}
        for inner_key, value in inner_dict.items():

            if inner_key == 'column_insight_ultra_summary':
                question_data[inner_key] = value
            elif inner_key == 'plot_chart':
                is_contain_plot_chart = True
                question_data[inner_key] = copy.deepcopy(value)
        graphs_data_wrapper.append(question_data)
    
    if is_contain_plot_chart:
      return graphs_data_wrapper
    else:
        return []