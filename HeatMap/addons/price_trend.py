
mapper = {'PT': 'Price_trend'}

span_ls_map = {'3M': 'last_3_month_avg_l_s_ratio',
               '6M': 'last_6_month_avg_l_s_ratio',
               '1Y': 'last_1_year_avg_l_s_ratio',
               '3Y': 'last_3_year_avg_l_s_ratio',
               '5Y': 'last_5_year_avg_l_s_ratio'}


def run(postalcode, span, span_filter):
    import datetime
    import numpy as np
    from cognub.propmixapi import PriceTrendAPI
    iprice = PriceTrendAPI()
    trend = iprice.get_trend(postalcode)
    try:
        forecast = iprice.get_future_trend(trend, date_list=np.asarray([datetime.datetime.now(), datetime.datetime.now() + datetime.timedelta(days=30)]), span=span)
        forecast_Closed = forecast[1][0]
        forecast_Active = (trend[span_ls_map[span]] / 100) * forecast_Closed
    except:
        forecast_Closed = None
        forecast_Active = None
    dict_Closed = {}
    dict_Closed['Price_trend'] = forecast_Closed
    dict_Active = {}
    dict_Active['Price_trend'] = forecast_Active
    dict_result = {}
    dict_result['Closed_Listings'] = dict_Closed
    dict_result['Active_Listings'] = dict_Active
    return dict_result
