
mapper = {'IE': 'iEstimate'}


def run(postalcode, span, span_filter):
    from pymongo import MongoClient
    import numpy
    client = MongoClient("52.91.122.15", 27017)
    db = client.iestimate
    Data = list(db['rets_standardized_data_predictions_2016-07-31_01'].find({"PostalCode": {"$eq": postalcode}, "CloseDate": {"$gte": span_filter}},
                                                                            {"iEstimate": 1, "StandardStatus": 1}))
    n_trans = len(Data)
    if n_trans > 0:
        try:
            iEstimate_Closed = [Data[k]['iEstimate'] for k in range(len(Data)) if Data[k]['StandardStatus'] == "Closed"]
            iEstimate_Active = [Data[k]['iEstimate'] for k in range(len(Data)) if Data[k]['StandardStatus'] == "Active"]
            MedianiEstimate_Closed = round(numpy.median([x for x in iEstimate_Closed if x is not None]), 2)
            MedianiEstimate_Active = round(numpy.median([x for x in iEstimate_Active if x is not None]), 2)
        except:
            MedianiEstimate_Closed = None
            MedianiEstimate_Active = None
    else:
        n_trans = 0
        MedianiEstimate_Closed = None
        MedianiEstimate_Active = None
    dict_Closed = {}
    dict_Closed['iEstimate'] = MedianiEstimate_Closed
    dict_Active = {}
    dict_Active['iEstimate'] = MedianiEstimate_Active
    dict_result = {}
    dict_result['Closed_Listings'] = dict_Closed
    dict_result['Active_Listings'] = dict_Active
    return dict_result
