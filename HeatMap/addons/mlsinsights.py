
mapper = {'CL': 'ClosePrice',
          'LP': 'ListPrice',
          'PS': 'PriceSqft',
          'NT': 'NumberOfTransactions'}


def run(postalcode, span, span_filter):
    from pymongo import MongoClient
    import numpy

    db_client = MongoClient("52.91.122.15", 27017)

    Data = list(db_client.MLSLite.listing_unique.find({"PostalCode": {"$eq": postalcode},
                                                       "CloseDate": {"$gte": span_filter}
                                                       },
                                                      {"CloseDate": 1,
                                                       "ListPrice": 1,
                                                       "ClosePrice": 1,
                                                       "price_sqft": 1,
                                                       "StandardStatus": 1,
                                                       "LivingArea": 1
                                                       }))
    n_trans = len(Data)
    if n_trans > 0:
        try:
            ListPrice_Closed = [Data[m]['ListPrice'] for m in range(len(Data)) if Data[m]['StandardStatus'] == "Sold"]
            ListPrice_Active = [Data[m]['ListPrice'] for m in range(len(Data)) if Data[m]['StandardStatus'] == "Active"]
            ClosePrice_Closed = [Data[j]['ClosePrice'] for j in range(len(Data)) if Data[j]['StandardStatus'] == "Sold"]
            PriceSqft_Closed = [Data[k]['price_sqft'] for k in range(len(Data)) if Data[k]['StandardStatus'] == "Sold"]
            PriceSqft_Active = [(Data[k]['ListPrice']) / (Data[k]['LivingArea']) for k in range(len(Data)) if Data[k]['StandardStatus'] == "Active" and Data[k]['ListPrice'] is not None and Data[k]['LivingArea'] is not None and Data[k]['LivingArea'] != 0]
            MedianListPrice_Closed = round(numpy.median([x for x in ListPrice_Closed if x is not None]), 2)
            MedianListPrice_Active = round(numpy.median([x for x in ListPrice_Active if x is not None]), 2)
            MedianClosePrice_Closed = round(numpy.median([x for x in ClosePrice_Closed if x is not None]), 2)
            MedianPriceSqft_Closed = round(numpy.median([x for x in PriceSqft_Closed if x is not None]), 2)
            MedianPriceSqft_Active = round(numpy.median(PriceSqft_Active), 2)
        except:
            MedianListPrice_Closed = 0
            MedianListPrice_Active = 0
            MedianClosePrice_Closed = 0
            MedianPriceSqft_Closed = 0
            MedianPriceSqft_Active = 0
            pass
    else:
        n_trans = 0
        MedianListPrice_Closed = 0
        MedianListPrice_Active = 0
        MedianClosePrice_Closed = 0
        MedianPriceSqft_Closed = 0
        MedianPriceSqft_Active = 0
    dict_Closed = {}
    dict_Closed['ListPrice'] = MedianListPrice_Closed
    dict_Closed['ClosePrice'] = MedianClosePrice_Closed
    dict_Closed['PriceSqft'] = MedianPriceSqft_Closed
    dict_Closed['NumberOfTransactions'] = n_trans
    dict_Active = {}
    dict_Active['ListPrice'] = MedianListPrice_Active
    dict_Active['ClosePrice'] = None
    dict_Active['PriceSqft'] = MedianPriceSqft_Active
    dict_Active['NumberOfTransactions'] = n_trans
    dict_result = {}
    dict_result['Closed_Listings'] = dict_Closed
    dict_result['Active_Listings'] = dict_Active
    return dict_result
