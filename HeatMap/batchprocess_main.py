from cognub.jobmanager import ConfigDistributer, DistributedJob
from pymongo import MongoClient
import pandas as pd
import numpy
import datetime
import os
import importlib
from os import listdir
from os.path import isfile
import math
from cognub.botmail.recipients import propmix_recepients

db_client = MongoClient("52.91.122.15", 27017)


class sched_HM():
    @staticmethod
    def sched(StateOrProvince_):
        def monthdelta(date, delta):
            '''
            Function to get a past date from a string of date in yyyy-mm-dd format
            '''
            m, y = (date.month + delta) % 12, date.year + ((date.month) + delta - 1) // 12
            if not m:
                m = 12
            d = min(date.day, [31, 29 if y % 4 == 0 and not y % 400 == 0 else 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31][m - 1])
            return date.replace(day=d, month=m, year=y)

        AllZips = list(db_client.Heat_Map.distinct_state_county_zip.distinct("_id.PostalCode"))
        OurZips = list(db_client.MLSLite.listing_unique.distinct("PostalCode", {"StateOrProvince": StateOrProvince_}))
        Zips_final = list(set(OurZips).intersection(AllZips))
        StateCountyData = list(db_client.Heat_Map.distinct_state_county_zip.find({}, {"_id.PostalCode": 1, "_id.State": 1, "_id.County": 1}))
        SC_data = []
        for value in StateCountyData:
            SC_data.append(value["_id"])
        StateCountyDatadf = pd.DataFrame(SC_data)  # @UndefinedVariable
        file_dir = os.path.dirname(os.path.realpath(__file__))
        addon_names = [_file for _file in listdir(os.path.join(file_dir, "addons")) if isfile(os.path.join(file_dir, "addons", _file)) and _file.endswith((".py",)) and _file != "__init__.py"]
        addons = map(importlib.import_module, map(lambda x: "addons.%s" % (x.replace(".py", "")), addon_names))

        main_splits = ["Active_Listings", "Closed_Listings"]

        def MedianData(postalcode="34953", spans=["3M", "6M", "1Y", "3Y", "5Y"]):
            '''Function to calculate the median of parameters w.r.t. each Post'''
            now = datetime.datetime.now()
            main_results = dict([(split, dict([(span, {}) for span in spans])) for split in main_splits])
            n_trans = 1
            for span in spans:
                if n_trans > 0:
                    if span[1] == "Y":
                        date_filter = monthdelta(now, -int(span[0]) * 12).strftime("%Y-%m-%d")
                    else:
                        date_filter = monthdelta(now, -int(span[0])).strftime("%Y-%m-%d")
                    sub_results = dict([(split, {}) for split in main_splits])
                    for result in map(lambda x: x.run(postalcode, span, date_filter), addons):
                        for split in main_splits:
                            sub_results[split].update(result[split])
                for split in main_splits:
                    main_results[split][span].update(sub_results[split])
            return(main_results)

        '''------------------------------------------------------------------------------------------------'''
        '''Inserting the calculated median values to the respective PostalCode'''
        '''------------------------------------------------------------------------------------------------'''

        spans = ["3M", "6M", "1Y", "3Y", "5Y"]
        PostalCodes = Zips_final
        for i in range(len(PostalCodes)):
            StateName = list(StateCountyDatadf.State[StateCountyDatadf.PostalCode == PostalCodes[i]])
            CountyName = list(StateCountyDatadf.County[StateCountyDatadf.PostalCode == PostalCodes[i]])
            data = MedianData(PostalCodes[i], spans)
            record = {"_id": {"PostalCode": PostalCodes[i], "State": StateName[0], "County": CountyName[0]}, "StateOrProvince": StateOrProvince_}
            record.update(data)
            db_client.Heat_Map.HeatMapMediansZipWise_mlslite.update({"_id": {"PostalCode": PostalCodes[i], "State": StateName[0], "County": CountyName[0]}}, record, upsert=True)

        '''------------------------------------------------------------------------------------------------'''
        '''State-wise aggregation and calculation of median for each time period'''
        '''------------------------------------------------------------------------------------------------'''

        aggregate_group = {"$group": {"_id": "$_id.State"}}
        aggregate_group["$group"].update(dict([("{0}_{1}_{2}".format(split, span, short_label), {"$push": "${0}.{1}.{2}".format(split, span, label)}) for span in spans for addon in addons for short_label, label in addon.mapper.items() for split in main_splits]))
        StateData = list(db_client.Heat_Map.HeatMapMediansZipWise_mlslite.aggregate([aggregate_group]))
        for state_record in StateData:
            record = {"_id": {"State": state_record["_id"]}, "StateOrProvince": StateOrProvince_}
            split_map = dict([(split, {}) for split in main_splits])
            record.update(split_map)
            for split in main_splits:
                record[split].update(dict([(span, dict([(label, round(numpy.median([value for value in state_record["{0}_{1}_{2}".format(split, span, short_label)] if value is not None]), 2) if short_label[0] != "N" else sum([value for value in state_record["{0}_{1}_{2}".format(split, span, short_label)] if value is not None])) for addon in addons for short_label, label in addon.mapper.items()])) for span in spans]))
            db_client.Heat_Map.HeatMapMediansStateWise_mlslite.update({"_id": {"State": state_record['_id']}}, record, upsert=True)

        '''------------------------------------------------------------------------------------------------'''
        '''County-wise aggregation and calculation of median for each time period'''
        '''------------------------------------------------------------------------------------------------'''

        aggregate_group = {"$group": {"_id": "$_id.County"}}
        aggregate_group["$group"].update(dict([("{0}_{1}_{2}".format(split, span, short_label), {"$push": "${0}.{1}.{2}".format(split, span, label)}) for span in spans for addon in addons for short_label, label in addon.mapper.items() for split in main_splits]))
        CountyData = list(db_client.Heat_Map.HeatMapMediansZipWise_mlslite.aggregate([aggregate_group]))
        for county_record in CountyData:
            record = {"_id": {"County": county_record["_id"]}, "StateOrProvince": StateOrProvince_}
            split_map = dict([(split, {}) for split in main_splits])
            record.update(split_map)
            for split in main_splits:
                record[split].update(dict([(span, dict([(label, round(numpy.median([value for value in county_record["{0}_{1}_{2}".format(split, span, short_label)] if value is not None]), 2) if short_label[0] != "N" else sum([value for value in county_record["{0}_{1}_{2}".format(split, span, short_label)] if value is not None])) for addon in addons for short_label, label in addon.mapper.items()])) for span in spans]))
            db_client.Heat_Map.HeatMapMediansCountyWise_mlslite.update({"_id": {"County": county_record['_id']}}, record, upsert=True)


class HeatMapConfigDistributer(ConfigDistributer):
    def distribution_algorithm(self):
        StateOrProvince_ = list(db_client.MLSLite.listing_unique.distinct("StateOrProvince"))
        n = int(math.ceil(float(len(StateOrProvince_)) / float(self.get_jobscount())))
        return [StateOrProvince_[i:i + n] for i in xrange(0, len(StateOrProvince_), n)]


class HeatMapJob(DistributedJob):
    zkroot = "/cognubapps/propmix/heatmap_scheduler"
    zkhost = "hdp-master.propmix.io:2181"
    distributer = HeatMapConfigDistributer
    job_name = "Heat map job."
    job_description = "Heat map data generator for different insights."
    mail_recepients = propmix_recepients + developers

    def __jobinit__(self):
        pass

    def run(self, StateOrProvince_):
        sched_HM.sched(StateOrProvince_)
        print "node %d config %s" % (self.initializer.node_id, str(StateOrProvince_))

job = HeatMapJob("SampleJob")
job.start()
