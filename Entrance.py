import sys
import logging
from typing import List
from pyspark.sql import SparkSession
from HotcellAnalysis import HotcellAnalysis
from HotzoneAnalysis import HotzoneAnalysis

class Entrance:
    # @staticmethod
    def main(args:List[str]):
        spark = SparkSession \
            .builder \
            .appName("CSE511-HotspotAnalysis-Mucheng") \
            .getOrCreate() # YOU NEED TO CHANGE YOUR GROUP NAME
            # .config("spark.security.manager", "false") \
            # .master("local[*]") \
        Entrance.paramsParser(spark, args)

    # @staticmethod
    def paramsParser(spark: SparkSession, args:List[str]):
        paramOffset = 1
        currentQueryParams = ""
        currentQueryName = ""
        currentQueryIdx = -1

        while paramOffset <= len(args):
            # Notice to param, `analysis` should not be in the sub folder path
            if paramOffset == len(args) or "analysis" in args[paramOffset].lower():
                # Turn in the previous query
                if currentQueryIdx != -1:
                    Entrance.queryLoader(spark, currentQueryName, currentQueryParams, args[0] + str(currentQueryIdx))

                # Start a new query call
                if paramOffset == len(args):
                    return

                currentQueryName = args[paramOffset]
                currentQueryParams = ""
                currentQueryIdx += 1
            else:
                # Keep appending query parameters
                currentQueryParams += args[paramOffset] + " "
            paramOffset += 1

    @staticmethod
    def queryLoader(spark: SparkSession, queryName: str, queryParams: str, outputPath: str):
        queryParam = queryParams.split()
        if queryName.lower() == "hotcellanalysis":
            if len(queryParam) != 1:
                raise IndexError(f"[CSE511] Query {queryName} needs 1 parameter but you entered {len(queryParam)}")
            hotCellDf = HotcellAnalysis.runHotcellAnalysis(spark, queryParam[0]).limit(50)
            hotCellDf.write.mode("overwrite").csv(outputPath)
        elif queryName.lower() == "hotzoneanalysis":
            if len(queryParam) != 2:
                raise IndexError(f"[CSE511] Query {queryName} needs 2 parameters but you entered {len(queryParam)}")
            hotZoneDf = HotzoneAnalysis.runHotZoneAnalysis(spark, queryParam[0], queryParam[1])
            hotZoneDf.write.mode("overwrite").csv(outputPath)

if __name__ == "__main__":
    if len(sys.argv) > 1: 
        ARGs = sys.argv[1:]
    else:
        ## Test case CMD to Run all via Spark CLI
        ## spark-submit --py-files HotzoneAnalysis.py Entrance.py test/output/ hotzoneanalysis src/resources/point_hotzone.csv src/resources/zone-hotzone.csv hotcellanalysis src/resources/yellow_trip_sample_100000.csv

        ## Test case CMD for HotcellAnalysis via Spark CLI
        ## spark-submit --py-files HotcellAnalysis.py Entrance.py test/output/ hotcellanalysis src/resources/yellow_trip_sample_100000.csv
        
        ## Test case CMD for HotzoneAnalysis via Spark CLI
        ## spark-submit --py-files HotzoneAnalysis.py Entrance.py test/output/ hotzoneanalysis src/resources/point_hotzone.csv src/resources/zone-hotzone.csv

        ## Python script
        ARGs = ['test/output/', 'HotcellAnalysis', 'src/resources/yellow_trip_sample_100000.csv']
        # ARGs = ['test/output/', 'HotzoneAnalysis', 'src/resources/point_hotzone.csv', 'src/resources/zone-hotzone.csv']
    Entrance.main(ARGs)