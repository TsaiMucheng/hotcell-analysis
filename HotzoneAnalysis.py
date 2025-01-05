from HotzoneUtils import HotzoneUtils
from pyspark.sql import DataFrame, functions, SparkSession
from pyspark.sql.types import BooleanType
import logging

class HotzoneAnalysis:
    logging.getLogger("org.spark_project").setLevel(logging.WARNING)
    logging.getLogger("org.apache").setLevel(logging.WARNING)
    logging.getLogger("akka").setLevel(logging.WARNING)
    logging.getLogger("com").setLevel(logging.WARNING)

    @staticmethod
    def runHotZoneAnalysis(spark: SparkSession, pointPath: str, rectanglePath: str) -> DataFrame:
        pointDf = spark.read.format("csv").option("delimiter",";").option("header","false").load(pointPath)
        pointDf.createOrReplaceTempView("point")

        # Parse point data formats
        spark.udf.register("trim", lambda string: string.replace("(", "").replace(")", ""))
        pointDf = spark.sql("select trim(_c5) as _c5 from point")
        pointDf.createOrReplaceTempView("point")

        # Load rectangle data
        rectangleDf = spark.read.csv(rectanglePath, sep='\t', header=False)
        rectangleDf.createOrReplaceTempView("rectangle")

        # Join two datasets
        spark.udf.register("ST_Contains", functions.udf(HotzoneUtils.ST_Contains, BooleanType()))
        joinDf = spark.sql("select rectangle._c0 as rectangle, point._c5 as point from rectangle,point where ST_Contains(rectangle._c0,point._c5)")
        joinDf.createOrReplaceTempView("joinResult")

        # Group by rectangle and count the points in each rectangle
        resultDf = spark.sql("select rectangle, count(*) as count from joinResult group by rectangle order by rectangle")
        resultDf.createOrReplaceTempView("HotzoneDf")
        return resultDf


# if __name__ == "__main__":
#     spark = SparkSession \
#             .builder \
#             .appName("CSE511-HotspotAnalysis-HotzoneAnalysis") \
#             .config("spark.hadoop.native.lib", "E:\\Hadoop\\bin") \
#             .config("spark.security.manager", "false") \
#             .master("local[*]") \
#             .getOrCreate()
#     pointPath = 'src/resources/point_hotzone.csv'
#     rectanglePath = 'src/resources/zone-hotzone.csv'
#     df = HotzoneAnalysis.runHotZoneAnalysis(spark, pointPath, rectanglePath)
#     df.show(truncate=False)
