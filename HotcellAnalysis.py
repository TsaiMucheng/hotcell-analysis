from HotcellUtils import HotcellUtils
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import IntegerType, DoubleType
from math import sqrt
import logging

class HotcellAnalysis:
    logging.getLogger("org.spark_project").setLevel(logging.WARNING)
    logging.getLogger("org.apache").setLevel(logging.WARNING)
    logging.getLogger("akka").setLevel(logging.WARNING)
    logging.getLogger("com").setLevel(logging.WARNING)
    
    @staticmethod
    def runHotcellAnalysis(spark: SparkSession, pointPath: str) -> DataFrame:
        # Return top 50 hot cells
        limit = 50
        # Load the original data from a data source
        pickupInfo = spark.read.format("csv").option("delimiter", ";").option("header", "false").load(pointPath)
        pickupInfo.createOrReplaceTempView("nyctaxitrips")
        # pickupInfo.show()

        # Assign cell coordinates based on pickup points
        spark.udf.register("CalculateX", lambda pickupPoint: HotcellUtils.CalculateCoordinate(pickupPoint, 0), IntegerType())
        spark.udf.register("CalculateY", lambda pickupPoint: HotcellUtils.CalculateCoordinate(pickupPoint, 1), IntegerType())
        spark.udf.register("CalculateZ", lambda pickupTime: HotcellUtils.CalculateCoordinate(pickupTime, 2), IntegerType())
        
        pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
        newCoordinateName = ["x", "y", "z"]
        pickupInfo = pickupInfo.toDF(*newCoordinateName)
        # pickupInfo.show()

        # Define the min and max of x, y, z
        minX = -74.50 / HotcellUtils.coordinateStep
        maxX = -73.70/ HotcellUtils.coordinateStep
        minY = 40.50 / HotcellUtils.coordinateStep
        maxY = 40.90 / HotcellUtils.coordinateStep
        minZ, maxZ = 1, 31
        numCells = (maxX - minX + 1) * (maxY - minY + 1) * (maxZ - minZ + 1)
        
        pickupInfo.createOrReplaceTempView("pickupInfo")
        hotCellPoints = spark.sql(f"""
            select x, y, z, Count(*) as pickupCount 
            from pickupInfo 
            where x>={minX} and x<={maxX} and 
                y>={minY} and y<= {maxY} and 
                z>={minZ} and z<={maxZ} 
            group by z, y, x
        """)
        hotCellPoints.createOrReplaceTempView("hotCellPoints")

        # XjSquare
        XjSquare = spark.sql("select sum(pickupCount) as sumXj, sum(pickupCount * pickupCount) as sqsumXj from hotCellPoints")
        XjSquare.createOrReplaceTempView("XjSquare")
        sumXj = float(XjSquare.first()[0])
        sqsumXj = float(XjSquare.first()[1])

        # Mean & STD of x
        X_mean = sumXj / numCells
        std_x = sqrt((sqsumXj / numCells) - (X_mean ** 2))

        # Calculate neighbors for each cell
        spark.udf.register("neighbors", \
            lambda X, Y, Z: HotcellUtils.calculateNeighbors( \
                X, Y, Z, minX, minY, minZ, maxX, maxY, maxZ), 
            IntegerType())
        neighborCells = spark.sql(f"""
            SELECT 
                neighbors(hc1.x, hc1.y, hc1.z) AS neighborCellCount,
                hc1.x AS x, 
                hc1.y AS y, 
                hc1.z AS z, 
                SUM(hc2.pickupCount) AS WijXj
            FROM hotCellPoints AS hc1, hotCellPoints AS hc2
            WHERE 
                (hc2.x = hc1.x-1 OR hc2.x = hc1.x OR hc2.x = hc1.x+1) 
                AND (hc2.y = hc1.y-1 OR hc2.y = hc1.y OR hc2.y = hc1.y+1)
                AND (hc2.z = hc1.z-1 OR hc2.z = hc1.z OR hc2.z = hc1.z+1)
            GROUP BY hc1.z, hc1.y, hc1.x
        """)
        neighborCells.createOrReplaceTempView("neighborCells")

        spark.udf.register("GetisOrdStat", \
            lambda WijXj, neighborCellCount: HotcellUtils.calculateGScore( \
                WijXj, neighborCellCount, numCells, X_mean, std_x),
            DoubleType())

        GetisOrdStatCells = spark.sql(f"""
            SELECT 
                GetisOrdStat(WijXj, neighborCellCount) AS GetisOrdStat, 
                x, y, z 
            FROM neighborCells 
            ORDER BY GetisOrdStat DESC
        """)
        GetisOrdStatCells.createOrReplaceTempView("GetisOrdStatCells")

        # GetisOrdStat score
        resultDf = spark.sql("SELECT x, y, z FROM GetisOrdStatCells").limit(limit)
        resultDf.createOrReplaceTempView("HotcellDf")

        return resultDf


# if __name__ == "__main__":
#     spark = SparkSession \
#             .builder \
#             .appName("CSE511-HotspotAnalysis-HotcellAnalysis") \
#             .config("spark.hadoop.native.lib", "E:\\Hadoop\\bin") \
#             .config("spark.security.manager", "false") \
#             .master("local[*]") \
#             .getOrCreate()
#     df = HotcellAnalysis.runHotcellAnalysis(spark, 'src/resources/yellow_trip_sample_100000.csv')
#     df.show(truncate=False)