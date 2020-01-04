package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
{
  // Load the original data from a data source
  var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
  pickupInfo.createOrReplaceTempView("nyctaxitrips")
  pickupInfo.show()

  // Assign cell coordinates based on pickup points
  spark.udf.register("CalculateX",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 0)
    )))
  spark.udf.register("CalculateY",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 1)
    )))
  spark.udf.register("CalculateZ",(pickupTime: String)=>((
    HotcellUtils.CalculateCoordinate(pickupTime, 2)
    )))
  pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
  var newCoordinateName = Seq("x", "y", "z")
  pickupInfo = pickupInfo.toDF(newCoordinateName:_*)
  pickupInfo.show()

  // Define the min and max of x, y, z
  val minX = -74.50/HotcellUtils.coordinateStep
  val maxX = -73.70/HotcellUtils.coordinateStep
  val minY = 40.50/HotcellUtils.coordinateStep
  val maxY = 40.90/HotcellUtils.coordinateStep
  val minZ = 1
  val maxZ = 31
  val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)


  // YOU NEED TO CHANGE THIS PART
    
    pickupInfo.createOrReplaceTempView("pickupInfoCell")
    pickupInfo = spark.sql("select x,y,z from pickupInfoCell where x>= " + minX + " and x<= " + maxX + " and y>= " + minY + " and y<= " + maxY + " and z>= " + minZ + " and z<= " + maxZ + " order by z,y,x")
    pickupInfo.createOrReplaceTempView("selectedCellVals")
    // pickupInfo.show()

    pickupInfo = spark.sql("select x, y, z, count(*) as hotCells from selectedCellVals group by x, y, z order by z,y,x")
    pickupInfo.createOrReplaceTempView("selectedCellHotness")
    // pickupInfo.show()

    val sumOfSelectedCcells = spark.sql("select SUM(hotCells) as sumHotCells from selectedCellHotness")
    sumOfSelectedCcells.createOrReplaceTempView("sumOfSelectedCcells")
    // sumOfSelectedCcells.show()

    val mean = (sumOfSelectedCcells.first().getLong(0).toDouble / numCells.toDouble).toDouble
    //val meanSQL = spark.sql("select AVG(hotCells) as sumHotCells from selectedCellHotness")
    //meanSQL.createOrReplaceTempView("meanSQL")
    //val mean = meanSQL.first().getDouble(0).toDouble
    // println(mean)

    //spark.udf.register("squared", (inputX: Int) => (((inputX*inputX).toDouble)))

    val sumOfSquares = spark.sql("select sum(hotCells * hotCells) as sumOfSquares from selectedCellHotness")
    sumOfSquares.createOrReplaceTempView("sumOfSquares")
        // sumOfSquares.show()

    val standardDeviation = scala.math.sqrt(((sumOfSquares.first().getLong(0).toDouble / numCells.toDouble) - (mean.toDouble * mean.toDouble))).toDouble
    // println(mean)
    //val standardDeviationSQL = spark.sql("select STDDEV(hotCells) as standardDevi from selectedCellHotness")
    //standardDeviationSQL.createOrReplaceTempView("standardDeviationSQL")
    // val standardDeviation = standardDeviationSQL.first().getDouble(0).toDouble

  spark.udf.register("adjacentCells", (inputX: Int, inputY: Int, inputZ: Int, minX: Int, maxX: Int, minY: Int, maxY: Int, minZ: Int, maxZ: Int) => ((HotcellUtils.calculateAdjacentCells(inputX, inputY, in$
  val adjacentCells = spark.sql("select sch1.x as x, sch1.y as y, sch1.z as z, "
                + "COUNT(*) as adjacentCellCount,"
                + "sum(sch2.hotCells) as sumHotCells "
                + "from selectedCellHotness as sch1, selectedCellHotness as sch2 "
                + "where (sch1.x = sch2.x+1 or sch1.x = sch2.x or sch1.x = sch2.x-1) "
                + "and (sch1.y = sch2.y+1 or sch1.y = sch2.y or sch1.y = sch2.y-1) "
                + "and (sch1.z = sch2.z+1 or sch1.z = sch2.z or sch1.z = sch2.z-1) "
                + "group by sch1.z, sch1.y, sch1.x "
                + "order by sch1.z, sch1.y, sch1.x")
        adjacentCells.createOrReplaceTempView("adjacentCells")
  // adjacentCells.show()

  spark.udf.register("zScore", (adjacentCellCount: Int, sumHotCells: Int, numCells: Int, x: Int, y: Int, z: Int, mean: Double, standardDeviation: Double) => ((HotcellUtils.calculateZScore(adjacentCellCou$

  pickupInfo = spark.sql("select zScore(adjacentCellCount, sumHotCells, "+ numCells + ", x, y, z," + mean + ", " + standardDeviation + ") as getisOrdStatistic, x, y, z from adjacentCells order by getisOr$
  pickupInfo.createOrReplaceTempView("zScore")

pickupInfo = spark.sql("select x, y, z, getisOrdStatistic from zScore")
  pickupInfo.createOrReplaceTempView("finalPickupInfo")


  return pickupInfo // YOU NEED TO CHANGE THIS PART
}
}