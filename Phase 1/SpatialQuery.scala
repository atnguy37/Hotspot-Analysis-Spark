package cse512

import org.apache.spark.sql.SparkSession

object SpatialQuery extends App{
  def runRangeQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>((ST_Contains(queryRectangle, pointString))))

    val resultDf = spark.sql("select * from point where ST_Contains('"+arg2+"',point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runRangeJoinQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    val rectangleDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    rectangleDf.createOrReplaceTempView("rectangle")

    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>((ST_Contains(queryRectangle, pointString))))

    val resultDf = spark.sql("select * from rectangle,point where ST_Contains(rectangle._c0,point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>((ST_Within(pointString1, pointString2, distance))))

    val resultDf = spark.sql("select * from point where ST_Within(point._c0,'"+arg2+"',"+arg3+")")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceJoinQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point1")

    val pointDf2 = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    pointDf2.createOrReplaceTempView("point2")

    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>((ST_Within(pointString1, pointString2, distance))))
    val resultDf = spark.sql("select * from point1 p1, point2 p2 where ST_Within(p1._c0, p2._c0, "+arg3+")")
    resultDf.show()

    return resultDf.count()
  }

  def ST_Contains(queryRectangle:String, pointString:String) : Boolean = {
    val pt = pointString.split(",")
    val pt_x = pt(0).trim().toDouble
    val pt_y = pt(1).trim().toDouble
    
    val rect = queryRectangle.split(",")
    val rect_x_1 = rect(0).trim().toDouble
    val rect_y_1 = rect(1).trim().toDouble
    val rect_x_2 = rect(2).trim().toDouble
    val rect_y_2 = rect(3).trim().toDouble
    
    var min_x = List(rect_x_1, rect_x_2).min
    var max_x = List(rect_x_1, rect_x_2).max
    
    var min_y = List(rect_y_1, rect_y_2).min
    var max_y = List(rect_y_1, rect_y_2).max
  
    
    if(pt_x >= min_x && pt_x <= max_x && pt_y >= min_y && pt_y <= max_y) {
      return true
    } else {
      return false
    }
  }

  def ST_Within(pointString1:String, pointString2:String, distance:Double) : Boolean = {
    val pt1 = pointString1.split(",")
    val pt_x_1 = pt1(0).trim().toDouble
    val pt_y_1 = pt1(1).trim().toDouble
    
    val pt2 = pointString2.split(",")
    val pt_x_2 = pt2(0).trim().toDouble
    val pt_y_2 = pt2(1).trim().toDouble
    
    val d = scala.math.sqrt(scala.math.pow((pt_x_1 - pt_x_2), 2) + scala.math.pow((pt_y_1 - pt_y_2), 2))
    
    return d <= distance
  }
}
