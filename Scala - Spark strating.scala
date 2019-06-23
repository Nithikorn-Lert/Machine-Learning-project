import org.apache.spark._ // import pyspark as sp

object Spark_context_session {
  def main(args:Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("First Application")
    val sc = new SparkContext(conf) //sc = sp.SparkContext(appName = 'myapp' */

    val spark = org.apache.spark.sql.SparkSession.builder
      .master("local")
      .appName("Spark CSV Reader")
      .getOrCreate;

//    import spark.implicits._sgsdg

//    import org.apache.spark.sql._
//    import org.apache.spark.sql.Row
//    import org.apache.spark.sql.types.{StructType,StructField,StringType};

    val statesDF = spark.read
      .format("csv")
      .option("header", "true") //first line in file has headers
      .option("mode", "DROPMALFORMED")
      .load("C:/Users/New/Downloads/Data science in Action/Data set for spark/Scala-and-Spark-for-Big-Data-Analytics-master/data/data/statesPopulation.csv")

      statesDF.show(5)
    println("***************************************")

    statesDF.createOrReplaceTempView("states")
    spark.sql("select * from states").show()

    println("***************************************")



  }
}
