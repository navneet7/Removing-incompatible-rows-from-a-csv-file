import org.apache.spark._
import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import spark.implicits._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode

object removeIncompatibleRows extends App{

  val conf: SparkConf = new SparkConf().setAppName("RemoveIncompatibleRows").setMaster("local")
  val sc: SparkContext = new SparkContext(conf)  
  val spark =SparkSession.builder().master("local").appName("Remove Incompatible Rows")
  .config("spark.master", "local")
  .getOrCreate()

  
  val filePath = yourBasePath+"/sample_file.csv"
  
  //Setting log level to ERROR
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  //Reading csv as textFile
  val text = sc.textFile(filePath)

  //Converting textFile to Dataframe
  val df = text.toDF()
  
  //Adding a new column which is having an array after splitting entire row by ','
  val df1 = df.withColumn("Cols", split(df("Value"),","))
  
  //Adding a new column which will get the length of column
  val df2 = df1.withColumn("Len", size($"Cols")).drop($"Cols")
  
  //Filter out such rows which are having other than 3 columns
  val df3 = df2.filter($"Len" === 3).drop($"Len").withColumn("DummyCol", lit(1))
  
  //Converting above dataframe to RDD
  val result = df3.rdd.map(_.mkString(","))
  
  //Getting the header of the RDD
  val headerColumns = result.first().split(",").to[List]
  
  //Creating Schema
  val schema = StructType(
                  Seq(
                    StructField(name = "Company", dataType = StringType, nullable = false),
                    StructField(name = "Person", dataType = StringType, nullable = false),
                    StructField(name = "Sales", dataType = StringType, nullable = false)
                  )
                )
   
   //This tells how many columns to pick out of a single row
   def row(line: List[String]): Row = Row(line(0), line(1), line(2))
  
   //Converting RDD[String] to RDD[row] and dropping 1st row which contains header
   val data = result
               .mapPartitionsWithIndex((index, element) => if (index == 0) element.drop(1) else element)
               .map(_.split(",").to[List]).map(row)
               
   //Creating dataframe using RDD and schema
   val df_final = spark.createDataFrame(data, schema)
   
   df_final.show
   
}
