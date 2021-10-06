

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._
    
object DataFrames {
  
  case class Shoes(brand:String, color:String, priceMax:Double, priceMin:Double, isSale:Boolean, size: String)
  
  def mapper(line:String): Shoes = {
    val fields = line.split(',')  

    
    val shoe:Shoes = Shoes(fields(4), fields(5), fields(6).toDouble, fields(7).toDouble, fields(8).toBoolean, fields(9))
    return shoe
  }
  
  /** Our main function where the action happens */
  def main(args: Array[String]) {
    
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Use new SparkSession interface in Spark 2.0
    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()
    
    // Convert our csv file to a DataSet, using our Person case
    // class to infer the schema.
    import spark.implicits._
    import org.apache.spark.sql.functions._
    val lines = spark.sparkContext.textFile("/Datafiniti_Womens_Shoes.csv")
    val first_line = lines.first()
    val shoes =  lines.filter(row => row != first_line).map(mapper)
    //shoes.foreach(println)
    val shoeSchema = shoes.toDS().cache()
    shoeSchema.printSchema()
    //shoeSchema.show()
    //using SQL

    val lowerSchema = shoeSchema.withColumn("brand", lower(col("brand")));
    val preppedSchema = shoeSchema.filter((col("size").cast("int").isNotNull) and (shoeSchema("brand") =!= "unbranded"));
    preppedSchema.createOrReplaceTempView("women_shoes")
    
    println("Top 10 Most Expensive Shoe Brands")
    val expensiveShoes = spark.sql("SELECT DISTINCT brand, MAX(priceMax) as price FROM women_shoes WHERE brand <> 'unbranded' GROUP BY brand ORDER BY price DESC limit 10")
    expensiveShoes.show()
    
    println("Top 10 Cheapest Shoe Brands")
    val cheapestShoes = spark.sql("SELECT DISTINCT brand, MAX(priceMax) as price FROM women_shoes WHERE brand != 'unbranded' GROUP BY brand ORDER BY price ASC limit 10")
    cheapestShoes.show()
    
    println("Average Original Shoe Price per Brand")
    val avgPrice = spark.sql("SELECT brand, ROUND(AVG(priceMax),2) as price FROM women_shoes GROUP BY brand ORDER BY price Desc")
    avgPrice.show()
    
    println("Most Popular Shoe Color in Order (Most Popular to Least Popular")
    val popularColor = spark.sql("SELECT LOWER(color) as color, MAX(colorCount) as maxColorCount FROM colorCountBrand GROUP BY color ORDER BY maxColorCount DESC")
    popularColor.show()
    val popularColorBrand = spark.sql("SELECT brand, color FROM colorCountBrand WHERE colorCount = (select max(colorCount) from colorCountBrand GROUP BY brand)")
    popularColorBrand.show()
    
    println("Most Expensive Shoe Color")
    val expensiveColor = spark.sql("SELECT brand, color, max(priceMax) as colorCount FROM women_shoes GROUP BY brand, color")
    expensiveColor.show()
    
    println("Brands with the Biggest Discounts")
    val maxDiscount = spark.sql("SELECT LOWER(brand) as brand, round(max(priceMax-priceMin),2) as discount FROM women_shoes WHERE isSale = 'true' GROUP BY brand ORDER BY discount DESC")
    maxDiscount.show()
    
    println("Shoes Prices per Size")
    val sizePrices = spark.sql("SELECT size, round(AVG(priceMax),2) as averagePrice FROM women_shoes GROUP BY size ORDER BY averagePrice DESC")
    sizePrices.show()
    
    println("Size Range per Brand")
    val sizeRange = spark.sql("SELECT brand, COUNT(DISTINCT size) as sizeRange FROM women_shoes GROUP BY brand ORDER BY sizeRange DESC")
    sizeRange.show()
    
   
    spark.stop()
  }
}