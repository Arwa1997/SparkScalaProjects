import org.apache.spark
object WomensShoesPrices {
  val df = spark.read.format("csv").option("header","true").load("file:///F:\\big-data/test.csv").show() //> Welcome to the Scala worksheet
}