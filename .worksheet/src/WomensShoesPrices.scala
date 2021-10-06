import org.apache.spark
object WomensShoesPrices {;import org.scalaide.worksheet.runtime.library.WorksheetSupport._; def main(args: Array[String])=$execute{;$skip(155); 
  val df = spark.read.format("csv").option("header","true").load("file:///F:\\big-data/test.csv").show();System.out.println("""df  : <error> = """ + $show(df ))}
}
