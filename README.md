package newsalary
import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
object highestsalary {
  
  def main(args:Array[String]):Unit={
    
    val conf = new SparkConf().setAppName("first").setMaster("local[*]").set("spark.driver.hostname","localhost")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
   val data = Seq(
  ("Alice", 60000, "HR"),
  ("Bob", 80000, "IT"),
  ("Cathy", 75000, "HR"),
  ("David", 90000, "IT"),
  ("Eve", 85000, "Finance")
).toDF("name", "salary", "department")

// Group by department and find the highest salary in each department
val maxSalaryPerDept = data.groupBy("department").agg(max("salary").as("max_salary"))

// Show the result
maxSalaryPerDept.show()
    
  }
}
