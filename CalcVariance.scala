import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.collection.mutable.Map


object CalcVariance {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("Calculate Variance").setMaster("local")
    val sc = new SparkContext(conf)
    val temp = sc.textFile("./source/data2.txt").map(line=>line.split(" ")).flatMap(x=>{
      var i = 0
      var map = Map[Int, Double]()
      while(i<x.length){
        map+=i->x(i).toDouble
        i = i+1
      }
      map
    })
    val mean = temp.mapValues(i=>(i,1)).reduceByKey((x,y)=>(x._1+y._1,x._2+y._2)).mapValues(x=>x._1/x._2)
    .sortBy(x=>x, true)
    
    val variance = temp.mapValues(i=>(i*i,1)).reduceByKey((x,y)=>(x._1+y._1,x._2+y._2)).join(mean)
    .mapValues(x=>(x._1._1 - x._1._2*x._2*x._2)/(x._1._2-1)).sortBy(x=>x, true)
    
    val variance2 = temp.join(mean).mapValues(x=>((x._1-x._2)*(x._1-x._2),1))
    .reduceByKey((x,y)=>(x._1+y._1,x._2+y._2)).mapValues(x=>x._1/(x._2-1)).sortBy(x=>x, true)
    
    mean.foreach(print)
    println
    variance.foreach(print)
    println
    variance2.foreach(print)
  }
}
