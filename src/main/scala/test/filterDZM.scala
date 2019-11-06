package test


import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.ListBuffer
import java.util.{Calendar, Date}
import java.text.SimpleDateFormat

object filterDZM {

  def filterData(x: String): Boolean = {

    try {
      val format=new SimpleDateFormat("yyyyMMddHHmmss")
      var start_date = format.parse(x.split(",")(1))
      var end_date = format.parse(x.split(",")(2))
      val lng = x.split(",")(3).toDouble
      val lat = x.split(",")(4).toDouble
      // !start_date.before(end_date) ||
      if (  lat<39.91093 || lat>39.9692 || lng<116.38197 || lng>116.46448) {
        return false
      }

      return true
    }
    catch {
      case e:Exception=>{
        return false
      }
    }
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("firstCluster").setMaster("spark://master01:7077")
    val sc = new SparkContext(conf)

    var start = "20190610"
    var end = "20190615"
    var month = start.substring(0,6)
    var sdf = new SimpleDateFormat("yyyyMMdd")
    var dstart = sdf.parse(start)
    var dend = sdf.parse(end)
    var calstart = Calendar.getInstance()
    var calend = Calendar.getInstance()
    calstart.setTime(dstart)
    calend.setTime(dend)
    while (calstart.before(calend)) {
      var currentDate = calstart.getTime()
      var currentDString = sdf.format(currentDate)
      var inPath = "hdfs://dcoshdfs/private_data/useryjj/ImsiPath/2019/" + month + "/" + currentDString + "/*"
      var outputPathDir = "hdfs://dcoshdfs/private_data/useryjj/DZMImsiPath/2019/" + month + "/" + currentDString + "/"
      var data = sc.textFile(inPath).filter(x => filterData(x)).map(x=>(x.split(",")(0),x)).groupByKey(8).flatMap(x=>x._2).saveAsTextFile(outputPathDir)
      calstart.add(Calendar.DAY_OF_MONTH, 1)
    }






  }

}
