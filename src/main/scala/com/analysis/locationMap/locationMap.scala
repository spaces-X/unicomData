package com.analysis.locationMap

import java.security.InvalidParameterException
import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.{SparkConf, SparkContext}

object locationMap {
  /**
   * 联通数据处理，将基站映射成位置信息
   */

  /**
   * 从hdfs 读取后简单处理联通数据
   * 将基站的id 作为key
   *
   * @param x
   * @return
   */
  def parseLocationData(x: String): (String, String) = {
    var items = x.split("\\|")
    var is4G: Boolean = items(5).equals("4")
    var id = ""
    if (is4G) {
      id = items(1)
    }
    else {
      id = items(0) + items(1)
    }
    var location = items(2) + "," + items(3)
    (id, location)
  }

  def filter(data: (String, String),lng1: Double, lat1: Double, lng2: Double, lat2:Double): Boolean = {
    var value = data._2
    var splits : Array[String] = value.split(",")
    var lng = splits(0).toDouble
    var lat = splits(1).toDouble
    if (lng < lng1 || lng > lng2 || lat < lat2 || lat > lat1)
      false
    else
      true

  }

  /**
   * 处理轨迹data 判断 是否是4G基站，生成基站id 并且作为key
   *
   * @param x
   * @return
   */
  def parseData(x: String): (String, String) = {
    var items = x.split(",")
    var is4G: Boolean = items(3).equals("4")
    var dataId = ""
    if (is4G) {
      dataId = items(4)
    } else {
      dataId = items(3) + items(4)
    }
    (dataId, x)
  }

  /**
   * 将基站id 坐标的RDD 和 数据基站id的Rdd 进行Join 关联
   *
   * @param keyValue
   * @return
   */
  def parsedJoined(keyValue: (String, String)): (String, String) = {
    var x = keyValue._1
    var y = keyValue._2
    var items = x.split(",")
    // id, starttime, endtime, lng, lat, timelengh, phonenumber
    var toReturn = items(0) + "," + items(2) + "," + items(1) + "," + y + "," + items(5) + "," + items(6)
    (items(0), toReturn)
  }

  /**
   * 去除错误数据
   *
   * @param x
   * @return
   */
  def judgeData(x: String): Boolean = {

    var judgeLength = 0
    try {
      var items = x.split(",")
      var is4G = items(3).equals("4")
    }
    catch {
      case exLength: java.lang.ArrayIndexOutOfBoundsException => {
        judgeLength = 1
      }
    }
    if (judgeLength == 1)
      false
    else
      true

  }

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      throw new InvalidParameterException("Input must be a Date like yyyymmdd");
    }
    val conf = new SparkConf().setAppName("com/analysis/locationMap").setMaster("spark://master01:7077")
    val sc = new SparkContext(conf)
    var locationData = sc.textFile("hdfs://dcoshdfs/private_data/useryjj/jizhan20190617.txt")
    var locationMapped = locationData.repartition(1).map(x => parseLocationData(x))
    var start = args(0)
    var month = start.substring(0,6)
    var sdf = new SimpleDateFormat("yyyyMMdd")
    var dstart = sdf.parse(start)
    var dend = sdf.parse(args(1))
    var calstart = Calendar.getInstance()
    var calend = Calendar.getInstance()
//    calstart.setTime(dstart)
//    calstart.add(Calendar.MONTH, 1)


    calstart.setTime(dstart)
    calend.setTime(dend)
    while (! calstart.after(calend)) {
      var currentDate = calstart.getTime()
      var currentDString = sdf.format(currentDate)
      var path = "hdfs://dcoshdfs/private_data/userrx/ImsiPath/2019/" + month + "/" + currentDString + "/" + "imsipath" + currentDString + ".csv"
      var outputPath = "hdfs://dcoshdfs/private_data/useryjj/ImsiPath/2019/" + month + "/" + currentDString
      var data = sc.textFile(path).filter(x => judgeData(x))
      var mapped = data.map(x => parseData(x))
      var joined = mapped.join(locationMapped)
      var joinMap = joined.map(x => parsedJoined(x._2))
      var out = joinMap.groupByKey(8).map(x => (x._1,x._2.toArray.sortBy(item => item.split(",")(1))))
        .flatMap(x => x._2.iterator).map( x=> x)
      out.saveAsTextFile(outputPath)
      calstart.add(Calendar.DAY_OF_MONTH, 1)
    }

  }


}
