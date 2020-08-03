package com.analysis.unicomModel

import com.alibaba.fastjson.{JSONArray, JSONObject}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DataTypes, StringType, StructField, StructType}

import scala.collection.mutable
import scala.collection.mutable.HashMap
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks
import collection.JavaConverters._




object AggSQL {

  case class record(uid: String, attribute: String, times: String) {

  }


  class AggEncodeUDAF extends org.apache.spark.sql.expressions.UserDefinedAggregateFunction {

    override def inputSchema: StructType = StructType(StructField("String", StringType):: StructField("String", StringType) :: Nil);
    override def bufferSchema: StructType = StructType(
//        StructField("ArrayString", DataTypes.createArrayType(DataTypes.StringType,false)) ::
//        StructField("ArrayString", DataTypes.createArrayType(DataTypes.StringType, false)) :: Nil
      StructField("Map", DataTypes.createMapType(StringType, DataTypes.createArrayType(StringType))) :: Nil
    )

    override def dataType: DataType = StringType

    override def deterministic: Boolean = true

    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer.update(0,  HashMap[String,ArrayBuffer[String]]())
    }

    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      var tmp1: HashMap[String,ArrayBuffer[String]] = HashMap[String,ArrayBuffer[String]]()
      tmp1 ++= buffer.getAs[HashMap[String,ArrayBuffer[String]]](0)
      var key: String = input.getAs[String](0)
      var value: String = input.getAs[String](1)

      var valueArrBuf: ArrayBuffer[String] = ArrayBuffer()
      valueArrBuf ++= value.split("\\|")
      var finalValues: ArrayBuffer[String] = tmp1.getOrElse(key, ArrayBuffer[String]())
      finalValues ++= valueArrBuf;
      finalValues = finalValues.sortWith{
        case (a,b) => {
          a.split("-")(0).toInt.compare(
            b.split(("-"))(0).toInt
          ) > 0
        }
      }
      tmp1.put(key, finalValues);
      buffer.update(0, tmp1)
    }

    def judge(arrayBuffer: Seq[String]): String ={
      /**
        * 这里是判断 home 的逻辑 分为以下几个点
        *  1. 存在超过20个小时的 则必定为Home_1_{天数}_{总天数}
        *  2. 0～5 点 超过2个小时在的天数 天数过半 倾向于Home_2_{天数}_{总天数}
        *  3. 存在0～5点 超过2小时 Home_3_{天数}_{总天数}
        *  4. 计算0～8 和 13～18点 超过2小时的停留天数，多的为home_4_{凌晨天数}_{下午天数}_{总天数},
        *                                               work_4_{凌晨天数}_{下午天数}_{总天数}
        *  5. 以上都无法判断，将根据他所在的时间0～8,20~24 为Home_5_{home5天数}_{work5天数}_{总天数}
        *                           8～20 为Work_5_{home5天数}_{work5天数}_{总天数}
        *                           哪个个多为哪个
        *
        * */
      var days = mutable.HashSet[Int]()
      var day2Time = HashMap[Int, ArrayBuffer[Tuple2[Int,Int]]]()
      var result = ""
      val loop = new Breaks;

      loop.breakable {
        var total_home = 0
        for (time: String <- arrayBuffer) {
          var start = time.split("-")(0).toInt
          var end = time.split("-")(1).toInt
          var day = start / 48
          days.add(day)
          start = start % 48
          end = end - day * 48
          if (end - start > 40) {
            result = "home_1"
            total_home += 1
          }

          var times = day2Time.getOrElse(day, ArrayBuffer[Tuple2[Int,Int]]())
          times += Tuple2[Int,Int](start, end)
          day2Time.put(day, times)

        }

        if (total_home > 0) {
          result = result + "_" + total_home.toString
          loop.break()
        }
        // 过滤用户在 0 ～ 5 点 产生超过2个小时
        var zero2five = day2Time.filter{
          case (key, times) => {
              var sumSlots = 0
              for(time <- times) {
                if (time._1 < 10){
                  var end : Int = Math.min(10, time._2)
                  sumSlots += (end - time._1)
                }
              }
            sumSlots >= 4
          }
        }

        if (zero2five.keys.size >= days.size/2.0 ){
          result = "home_2"
        } else if (zero2five.keys.size > 0){
          result = "home_3"
        }
        // 返回 home_2_{天数}_{总天数} 或 home_3_{天数}_{总天数}
        if(result.size != 0) {
          result = result + "_" + zero2five.size.toString
          loop.break()
        }
        // 过滤用户在 0 ～ 8 点 产生超过2个小时
        var zero2Eight = day2Time.filter{
          case (key, times) => {
            var sumSlots = 0
            for(time <- times) {
              if (time._1 < 16){
                var end : Int = Math.min(16, time._2)
                sumSlots += (end - time._1)
              }
            }
            sumSlots >= 4
          }
        }
        // 过滤用户在 13 ~ 16 点 产生超过2个小时
        var thirteen2sixteen = day2Time.filter{
          case (key, times) => {
            var sumSlots = 0
            for(time <- times) {
              if (time._2 >= 26 && time._1 <= 36){
                var start: Int = Math.max(26, time._1)
                var end : Int = Math.min(36, time._2)
                sumSlots += (end - start)
              }
            }
            sumSlots >= 4
          }
        }

        if (thirteen2sixteen.keys.size > zero2Eight.keys.size){
          result = "work_4" + "_" + zero2Eight.keys.size.toString + "_" + thirteen2sixteen.size.toString
        } else if (thirteen2sixteen.keys.size < zero2Eight.keys.size){
          result = "home_4" + "_" + zero2Eight.keys.size.toString + "_" + thirteen2sixteen.size.toString
        } else {
          // 等于
          var workHome = day2Time.mapValues{
            case (times) => {
              var workSum = 0;
              var homeSum = 0;
              for (time <- times) {
                // 计算 非08～20点 所在时间集合
                if(time._2 > 40 || time._2 <= 16){

                  if(time._2 <= 16) {
                    // 08 点前结束 只能为 home
                    homeSum += time._2 - time._1
                  } else {
                    homeSum += time._2 - 40
                    workSum += Math.max(0, 40 - time._1)
                  }

                } else {
                  var workTime = time._2 - Math.max(time._1, 16)
                  workSum += workTime
                  homeSum += time._2 - time._1 - workTime
                 }
              }
              (homeSum, workSum)
            }
          }
          val totalhome = workHome.filter(x => x._2._1 > x._2._2)
          val totalwork = workHome.filter(x => x._2._1 < x._2._2)

          if (totalhome.size < totalwork.size){
            result = "work_5_" + totalhome.size.toString + "_" + totalwork.size.toString
          } else if (totalhome.size > totalwork.size) {
            result = "home_5_" + totalhome.size.toString + "_" + totalwork.size.toString
          } else {
            result = "unknow_5_" + totalhome.size.toString + "_" + totalwork.size.toString
          }
        }
      }

      result = result + "_" + days.size.toString()
      result

    }

    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      var tmp1:HashMap[String,ArrayBuffer[String]] =  HashMap[String,ArrayBuffer[String]]()
      tmp1 ++= buffer1.getAs[HashMap[String,ArrayBuffer[String]]](0)
      var in: Map[String,ArrayBuffer[String]] = buffer2.getAs[Map[String,ArrayBuffer[String]]](0)
      for (key <- in.keys) {
        // contains key in buffer1: conflicts
        if (tmp1.contains(key)) {
          var valueArrBuf: ArrayBuffer[String] = tmp1.getOrElse(key, ArrayBuffer[String]())
          valueArrBuf ++= in.getOrElse(key, ArrayBuffer[String]())

          // sort the result
          valueArrBuf = valueArrBuf.sortWith{
            case (a,b) => {
              a.split("-")(0).toInt.compare(
                b.split(("-"))(0).toInt
              ) > 0
            }
          }
          tmp1.put(key, valueArrBuf)
        } else {
          // no conflicts
          tmp1.put(key, in.getOrElse(key, ArrayBuffer[String]()))
        }
      }
      buffer1.update(0, tmp1)
    }

    override def evaluate(buffer: Row): Any = {
      var resultMap: HashMap[String, Seq[String]] = HashMap[String, Seq[String]]()
      resultMap ++= buffer.getAs[Map[String,Seq[String]]](0)
      var jsArray = new JSONArray()
      var mapped:mutable.Iterable[JSONObject] = resultMap.map{
        case (key, values) => {

          var times = values.mkString("|")
          var attribute = judge(values)
          var json:JSONObject = new JSONObject()
          var jsarray: JSONArray = new JSONArray()
          jsarray.add(attribute)
          jsarray.add(times)
          json.put(key, jsarray)
          json
        }
      }

      for (r:JSONObject <- mapped) {
        jsArray.add(r)
      }



      var json = new JSONObject()
      json.put("num", mapped.size)
      json.put("detail", jsArray);
      json.toJSONString;

    }
  }

  def main(args: Array[String]): Unit = {
    val input = "hdfs://namenode/home/data/result/300M_30MIN/2019/201906/*"
    val outpath = "hdfs://namenode/home/data/agg/300M_30MIN/2019/201906/"
    val conf = new SparkConf().setAppName("EncodeSQL").setMaster("spark://spark-master:7077");
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate();
    val sc = spark.sparkContext;

    val rdd = sc.textFile(input);
    val structSchema: StructType = StructType(
      List(
        StructField("uid", StringType, true),
        StructField("location", StringType, true),
        StructField("times", StringType, true)
      ));
    spark.udf.register("agg", new AggEncodeUDAF)

    import spark.implicits._
    val mapped = rdd.map{
      case line => {
        var items = line.split(",")
        (items(0), items(1), items(2), items(3))
      }
    }.map(x => Row(x._1.trim(), x._2.trim()+"-"+ x._3.trim(), x._4.trim()))




    var dataframe = spark.createDataFrame(mapped,structSchema)
    dataframe.createOrReplaceTempView("stop_point")
    var df: DataFrame = spark.sql("select uid, agg(location,times) from stop_point group by uid")
    df = df.sort(new Column("uid"))
    df = df.repartition(16, new Column("uid"))
    df.write.csv(outpath)


  }
}
