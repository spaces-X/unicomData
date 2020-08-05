package com.analysis.unicomModel

import java.security.InvalidParameterException
import java.text.{DecimalFormat, SimpleDateFormat}
import java.util.Date

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.sql.types.{ArrayType, DataType, DataTypes, DoubleType, IntegerType, StringType, StructField, StructType}

import scala.collection.mutable.ArrayBuffer


object EncodeSQL {

  class EncodeUDAF() extends UserDefinedAggregateFunction {
    override def inputSchema: StructType = StructType(StructField("String", StringType):: Nil);

    override def bufferSchema: StructType = StructType(StructField("ArrayString", DataTypes.createArrayType(DataTypes.StringType,false)) ::Nil);

    override def dataType: DataType = {
      StringType
    }

    override def deterministic: Boolean = {
      true;
    }

    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer.update(0, ArrayBuffer[String]());
      //      buffer.update(0, scala.collection.mutable.ArrayBuffer.empty[String])
    }

    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      //      if (input.isNullAt(0)){
      //        return ;
      //      }
      var tmp: ArrayBuffer[String] = ArrayBuffer[String]()
      tmp ++= buffer.getAs[ArrayBuffer[String]](0)

      tmp += input.getString(0)
      buffer.update(0, tmp)
    }

    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      //      buffer1(0) = buffer1.getString(0) + "-" + buffer2.getString(0);

      var tmp:ArrayBuffer[String] = ArrayBuffer[String]()
      tmp ++= buffer1.getAs[ArrayBuffer[String]](0)
      tmp ++= buffer2.getAs[ArrayBuffer[String]](0)
      buffer1.update(0,tmp)
    }

    override def evaluate(buffer: Row): String = {
      var res_set: ArrayBuffer[String] = ArrayBuffer[String]()
      res_set ++= buffer.getAs[ArrayBuffer[String]](0);
      var sb = new StringBuilder();
      var flag = false;

      for (ele:String <- res_set) {
        if (flag) {
          sb.append("|")
        }
        sb.append(ele)
        flag = true;
      }
      sb.toString()
    }
  }



  def buildDataFrame(rdd: RDD[Row], structType: StructType, sparkSession: SparkSession): DataFrame = {
    val dataFrame = sparkSession.createDataFrame(rdd, structType);
    return dataFrame;
  }

  def Parse2RDD(line: String, start: Date): (String, String, String, String) = {
    var items = line.split(",")
    var df = new DecimalFormat("0.0000")
    val id = items(0)
    val lng = items(1).toDouble
    val lat = items(2).toDouble
    val dateStart = new Date(items(3).replace("CST",""))
    val dateEnd = new Date(items(4).replace("CST",""))
    var start_dela = dateStart.getTime() - start.getTime()
    var end_dela = dateEnd.getTime() - start.getTime()

    var start_num  = (start_dela / (30*60*1000)).toString()
    var end_num = (end_dela / (30*60*1000)).toString()

    var timezone = start_num + "-" + end_num
    (id, df.format(lng), df.format(lat), timezone)
  }



  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("EncodeSQL-Move").setMaster("spark://master01:7077");
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate();
    val sc: SparkContext = spark.sparkContext;
    val structSchema: StructType = StructType(
      List(
        StructField("uid", StringType, true),
        StructField("lng", StringType, true),
        StructField("lat", StringType, true),
        StructField("timezone", StringType, true)
      ));
    var sdf = new SimpleDateFormat("yyyyMMddHHmmss");
    var start = args(0)
    var distance = args(1)
    if ( !distance.equals("300M") && !distance.equals("600M")) {
      throw new InvalidParameterException("must be 300M or 600M")
    }
    var year = start.substring(0,4)
    var month = start.substring(0,6)
    var sdate = sdf.parse(start)
    //var path = "hdfs://dcoshdfs/private_data/useryjj/1Cluster/600M_30MIN/2019/201902/201902*/AllStop/*"

    val path = "hdfs://dcoshdfs/private_data/useryjj/1Cluster/"+ distance + "_30MIN/" + year + "/" + month + "/" + month + "*/" + "AllStop/*"
    val outpath = "hdfs://dcoshdfs/private_data/useryjj/result/" + distance + "_30MIN/" + year + "/" + month + "/"
    val rdd = sc.textFile(path)
    import spark.implicits._
    val mapped = rdd.filter(x =>x.split(",")(0)!="id").map(x => Parse2RDD(x,sdate))
      .map(x => (x._1.trim(), x._2.trim(), x._3.trim(), x._4.trim()))
    var dataFrame: DataFrame = mapped.toDF("uid","lng","lat","timezone");

//    var dataFrame : DataFrame = buildDataFrame(mapped, structSchema, spark);
    dataFrame.createOrReplaceTempView("people");
    spark.udf.register("encoding",new EncodeUDAF);

    var df: DataFrame = spark.sql("select uid,lng,lat, encoding(timezone)  from people group by uid,lng,lat");
    df = df.toDF("uid","lng","lat","coding")
    df = df.sort(new Column("uid"))
    // 对coding 列进行单独操作 比如 末尾 添加标记
//    var my = df.printSchema()
//    implicit val mapenc = org.apache.spark.sql.Encoders.kryo[(String, String)]
//    var df1 = df.map { row => {
//      val coding = row.getAs[String]("encode");
//      (coding, coding + "hello")
//      }
//
//    }
//    df1.toDF("a","b")


    df = df.repartition(64, new Column("uid"))
    df.write.csv(outpath)


  }

}