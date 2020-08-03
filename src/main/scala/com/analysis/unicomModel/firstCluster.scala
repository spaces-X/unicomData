package com.analysis.unicomModel


import java.security.InvalidParameterException
import java.text.SimpleDateFormat

import java.text.DecimalFormat
import scala.collection.mutable.ListBuffer
import java.util.{Calendar, Date}

import org.apache.hadoop.yarn.util.Records
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.io.Source
import scala.util.control.Breaks




object firstCluster {


// 不能用全局变量
//  var calendar = Calendar.getInstance()
//  val format=new SimpleDateFormat("yyyyMMddHHmmss")
  /**
    * 天级别（短期）停留点
    * @param plng
    * @param plat
    * @param pdStart
    * @param pdEnd
    * @param pattri
    */
  class stopPoint(plng:Double,plat:Double,pdStart:java.util.Date,pdEnd:java.util.Date,pattri:String) extends Serializable {
    var lng=plng
    var lat=plat
    var dStart=pdStart
    var dEnd=pdEnd
    var attri=pattri

    override def toString: String = {
      var df = new DecimalFormat("0.0000")

      df.format(lng)+","+df.format(lat)+","+dStart+","+dEnd+","+attri
    }
  }

  /**
    * 抽象对象代表一条记录所需要的信息
    * @param id
    * @param sdate
    * @param edate
    * @param lng
    * @param lat
    */

  case class cellData(id:String,sdate: Date,edate: Date ,var lng:Double, var lat:Double) {
    override def toString: String = {
      var df = new DecimalFormat("0.0000")
      id + "," + sdate.toString+ "," + edate.toString + "," + df.format(lng) + "," + df.format(lat)
    }
  }

  /**
    * 移动点，目前我们不是很关注这些但是留着以后用
    * @param lng
    * @param lat
    * @param smove
    * @param emove
    */
  case class movePoint(lng:Double,lat:Double,smove:Date, emove:Date){
    override def toString: String = {
      var df = new DecimalFormat("0.0000")
      df.format(lng)+","+df.format(lat)+","+ smove.toString() + "," + emove.toString()
    }
  }

  /**
    * 读取数据并映射成（id, cellData）的格式
    * @param line
    * @return
    */
  def parseData(line: String): (String, cellData) = {
    var items = line.split(",")
    var uuid = items(0)
    var format = new SimpleDateFormat("yyyyMMddHHmmss")
    var sdate = format.parse(items(1))
    var edate = format.parse(items(2))
    var lng = items(3).toDouble
    var df = new DecimalFormat("0.0000")
    df.format(lng)
    var lat = items(4).toDouble
    df.format(lat)

    (uuid,cellData(uuid,sdate,edate,lng,lat))
  }


  /**
    * 去除错误数据
    *
    * @param x
    * @return
    */
  def judgeData(x: String): Boolean = {

    try {
      var format = new SimpleDateFormat("yyyyMMddHHmmss")
      var start_date = format.parse(x.split(",")(1))
      var end_date = format.parse(x.split(",")(2))
      val lng = x.split(",")(3).toDouble
      val lat = x.split(",")(4).toDouble
      // !start_date.before(end_date) ||
      if ( !(start_date.before(end_date)) || lat<39.4333 || lat>41.05 || lng<115.41667 || lng>117.5) {
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

  /**
    * groupby 用户id, 提取日大于n个小时的用户
    * @param data
    * @param n (hour)
    * @return
    */
  def activeData(data:(String, Iterable[cellData]), n:Int) :Boolean = {
    var cellDatas = data._2
    var DaySet = mutable.Set[Int]()
    var activeTime = 0L
    for (data <- cellDatas) {
      activeTime += (data.edate.getTime - data.sdate.getTime)
    }
    activeTime >= n*3600*1000
  }

  /**
    * 距离计算
    * 单位：米
    * @param lng1
    * @param lat1
    * @param lng2
    * @param lat2
    * @return
    */
  def calcDis(lng1:Double,lat1:Double,lng2:Double,lat2:Double):Double={
    val r=6371393 //地球半径
    val con=Math.PI/180.0 //用于计算的常量
    val radLat1: Double = lat1*con
    val radLat2: Double = lat2*con
    val a: Double = radLat1 - radLat2
    val b: Double = lng1*con - lng2*con
    var s: Double = 2.0 * Math.asin(Math.sqrt(Math.pow(Math.sin(a / 2.0), 2.0) + Math.cos(radLat1) * Math.cos(radLat2) * Math.pow(Math.sin(b / 2.0), 2.0)))
    s = s * r
    s = Math.round(s * 10000.0) / 10000.0
    s
  }

  /**
    * 为TDBSCAN算法找邻居
    *
    * @param index_center
    * @param df
    * @param spatial_threshold
    * @param temporal_threshold
    × @param min_neighbors : 包含自己的最小邻居数目
    * @return
    */
  def retrieve_neighborsT(index_center:Int, df:Array[(Int,Date,Date,Double,Double,Array[Int])],
                          spatial_threshold:Double, temporal_threshold:Long, min_neighbors:Int)={
    val res=new scala.collection.mutable.ArrayBuffer[(Int,Date,Date,Double,Double,Array[Int])]
    val empty=new scala.collection.mutable.ArrayBuffer[(Int,Date,Date,Double,Double,Array[Int])]
    var forward=index_center
    var backward=index_center
    val loop1=new Breaks
    val loop2=new Breaks
    loop1.breakable{
      while(backward>0)
      {
        backward-=1
        if(calcDis(df(backward)._4,df(backward)._5,df(index_center)._4,df(index_center)._5)<=spatial_threshold)
        {
          res+=df(backward)
        }
        else
          loop1.break()
      }
    }
    loop2.breakable{
      while(forward<df.length-1)
      {
        forward+=1
        if(calcDis(df(forward)._4,df(forward)._5,df(index_center)._4,df(index_center)._5)<=spatial_threshold)
        {
          res+=df(forward)
        }
        else
          loop2.break()
      }
    }
    res+=df(index_center)
    res.sortBy(x=>x._2)
    if(res(res.length-1)._3.getTime-res(0)._2.getTime<temporal_threshold)
      empty
    if( res.length < min_neighbors && (res.map( x => x._3.getTime()-x._2.getTime()).sum)/(1000*10*60) < 3) // 不关心邻居的个数 但是关心邻居的时长（上面）
      empty
    else
      res.filter(x=>x._1!=index_center)
  }

  /**
    * TDBSCAN 算法
    * @param line
    * @param spatial_threshold
    * @param temporal_threshold
    * @param min_neighbors
    * @return
    */
  def tDbscanAndJudgeAttri(line:(String,Iterable[cellData]),spatial_threshold:Double,
                           temporal_threshold:Long,min_neighbors:Int) ={
    var index= -1
    var clusterIndex=0
    var stack=new mutable.Stack[Int]()
    /*
      -1表示为未标记
      0表示离群点
      1....n表示簇集的id
     */
    val df=line._2.map { x =>
      val kind = Array(-1)
      index+=1
      (index,x.sdate,x.edate,x.lng,x.lat,kind)
    }.toArray

    for(data<-df)
    {
      if(data._6(0) == -1) {
        var neighbor = retrieve_neighborsT(data._1, df, spatial_threshold, temporal_threshold, min_neighbors)
        //        if(neighbor.length<min_neighbors)
        //          data._6(0)=0
        if(neighbor.length < 1) {
          if (data._3.getTime() - data._2.getTime() >= temporal_threshold){
            clusterIndex += 1
            data._6(0) = clusterIndex
          }
          else {
            data._6(0) = 0
          }

        }
        else{
          //          neighbor.remove(data._1)
          clusterIndex+=1
          data._6(0)=clusterIndex

          for(dataNeighbor<-neighbor)
          {
            dataNeighbor._6(0)=clusterIndex
            stack.push(dataNeighbor._1)
          }
          while (stack.isEmpty==false)
          {
            val cur=stack.pop()
            val newNeighbor=retrieve_neighborsT(cur,df,
              spatial_threshold,temporal_threshold,min_neighbors)
            // 找到newNeighbor
            for(s<-newNeighbor)
            {
              if(s._6(0)== -1||s._6(0)==0)
              {
                s._6(0)=clusterIndex
                stack.push(s._1)
              }
            }

          }
        }
      }
    }
    /*
      输出格式：
      停留点：（中心lng,中心lat,(停留开始时间，停留结束时间)，STOP）
      移动点： (lng,lat,（移动发生时间，移动发生时间）,MOVE)
     */
    val stop=df.groupBy(x=>x._6(0)).filter(x=>x._1!=0).map{x=>
      var clng=0.0
      var clat=0.0
      val l=x._2.sortBy(t=>t._2)
      for(y<-l)
      {
        clng+=y._4
        clat+=y._5
      }
      new stopPoint(clng/l.length,clat/l.length,l(0)._2,l(l.length-1)._3,
        judgePointAttri(l(0)._2,l(l.length-1)._3))
    }.toArray.sortBy(x=>x.dStart)

    val move=df.filter(x=>x._6(0)==0).map{
      x=>
        movePoint(x._4, x._5, x._2, x._3)
    }.toArray.sortBy(x=>x.smove)
    //用户id,停留点集合,移动点集合
    (line._1,stop,move)
  }

  /**
    * 判断一个天级别停留点的属性，其实并没有用 只是留着判断结果日后处理
    * @param start
    * @param end
    * @return
    */
  def judgePointAttri(start:Date,end:Date)={
    val threeH= 3*60*60*1000
    val twoH= 2*60*60*1000
    val tmp=start
    var attri="unknown"
    /*
      定义居家时间段与工作时间段
     */
    val workTime=(new Date(tmp.getYear,tmp.getMonth,tmp.getDate,7,0,0),
      new Date(tmp.getYear,tmp.getMonth,tmp.getDate,19,0,0))
    val homeTime1=(new Date(tmp.getYear,tmp.getMonth,tmp.getDate,0,0,0),
      new Date(tmp.getYear,tmp.getMonth,tmp.getDate,7,0,0))
    val homeTime2=(new Date(tmp.getYear,tmp.getMonth,tmp.getDate,19,0,0),
      new Date(tmp.getYear,tmp.getMonth,tmp.getDate,24,0,0))
    /*
      计算工作时间段交集
     */
    var Intersection=0.0
    var ts=0L
    var te=0L
    if(start.before(workTime._2)&&end.after(workTime._1))
    {
      if(start.before(workTime._1))
        ts=workTime._1.getTime
      else
        ts=start.getTime
      if(end.before(workTime._2))
        te=end.getTime
      else
        te=workTime._2.getTime
      Intersection=(te-ts).toDouble
      val t1=(end.getTime-start.getTime).toDouble
      if(Intersection/t1>0.5&&Intersection>threeH.toLong)
      {
        attri="work"
      }
      else if((Intersection/t1)<=0.5&&(t1-
        Intersection)>twoH.toLong)
      {
        attri="home"
      }
    }
    else
    {
      Intersection=end.getTime-start.getTime
      if(Intersection>twoH.toLong)
        attri="home"
    }
    attri
  }

  def correctShake(cellDatas: Iterable[cellData]): Iterable[cellData] = {
    var records = cellDatas.toArray
    var before = records(0)
    var i = 1
    while(i < records.length-1) {
      var current = records(i)
      var duration = current.edate.getTime - current.sdate.getTime
      if (duration > 10*60*1000){
        // 当前记录持续时间够长 就不算抖动 do nothing 等效于 continue

      }
      else {
        var next = records(i+1)
        if (calcDis(before.lng,before.lat,next.lng,next.lat) < 50
          && current.edate.equals(next.sdate) && current.sdate.equals(before.edate)
          && calcDis(before.lng,before.lat,current.lng,current.lat) > 300
        ) {
          current.lng = before.lng
          current.lat = before.lat
        }


      }
      before = current
      i+=1
    }

    records
  }







  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("firstCluster300M").setMaster("spark://master01:7077")
    val sc = new SparkContext(conf)

    if (args.length != 2  || !args(0).substring(0,6).equals(args(1).substring(0,6))) {
      throw new InvalidParameterException("Two input parameters must be two date in the same month like yyyyMMdd")
    }


    var start = args(0)
    var end = args(1)
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
      var outputPathDir = "hdfs://dcoshdfs/private_data/useryjj/1Cluster/300M_30MIN/2019/" + month + "/" + currentDString + "/"

      var data = sc.textFile(inPath).filter(x => judgeData(x)).map(x => parseData(x)).groupByKey(32)

      // 过滤出 有数据时长占比为20h 的用户为活跃用户
      var active_data = data.filter(x => activeData(x, 20)).mapValues( x=>correctShake(x))

      var first_results = active_data.map(x => tDbscanAndJudgeAttri(x, 300, 30 * 60 * 1000, 5))
//      var sum = first_results.count()
//      var all_stop_sum = first_results.filter(x => x._2.size > 0).count()
//      var all_move_sum = first_results.filter(x => x._3.size > 0).count()

      var allStopPoint = first_results.map(x => (x._1, x._2)).filter(x => x._2.size > 0).flatMapValues(x=>x).map(x => x._1 + "," + x._2.toString)
      //  .flatMap(x => x._2 map (x._1 -> _)).map(x => x._1 + "," + x._2.toString)
      allStopPoint.saveAsTextFile(outputPathDir + "AllStop")
      var allMovePoint = first_results.map(x => (x._1, x._3)).filter(x => x._2.size > 0).flatMapValues(x=>x).map(x => x._1 + "," + x._2.toString)
//          flatMap(x => x._2 map (x._1 -> _)).map(x => x._1 + "," + x._2.toString)
      allMovePoint.saveAsTextFile(outputPathDir + "AllMove")

      var onlyMove = first_results.filter(x => x._2.size == 0 && x._3.size > 0).map(x => (x._1, x._3))
//      var onlyMove_count = onlyMove.count()
      onlyMove.flatMapValues(x=>x).map(x => x._1 + "," + x._2.toString).saveAsTextFile(outputPathDir + "OnlyMove")

      calstart.add(Calendar.DAY_OF_MONTH, 1)
//      println("=============================================================")
//      println("sum:"+sum)
//      println("stop_sum:" + all_stop_sum)
//      println("move_sum:" + all_move_sum)
//      println("only_move_sum:" + onlyMove_count)
//      println("==============================================================")
    }
  }
}
