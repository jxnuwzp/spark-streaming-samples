package com.hshbic.cloud.bigdata.sparkstreaming.statistics.scala

import java.text.SimpleDateFormat
import java.util.Date

import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.JavaConversions.mapAsJavaMap

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Durations
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream.toPairDStreamFunctions
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.zookeeper.WatchedEvent
import org.apache.zookeeper.Watcher
import org.apache.zookeeper.ZooKeeper

import com.hshbic.cloud.bigdata.utils.RedisClusterUtils

import net.sf.json.JSONObject
/**
 * 统计HeartBeat上报数据 每30秒统计前3分钟的上报数据,
 * 将原始数据转为key=deviceType, value=macid
 * 根据deviceType分组,并统计去重的macid数,最终存储到redis
 *
 * @author Wei Yue
 *
 */
object HeartBeatStatisticsWithReceiver {

  val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
  

  def main(args: Array[String]): Unit = {
    if (args.length < 8) {
      println("missing argument ! \n argument list : brokers,topics,redisCluster,interval,redisPassword,windowDuration,slideDuration,jobName")
      System.exit(1)
    }

    val zkQuorum = args(0) //zkQuorum集群
    val topics = args(1) // kafka主题
    val redisCluster = args(2) // redis集群，sentinels地址
    val interval = args(3).toInt // batch计算周期
    val redisPassword = args(4) // redis 集群密码,没有的话为""
    val windowDuration = args(5).toInt // 计算窗口时长
    val consumerGroup = args(6) //is the name of kafka consumer group
    val consumerNumThreads = args(7).toInt //is the number of threads the kafka consumer should use
    println("zkQuorum:" + zkQuorum + ",interval:" + interval + ",topics:" +
      topics + ",redisCluster : " + redisCluster + ",redisPassword : " +
      redisPassword + ",windowDuration : " + windowDuration 
      + ",consumerGroup:" + consumerGroup + ",consumerNumThreads:" + consumerNumThreads);

    Logger.getRootLogger.setLevel(Level.WARN)
    
    //业务场景不需要处理程序启动前的累积数据,所以删除offset
    initZKAndRemovePath(consumerGroup, zkQuorum)
    
    val ssc = createContext(topics, zkQuorum,
      redisCluster, interval, redisPassword, windowDuration, consumerGroup, consumerNumThreads)

    ssc.start()
    ssc.awaitTermination()
  }

  def initZKAndRemovePath(consumerGroup: String, zkQuorum: String) = {
    if(consumerGroup != null){
      var zk:ZooKeeper=null
      try {
      //初始化ZK
        val emptyWatcher: Watcher=new Watcher() { 
          def  process(event: WatchedEvent):Unit= {}
        }
      zk = new ZooKeeper(zkQuorum, 10000, emptyWatcher)
      //删除old offset目录,读取最新的offset
      removePath(zk,"/consumers/" + consumerGroup)
      } catch {
        case t: Exception => {t.printStackTrace();throw t}
      }finally {
        if(zk != null){
          zk.close()
        }
      }
    }
  }
  //计算主体
  def createContext(topics: String, zkQuorum: String, redisCluster: String, interval: Int, redisPassword: String,
                    windowDuration: Int, consumerGroup: String, consumerNumThreads: Int) = {
    val ssc = initSparkStreamingContext(interval);

    val messages =
      (1 to consumerNumThreads) map { _ =>
        KafkaUtils.createStream(ssc, zkQuorum, consumerGroup, Map(topics -> 1), StorageLevel.MEMORY_AND_DISK_SER_2)
      }
    val message = ssc.union(messages)
    message
      .map[(String, String)] { x =>
        //将原始Json数据转换为成对的deviceType,issuerId数据
        val t = mapToPair(x)
        t
      }
      .groupByKeyAndWindow(Durations.seconds(windowDuration), Durations.seconds(interval)) //累积时间窗口，按照deviceType做分组
      .foreachRDD(rdd => {
        //将批次数据的统计时间和枚举的deviceTypes存储到redis
        statisticsTimeToRedis(redisCluster, rdd)

        rdd.foreachPartition(iterator => {
          //分区统计各个devicetype的macid数(去重后)
          statisticsMacidByDeviceType(redisCluster, iterator)
        })
      })
    ssc
  }

  /**
   * 去掉消息body中的前缀，将原始Json数据转换为成对的deviceType,issuerId数据
   */
  def mapToPair(x: (String, String)) = {
    val startIndex = x._2.indexOf(":")
    val jsonText = x._2.substring(startIndex + 1)
    val fromObject = JSONObject.fromObject(jsonText)
    val t = (fromObject.get("deviceType").toString(), fromObject.get("issuerId").toString())
    t
  }

  /**
   * 初始化SparkStreamingContext
   */
  def initSparkStreamingContext(interval: Int) = {
    val conf = new SparkConf()
    //    val conf = new SparkConf().setAppName("HeartBeatStatistics").setMaster("local[2]");//本地测试
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(interval))
    ssc
  }

  /**
   * 分区统计各个devicetype的macid数(去重后)
   */
  def statisticsMacidByDeviceType(redisCluster: String, iterator: Iterator[(String, Iterable[String])]) = {
    var redis :RedisClusterUtils = null
    try {
      redis = new RedisClusterUtils(redisCluster)
      iterator.foreach(x => { //每个deviceType下的统计
        val deviceType = x._1
        val macSet = x._2.toSet //不重复macid的数目
        val macCount = x._2.size //去重前的macid数目，debug用
        redis.setValue("bigdata:realcount:" + deviceType, macSet.size + "")
        //        println("deviceType :" + deviceType + ",去重后的mac数" + macSet.size + ",去重前的mac数 : " + macCount)
      })
    } catch {
      case t: Throwable => t.printStackTrace()
    } finally {
      if (redis!=null)
      {
        redis.close()
      }
    }
  }

  /**
   * 将批次数据的统计时间存储到redis
   */
  def statisticsTimeToRedis(redisCluster: String, rdd: RDD[(String, Iterable[String])]) = {
    var redis :RedisClusterUtils = null
    try {
      redis = new RedisClusterUtils(redisCluster);
      val jedis = redis.getJedis4Write();
      //将所有devicetype组成03001001,03001002,03003003,...
      val types = rdd.toLocalIterator.map(_._1).toSet.mkString(",")
      val time = dateFormat.format(new Date().getTime)
//      println("types : " + types + ",time : " + time + ",partitionSize:" + rdd.partitions.size)
      //把计算时间和所有的devicetype记录到redis
      val timeMap = Map("time" -> time, "types" -> types)
      jedis.hmset("bigdata:realcount", timeMap)
    } catch {
      case t: Exception => t.printStackTrace() // 
    } finally {
      //注意此处需要关闭整个RedisClusterUtils，而不只是master redis
      if (redis!=null)
      {
        redis.close()
      }
    }
  }

  /**
   * 删除zkNode目录
   */
  def removePath(zk:ZooKeeper,path: String): Unit = {
    val child = zk.getChildren(path, false)
    for (cPath  <- child) {
      removePath(zk,path + "/" + cPath)
    }
    println("zkPath : " + path)
    zk.delete(path, -1);
  }
}