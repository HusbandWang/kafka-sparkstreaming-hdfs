
import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, TypeReference}
import kafka.serializer.StringDecoder
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.hadoop.mapred.{InvalidJobConfException, JobConf}
import org.apache.hadoop.mapreduce.security.TokenCache
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}


object  kafka_sparkstreaming_hdfs3 {


  def main(args: Array[String]): Unit = {
  val hdfsurl="hdfs://dev.node1.com:8020/zhenghao/test.properties"

   val checkpoint= HDFSUtil.getProperties(hdfsurl,"checkpoint.path")
    val kafkaServers= HDFSUtil.getProperties(hdfsurl,"bootstrap.servers")
    val groupId= HDFSUtil.getProperties(hdfsurl,"group.id")
    val offsetRest= HDFSUtil.getProperties(hdfsurl,"auto.offset")
    val hdfsSavePath= HDFSUtil.getProperties(hdfsurl,"hdfs.savePath")
    val secondTimeS= HDFSUtil.getProperties(hdfsurl,"second.time")
    val topicName= HDFSUtil.getProperties(hdfsurl,"topic.name")
    val master= HDFSUtil.getProperties(hdfsurl,"master")
    val appName= HDFSUtil.getProperties(hdfsurl,"app.name")
    val coresMax= HDFSUtil.getProperties(hdfsurl,"cores.max")



    val executorMemory= HDFSUtil.getProperties(hdfsurl,"executor.Memory")


    val secondTime=Integer.parseInt(secondTimeS)
    //1、创建sparkConf
    //System.setProperty("user.name","root")
    val sparkConf: SparkConf = new SparkConf()
      .setAppName(appName)

       .set("spark.cores.max", coresMax)
       .set("spark.executor.memory",executorMemory)
    //  .set("spark.cores.max", "4")
     // .set("spark.executor.memory","2G")
     .setMaster(master)




    //2、创建sparkContext
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")
    //3、创建StreamingContext
    val ssc = new StreamingContext(sc, Seconds(secondTime))
    //ssc.checkpoint("./Kafka_Direct")
    //集群
    ssc.checkpoint(checkpoint)
    //4、配置kafka相关参数
    val kafkaParams = Map(
      "bootstrap.servers" -> kafkaServers,
      "group.id" -> groupId,
      "auto.offset.reset"->offsetRest
    )
    //5、定义topic
    // dev.consume
    val topics = Set(topicName)


    val dstream=KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc, kafkaParams, topics)

    var now: Date = new Date()
    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    var hehe = dateFormat.format(now)
    val savePath = hdfsSavePath + hehe



    val topicData = dstream.map(_._2)
    val processDstream = topicData.transform(
      rdd => {
        val processRdd = rdd.mapPartitions(iter => {

          val iterator = iter.map(line => {
            //val bsle = JSON.parseObject(line, new TypeReference[bsle]())
            val sb = new StringBuffer()
            val bsle=JSON.parseObject(line,new TypeReference[bsle]() {})
            sb.append(if (bsle.getName == null) "-1" else bsle.getName)
            sb.append(",")
            sb.append(if (bsle.getAge == null) "-1" else bsle.getAge)
            sb.toString
          })
          iterator
        })
        processRdd

      })

     //接下来操作processDstream就可以了
    processDstream.foreachRDD(rdd=>{

     val shuchu= rdd.map(x=>("",x))
     RDD.rddToPairRDDFunctions(shuchu).partitionBy(new HashPartitioner(3)).saveAsHadoopFile(savePath, classOf[String], classOf[String], classOf[RDDMultipleTextOutputFormat])

    })
    //开启计算
    ssc.start()
    ssc.awaitTermination()
  }


  /**
    * 输出文件类
    */
  case class RDDMultipleTextOutputFormat() extends MultipleTextOutputFormat[Any, Any] {

    val currentTime: Date = new Date()
    val formatter = new SimpleDateFormat("yyyy-MM-dd-HHmmss");
    val dateString = formatter.format(currentTime);

    //自定义保存文件名
    override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String = {
      //key 和 value就是rdd中的(key,value)，name是part-00000默认的文件名
      //保存的文件名称，这里用字符串拼接系统生成的时间戳来区分文件名，可以自己定义
      "HTLXYFY" + dateString
    }

    override def checkOutputSpecs(ignored: FileSystem, job: JobConf): Unit = {
      val name: String = job.get(org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.OUTDIR)
      var outDir: Path = if (name == null) null else new Path(name)
      //当输出任务不等于0 且输出的路径为空，则抛出异常
      if (outDir == null && job.getNumReduceTasks != 0) {
        throw new InvalidJobConfException("Output directory not set in JobConf.")
      }
      //当有输出任务和输出路径不为null时
      if (outDir != null) {
        val fs: FileSystem = outDir.getFileSystem(job)
        outDir = fs.makeQualified(outDir)
        outDir = new Path(job.getWorkingDirectory, outDir)
        job.set(org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.OUTDIR, outDir.toString)
        TokenCache.obtainTokensForNamenodes(job.getCredentials, Array[Path](outDir), job)
        //下面的注释掉，就不会出现这个目录已经存在的提示了
        /* if (fs.exists(outDir)) {
             throw new FileAlreadyExistsException("Outputdirectory"
                     + outDir + "alreadyexists");
         }
      }*/


      }
    }
  }

}

