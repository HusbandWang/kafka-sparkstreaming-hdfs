import java.io.IOException
import java.net.URI
import java.util.Properties

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}

object HDFSUtil {
  val conf: Configuration = new Configuration
  var fs: FileSystem = null
  var hdfsInStream: FSDataInputStream = null
  val prop = new Properties()

  //获取文件输入流
  def getFSDataInputStream(path: String): FSDataInputStream = {
    try {
      fs = FileSystem.get(URI.create(path), conf)
      hdfsInStream = fs.open(new Path(path))
    } catch {
      case e: IOException => {
        e.printStackTrace
      }
    }
    return hdfsInStream
  }

  //读取配置文件
  def getProperties(path: String, key: String): String = {
    prop.load(this.getFSDataInputStream(path))
    prop.getProperty(key)


  }
}