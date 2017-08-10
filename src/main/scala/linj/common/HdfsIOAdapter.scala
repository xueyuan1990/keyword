package linj.common

import java.io.{IOException, InputStream, OutputStream}

import com.hankcs.hanlp.corpus.io.IIOAdapter
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

/**
  * Created by linjiang on 2017/7/13.
  */
class HdfsIOAdapter extends IIOAdapter {


  val conf = new Configuration()
  conf.addResource(classOf[HdfsIOAdapter].getResourceAsStream("/linj/common/hdfs-site.xml"))
  conf.addResource(classOf[HdfsIOAdapter].getResourceAsStream("/linj/common/core-site.xml"))
//  conf.addResource(new Path("/opt/hadoop/etc/hadoop/hdfs-site.xml"))
//  conf.addResource(new Path("/opt/hadoop/etc/hadoop/core-site.xml"))
  val fs: FileSystem = FileSystem.get(conf)

  override def create(path: String): OutputStream = new OutputStream() {
    @throws[IOException]
    override def write(b: Int): Unit = {}
  }

  override def open(path: String): InputStream = {
    fs.open(new Path(path))
  }
}

