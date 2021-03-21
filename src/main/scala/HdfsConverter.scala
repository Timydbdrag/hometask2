import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._

import java.io.File
import java.net.URI
import scala.collection.mutable.ArrayBuffer

object HdfsConverter {

  val defPath = "/stage"
  val writePath = "/ods"
  val saveFile = "/part-0000.csv"

  def main(args: Array[String]): Unit = {
    val hdfs = Hdfs("hdfs://namenode:9000/")

    println("Start...")
    getDirectory(defPath)(hdfs).foreach(el => {
      if (el.isDirectory) {
        val dataAllFiles = getData(el, hdfs)
        val endPath = writePath + "/" + el.getPath.getName + saveFile
        writer(endPath, hdfs, dataAllFiles)
      }
    })
    println("Done!")
    hdfs.close()
  }

  def printResult(hdfs: FileSystem): Unit ={
    getDirectory(writePath)(hdfs).foreach(el => {
      getDirectory(writePath + "/" + el.getPath.getName)(hdfs).foreach(els => {
        println(els.getPath)
        val path = new Path(writePath + "/" + el.getPath.getName + "/" + els.getPath.getName)
        val stream = hdfs.open(path)
        def readLines = Stream.cons(stream.readLine, Stream.continually( stream.readLine))
        readLines.takeWhile(_ != null).foreach(line => println(line))
      })
    })
  }

  def writer(wPath: String, hdfs: FileSystem, data: ArrayBuffer[Byte]) = {
    val dt = hdfs.create(new Path(wPath))
    dt.write(data.toArray)
  }

  def getData(el: FileStatus, hdfs: FileSystem): ArrayBuffer[Byte] = {
    val arr = ArrayBuffer[Byte]()
    getDirectory(defPath + "/" + el.getPath.getName)(hdfs).foreach(el2 => {
      if (el2.isFile) {
        val path = new Path(defPath + "/" + el.getPath.getName + "/" + el2.getPath.getName)

        if (isCSV(path)) {
          val data = hdfs.open(path)

          def dt = data.getWrappedStream.readAllBytes()

          arr ++= dt
        }
      }
    })
    arr
  }

  def isCSV(path: Path): Boolean = {
    path.getName.matches(".*csv.*")
  }

  def Hdfs(urlHdfs: String): FileSystem = {
    FileSystem.get(new URI(urlHdfs), new Configuration())
  }

  def getDirectory(path: String)(hdfs: FileSystem): Array[FileStatus] = {
    hdfs.listStatus(new Path(path))
  }


}
