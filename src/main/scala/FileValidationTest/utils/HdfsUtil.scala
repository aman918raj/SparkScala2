package FileValidationTest.utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._

import scala.collection.mutable.ListBuffer

object HdfsUtil {

  def getListOfDataFiles(inputPath:String): List[String] ={
    val conf = new Configuration
    val fs = FileSystem.get(conf)
    val path = new Path(inputPath)
    val listOfFiles = new ListBuffer[String]
    if (fs.isDirectory(path)) {
      val files:RemoteIterator[LocatedFileStatus] = fs.listFiles(path,false)
      while (files.hasNext){
        listOfFiles += files.next().getPath.toString
      }
    }else{
      listOfFiles += inputPath
    }
    return listOfFiles.toList
  }

  def moveFileToValidorInvalid(filesPath:String, inputFile:String): Unit={
    val conf = new Configuration
    val srcPath = new Path(inputFile)
    val destPath = new Path(filesPath)
    val fs:FileSystem = srcPath.getFileSystem(conf)
    if(fs.exists(destPath)){
      FileUtil.copy(fs, srcPath, fs, destPath, false, conf)
    }else{
      fs.mkdirs(destPath)
      FileUtil.copy(fs, srcPath, fs, destPath, false, conf)
    }
  }

  def makeDirectory(vaildFilesPath:String): Unit ={
    val conf = new Configuration
    val destPath = new Path(vaildFilesPath)
    val fs:FileSystem = destPath.getFileSystem(conf)
    if(!fs.exists(destPath)){
      fs.mkdirs(destPath)
    }
  }

  def getFileName(inputFile:String): String ={
    val conf = new Configuration
    val destPath = new Path(inputFile)
    val fs:FileSystem = destPath.getFileSystem(conf)
    return fs.getFileStatus(destPath).getPath.getName
  }
}
