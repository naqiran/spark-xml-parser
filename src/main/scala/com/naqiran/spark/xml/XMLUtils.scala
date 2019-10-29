package com.naqiran.spark.xml

import java.io.File

import org.apache.hadoop.fs.Path

object XMLUtils extends Serializable {

  def getPartitionFileName(partitionId : Int, path : String): String = {
    getPartitionFolder(path) + File.separator + "/part-" + partitionId
  }

  def getPartitionFolder(path : String) : String = {
    val filePath = new Path(path)
    filePath.getParent.toString + File.separator + "__temp"
  }
}