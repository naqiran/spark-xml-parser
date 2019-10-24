package com.naqiran.spark.xml

import java.util.List

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecificInternalRow
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, InputPartition, InputPartitionReader}
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.JavaConverters

class XMLFileReader(options : DataSourceOptions)
  extends DataSourceReader
  with Logging {

  override def readSchema(): StructType = {
    options.ensuring(!options.paths().isEmpty)
    new StructType().add("id", DataTypes.StringType)
  }

  override def planInputPartitions(): List[InputPartition[InternalRow]] = {
    val partitions = options.paths().map(path => {
      new XMLPartition(path, options)
    }).toList
    JavaConverters.seqAsJavaList(partitions)
  }
}

class XMLPartition(path: String, options: DataSourceOptions) extends InputPartition[InternalRow]
  with Serializable
{
  override def createPartitionReader(): InputPartitionReader[InternalRow] = {
    XMLPartitionReader(path)
  }


}

case class XMLPartitionReader(st: String) extends InputPartitionReader[InternalRow] {

  override def next(): Boolean = {
    true
  }

  override def get(): InternalRow = {
    val struct = new StructType().add("id", DataTypes.StringType)
    val row = new SpecificInternalRow(struct)
    row.update(0, UTF8String.fromString("test"))
    row
  }

  override def close(): Unit = {

  }
}