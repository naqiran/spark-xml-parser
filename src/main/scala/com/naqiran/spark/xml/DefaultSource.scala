package com.naqiran.spark.xml

import java.util.Optional

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.sources.v2.{DataSourceOptions, ReadSupport, WriteSupport}
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.v2.reader.DataSourceReader
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter
import org.apache.spark.sql.types.StructType

private[xml] class DefaultSource extends DataSourceRegister
  with ReadSupport
  with WriteSupport
  with Logging {

  override def shortName(): String = XMLConfiguration.XmlFormat

  override def createReader(options: DataSourceOptions) : DataSourceReader = throw new UnsupportedOperationException(shortName() + " does not support inferred schema")

  override def createReader(schema: StructType, options: DataSourceOptions) : DataSourceReader = {
    new XMLFileReader(schema, options)
  }

  override def createWriter(writeUUID: String, schema: StructType, mode: SaveMode, options: DataSourceOptions): Optional[DataSourceWriter] = {
    Optional.of(new XMLSourceWriter(schema, mode, options))
  }

  def validate(options : DataSourceOptions): Boolean = {
    if (options.paths().isEmpty) {
      log.debug("Paths should not be empty")
      return false
    }
    true
  }
}