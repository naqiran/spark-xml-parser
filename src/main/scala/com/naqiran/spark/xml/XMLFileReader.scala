package com.naqiran.spark.xml

import java.io.InputStream
import java.util.{Arrays, List}

import javax.xml.stream.events.StartElement
import javax.xml.stream.{XMLEventFactory, XMLEventReader, XMLInputFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IOUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecificInternalRow
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, InputPartition, InputPartitionReader}
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String


private[xml] class XMLFileReader(schema: StructType, options : DataSourceOptions)
  extends DataSourceReader
  with Logging {

  override def readSchema(): StructType = schema

  override def planInputPartitions(): List[InputPartition[InternalRow]] = {
     Arrays.asList(new XMLPartition(new XMLConfiguration(options.asMap()), schema))
  }
}

private[xml] class XMLPartition(configuration: XMLConfiguration, schema : StructType) extends InputPartition[InternalRow] with Serializable {
  override def createPartitionReader(): InputPartitionReader[InternalRow] = XMLPartitionReader(configuration, schema)
}

case class XMLPartitionReader(configuration: XMLConfiguration, schema : StructType) extends InputPartitionReader[InternalRow] with Logging {

  val fileStream = createFileStream
  val xmlWriter  = createXMLReader()
  val eventFactory = XMLEventFactory.newInstance()
  var rootIdentified = false
  var currentElement : StartElement = null

  override def next(): Boolean = {
    if (!rootIdentified) {
      getNextElement(configuration.rootElementName)
      currentElement = getNextElement(configuration.recordElementName)
      rootIdentified = true
    } else {
      currentElement = getNextElement(configuration.recordElementName)
    }
    log.info("Dont know" + currentElement)
    currentElement != null
  }

  def getNextElement(elementName : String) : StartElement = {
    while (xmlWriter.hasNext()) {
      xmlWriter.nextEvent() match {
        case startElement: StartElement =>
          if (startElement.getName.getLocalPart.equals(elementName)) {
            return startElement
          }
        case _ =>
      }
    }
    log.info("Somehow got here:" + elementName)
    null
  }

  override def get(): InternalRow = {
    val row = new SpecificInternalRow(schema)
    row.update(0, UTF8String.fromString(currentElement.getName().getLocalPart))
    row
  }

  override def close(): Unit = {
    IOUtils.closeStream(fileStream)
  }

  def createXMLReader() : XMLEventReader = {
    XMLInputFactory.newInstance().createXMLEventReader(fileStream)
  }

  def createFileStream() : InputStream = {
    val path = new Path(configuration.pathString)
    val fileSystem = path.getFileSystem(new Configuration())
    fileSystem.open(path)
  }
}