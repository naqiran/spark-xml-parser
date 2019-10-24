package com.naqiran.spark.xml

import java.io.OutputStream
import java.util

import javax.xml.stream.events.{Attribute, Namespace}
import javax.xml.stream.{XMLEventFactory, XMLEventWriter, XMLOutputFactory}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.writer.{DataSourceWriter, DataWriter, DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

class XMLSourceWriter(schema: StructType, mode: SaveMode, options: DataSourceOptions) extends DataSourceWriter {

  val config = new XMLConfiguration(options.asMap())

  override def createWriterFactory(): DataWriterFactory[InternalRow] = {
    new XMLWriterFactory(schema, config)
  }

  override def commit(messages: Array[WriterCommitMessage]): Unit = {

  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {

  }
}

class XMLWriterFactory(schema : StructType, options : XMLConfiguration) extends DataWriterFactory[InternalRow] with Serializable {
  override def createDataWriter(partitionId: Int, taskId: Long, epochId: Long): DataWriter[InternalRow] = {
    new XMLFileWriter(partitionId, schema, options)
  }
}

class XMLFileWriter(partitionId : Int, recordSchema : StructType, configuration : XMLConfiguration) extends DataWriter[InternalRow]
  with Serializable
  with Logging {

  val fileStream = createFileStream()
  val eventFactory = XMLEventFactory.newInstance()
  val xmlWriter  = createXMLWriter()

  override def write(record: InternalRow): Unit = {
    writeComplexElement(configuration.ELEMENT_NAME, record, recordSchema)
  }

  def writeComplexElement(elementName : String, record : InternalRow, schema : StructType): Unit = {
    val attributes = new util.ArrayList[Attribute]()
    val namespaces = new util.ArrayList[Namespace]()
    var childElements = Map[StructField, AnyRef]()
    var textValue : String = ""
    for (index <- 0 to schema.length - 1) {
      val field = schema.fields.apply(index)
      if (!record.isNullAt(index)) {
        field.dataType match {
          case structType: StructType => {
            childElements += (field -> record.getStruct(index, structType.size))
          }
          case StringType => {
            if (configuration.TEXT_VALUE.equals(field.name)) {
              textValue = record.getString(index)
            } else if (field.name.startsWith(configuration.ATTRIBUTE_PREFIX)) {
              attributes.add(eventFactory.createAttribute(configuration.getAttributeNameWithoutPrefix(field.name), record.getString(index)))
            } else {
              childElements += (field -> record.getString(index))
            }
          }
        }
      }
    }
    xmlWriter.add(eventFactory.createStartElement(configuration.SCHEMA_PREFIX, "", elementName, attributes.iterator(), namespaces.iterator()))
    if (!textValue.isEmpty) xmlWriter.add(eventFactory.createCharacters(textValue))
    else {
      for ((childElementType, childElement) <- childElements) {
        childElementType.dataType match {
          case structType: StructType => writeComplexElement(childElementType.name, childElement.asInstanceOf[InternalRow], structType)
          case StringType => writeSimpleElement(childElementType.name, childElement.asInstanceOf[String])
        }
      }
    }
    xmlWriter.add(eventFactory.createEndElement(configuration.SCHEMA_PREFIX, "", elementName))
  }

  def writeSimpleElement(elementName : String, elementValue : String): Unit = {
    xmlWriter.add(eventFactory.createStartElement(configuration.SCHEMA_PREFIX, "", elementName))
    xmlWriter.add(eventFactory.createCharacters(elementValue))
    xmlWriter.add(eventFactory.createEndElement(configuration.SCHEMA_PREFIX, "", elementName))
  }

  override def commit(): WriterCommitMessage = {
    try {
      xmlWriter.flush()
      xmlWriter.close()
      fileStream.flush()
      fileStream.close()
    } catch {
      case ex: Exception => log.error("Error closing the streams for partitions: {} - {}", partitionId, ex.getMessage)
    }
    new XMLWriterCommitMessage("Successfully committed  the partition: " + partitionId)
  }

  override def abort(): Unit = {

  }

  def createXMLWriter() : XMLEventWriter = {
    XMLOutputFactory.newInstance().createXMLEventWriter(fileStream)
  }

  def createFileStream() : OutputStream = {
    val pathString = configuration.PATH
    val path = new Path(pathString).getParent
    val fileSystem = path.getFileSystem(new Configuration)
    fileSystem.create(new Path(path.toString + "/part-" + partitionId))
  }
}

class XMLWriterCommitMessage(message : String) extends WriterCommitMessage with Logging {

}