package com.naqiran.spark.xml

import java.io.OutputStream
import java.util

import javax.xml.stream.events.{Attribute, Namespace}
import javax.xml.stream.{XMLEventFactory, XMLEventWriter, XMLOutputFactory}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.IOUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.writer.{DataSourceWriter, DataWriter, DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String


private[xml] class XMLSourceWriter(schema: StructType, mode: SaveMode, options: DataSourceOptions) extends DataSourceWriter {

  val config = new XMLConfiguration(options.asMap())

  override def createWriterFactory(): DataWriterFactory[InternalRow] = {
    new XMLWriterFactory(schema, config)
  }

  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    val partitionFolder = new Path(XMLUtils.getPartitionFolder(config.pathString))
    val fileSystem = partitionFolder.getFileSystem(new Configuration())
    val fileStream = fileSystem.create(new Path(config.pathString))
    try {
      val eventFactory = XMLEventFactory.newInstance()
      fileStream.writeBytes(eventFactory.createStartDocument().toString)
      fileStream.writeBytes(eventFactory.createStartElement(config.schemaPrefix, "", config.rootElementName).toString)
      if (fileSystem.isDirectory(partitionFolder)) {
        val files = fileSystem.listStatus(partitionFolder)
        for (file <- files) {
          val partitionInputStream = fileSystem.open(file.getPath)
          IOUtils.copyBytes(partitionInputStream, fileStream, new Configuration(), false)
        }
      }
      fileStream.writeBytes(eventFactory.createEndElement(config.schemaPrefix, "", config.rootElementName).toString)
      fileSystem.deleteOnExit(partitionFolder)
    }
    IOUtils.closeStream(fileStream)
  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {

  }
}

private[xml] class XMLWriterFactory(schema : StructType, options : XMLConfiguration) extends DataWriterFactory[InternalRow] with Serializable {
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
    writeComplexElement(configuration.recordElementName, record, recordSchema)
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
          case structType: StructType => childElements += (field -> record.getStruct(index, structType.size))
          case arrayType: ArrayType => childElements += (field -> record.getArray(index).toSeq(arrayType.elementType))
          case StringType => {
            if (configuration.textNodeName.equals(field.name)) {
              textValue = record.getString(index)
            } else if (field.name.startsWith(configuration.attributeNodePrefix)) {
              attributes.add(eventFactory.createAttribute(configuration.getAttributeNameWithoutPrefix(field.name), record.getString(index)))
            } else {
              childElements += (field -> record.getString(index))
            }
          }
        }
      }
    }
    xmlWriter.add(eventFactory.createStartElement(configuration.schemaPrefix, "", elementName, attributes.iterator(), namespaces.iterator()))
    if (!textValue.isEmpty) writeTextNode(textValue) else writeChildElements(childElements)
    xmlWriter.add(eventFactory.createEndElement(configuration.schemaPrefix, "", elementName))
  }

  def writeSimpleElement(elementName : String, elementText : String): Unit = {
    xmlWriter.add(eventFactory.createStartElement(configuration.schemaPrefix, "", elementName))
    writeTextNode(elementText)
    xmlWriter.add(eventFactory.createEndElement(configuration.schemaPrefix, "", elementName))
  }

  def writeChildElements(childElements: Map[StructField, AnyRef]): Unit = {
    for ((childElementType, childElement) <- childElements) {
      childElementType.dataType match {
        case structType: StructType => writeComplexElement(childElementType.name, childElement.asInstanceOf[InternalRow], structType)
        case StringType => writeSimpleElement(childElementType.name, childElement.asInstanceOf[String])
        case arrayType: ArrayType => {
          arrayType.elementType match {
            case structType: StructType => childElement.asInstanceOf[Seq[InternalRow]]
              .foreach(seqChildRow => writeComplexElement(childElementType.name, seqChildRow, structType))
            case StringType => childElement.asInstanceOf[Seq[UTF8String]]
              .foreach(seqChildRow => writeSimpleElement(childElementType.name, seqChildRow.toString))
          }
        }
      }
    }
  }

  def writeTextNode(textValue : String) : Unit = {
    xmlWriter.add(eventFactory.createCharacters(textValue))
  }

  override def commit(): WriterCommitMessage = {
    try {
      xmlWriter.close()
      IOUtils.closeStream(fileStream)
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
    val fileName = XMLUtils.getPartitionFileName(partitionId, configuration.pathString)
    val path = new Path(fileName)
    val fileSystem = path.getFileSystem(new Configuration())
    fileSystem.create(path)
  }
}

private[xml] class XMLWriterCommitMessage(message : String) extends WriterCommitMessage with Logging {

}