package com.naqiran.spark.xml

import java.io.InputStream
import java.util.{Arrays, List}

import javax.xml.stream.events.{Attribute, Characters, EndElement, StartElement}
import javax.xml.stream.{XMLEventFactory, XMLEventReader, XMLInputFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IOUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecificInternalRow
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, InputPartition, InputPartitionReader}
import org.apache.spark.sql.types.{DataType, DataTypes, StructType}
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
  val xmlReader  = createXMLReader()
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
    currentElement != null
  }

  def getNextElement(elementName : String) : StartElement = {
    while (xmlReader.hasNext()) {
      xmlReader.nextEvent() match {
        case startElement: StartElement =>
          if (startElement.getName.getLocalPart.equals(elementName)) {
            return startElement
          }
        case _ =>
      }
    }
    null
  }

  override def get(): InternalRow = {
    getElement(currentElement, schema)
  }

  def getFieldMap(schema : StructType) : Map[String, DataType] = {
    schema.fields.map(field => field.name -> field.dataType).toMap
  }

  def getElement(element : StartElement, elementSchema : StructType): InternalRow = {
    val row = new SpecificInternalRow(elementSchema)
    element.getAttributes.forEachRemaining(attr => {
      val attribute = attr.asInstanceOf[Attribute]
      updateRow(row, configuration.getAttributeName(attribute.getName.getLocalPart), elementSchema, attribute.getValue)
    })
    var reachedEndElement = true
    var currentElementName = element.getName.getLocalPart
    while(xmlReader.hasNext && reachedEndElement) {
      xmlReader.nextEvent() match {
        case startElement: StartElement => {
          currentElementName = startElement.getName.getLocalPart
          val elementIndex = elementSchema.names.indexOf(startElement.getName.getLocalPart)
          if (elementIndex >= 0){
            val structField = schema.apply(elementIndex)
            if (structField.dataType.typeName.equals("struct")) {
              row.update(elementIndex, getElement(startElement,structField.dataType.asInstanceOf[StructType]))
            }
          }
        }
        case endElement: EndElement => if (element.getName.getLocalPart.equals(endElement.getName.getLocalPart)) {
          reachedEndElement = false
        }
        case character: Characters => updateRow(row, currentElementName, elementSchema, character.getData)
      }
    }
    row
  }

  def updateRow(row : InternalRow, elementName : String, elementSchema : StructType, value : String) : Unit = {
    val fieldIndex = elementSchema.names.indexOf(elementName)
    if (fieldIndex >= 0) {
      elementSchema.apply(elementName).dataType match {
        case DataTypes.StringType => row.update(fieldIndex, UTF8String.fromString(value))
        case _ =>
      }
    }
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
