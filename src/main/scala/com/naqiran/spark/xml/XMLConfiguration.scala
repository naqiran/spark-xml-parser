package com.naqiran.spark.xml

import java.util
import java.util.Map

private[xml] class XMLConfiguration(@transient private val configuration: Map[String, String]) extends Serializable {
  def this() = this(new util.LinkedHashMap[String,String]())
  val schemaPrefix = configuration.getOrDefault(XMLConfiguration.XmlSchemaPrefix, XMLConfiguration.DefaultSchemaPrefix)
  val attributeNodePrefix = configuration.getOrDefault(XMLConfiguration.XmlAttributeNodePrefix, XMLConfiguration.DefaultAttributeNodePrefix)
  val textNodeName = configuration.getOrDefault(XMLConfiguration.XmlTextNodeName, XMLConfiguration.DefaultTextNodeName)
  val recordElementName = configuration.getOrDefault(XMLConfiguration.XmlRecordElementName, XMLConfiguration.DefaultRecordElementName)
  val path = configuration.getOrDefault("path", "")

  def getAttributeNameWithoutPrefix(attributeName: String): String = {
    attributeName.replace(attributeNodePrefix, "")
  }

  def getAttributeName(attributeName: String): String = {
    attributeNodePrefix + attributeName
  }
}

object XMLConfiguration {
  val XmlFormat = "com.naqiran.spark.xml"
  val XmlSchemaPrefix = "xml.default.schema.prefix"
  val DefaultSchemaPrefix = ""
  val XmlAttributeNodePrefix = "xml.default.attribute.prefix"
  val DefaultAttributeNodePrefix = "__attr__"
  val XmlTextNodeName = "xml.default.text.name"
  val DefaultTextNodeName = "__text__"
  val XmlRecordElementName = "xml.record.element.name"
  val DefaultRecordElementName = "default_element"
  val XmlRootElementName = "xml.default.root.element.name"
  val DefaultRootElementName = "default_root_element"

  def getAttributeName(attributeName: String): String = {
    DefaultAttributeNodePrefix + attributeName
  }
}