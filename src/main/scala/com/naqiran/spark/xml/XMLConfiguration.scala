package com.naqiran.spark.xml

import java.util
import java.util.Map

private[xml] class XMLConfiguration(@transient private val configuration: Map[String, String]) extends Serializable {
  def this() = this(new util.LinkedHashMap[String,String]())
  val SCHEMA_PREFIX = configuration.getOrDefault(XMLConfiguration.XML_SCHEMA_PREFIX, XMLConfiguration.XML_DEFAULT_SCHEMA_PREFIX)
  val ATTRIBUTE_PREFIX = configuration.getOrDefault(XMLConfiguration.XML_ATTRIBUTE_NAME_PREFIX, XMLConfiguration.XML_DEFAULT_ATTRIBUTE_NAME_PREFIX)
  val TEXT_VALUE = configuration.getOrDefault(XMLConfiguration.XML_TEXT_NAME, XMLConfiguration.XML_DEFAULT_TEXT_NAME)
  val ELEMENT_NAME = configuration.getOrDefault(XMLConfiguration.XML_ELEMENT_NAME, XMLConfiguration.XML_DEFAULT_ELEMENT_NAME)
  val PATH = configuration.getOrDefault("path", "")

  def getAttributeNameWithoutPrefix(attributeName: String): String = {
    attributeName.replace(ATTRIBUTE_PREFIX, "")
  }

  def getAttributeName(attributeName: String): String = {
    ATTRIBUTE_PREFIX + attributeName
  }
}

object XMLConfiguration {
  val XML_FORMAT = "com.naqiran.spark.xml"
  val XML_SCHEMA_PREFIX = "xml.default.schema.prefix"
  val XML_DEFAULT_SCHEMA_PREFIX = ""
  val XML_ATTRIBUTE_NAME_PREFIX = "xml.default.attribute.prefix"
  val XML_DEFAULT_ATTRIBUTE_NAME_PREFIX = "__attr__"
  val XML_TEXT_NAME = "xml.default.text.name"
  val XML_DEFAULT_TEXT_NAME = "__text__"
  val XML_ELEMENT_NAME = "xml.default.element.name"
  val XML_DEFAULT_ELEMENT_NAME = "default_element"
  val XML_ROOT_ELEMENT_NAME = "xml.default.root.element.name"
  val XML_DEFAULT_ROOT_ELEMENT_NAME = "default_root_element"

  def getAttributeName(attributeName: String): String = {
    XML_DEFAULT_ATTRIBUTE_NAME_PREFIX + attributeName
  }
}