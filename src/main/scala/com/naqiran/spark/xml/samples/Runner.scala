package com.naqiran.spark.xml.samples

import com.naqiran.spark.xml.{XMLConfiguration}
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{RowFactory, SparkSession}

object Runner {

  def main(args: Array[String]): Unit = {
    readXML()
  }

  def readXML() : Unit = {
    getSession()
      .read
      .format(XMLConfiguration.XmlFormat)
      .schema(getSchema)
      .option(XMLConfiguration.XmlRootElementName, "products")
      .option(XMLConfiguration.XmlRecordElementName, "product")
      .load("/Development/workspace/test-1/test.xml")
      .show(10)
  }

  def writeXML() : Unit = {
    val values = Seq(RowFactory.create("1", RowFactory.create("one"), null),
      RowFactory.create("2", null, null),
      RowFactory.create("3", null, null),
      RowFactory.create("4", null, null),
      RowFactory.create("5", RowFactory.create("five"), RowFactory.create(Seq[String]("1", "2", "3"))))

    getSession().createDataFrame(getSession().sparkContext.parallelize(values), getSchema())
      .write
      .option(XMLConfiguration.XmlRecordElementName, "product")
      .option(XMLConfiguration.XmlRootElementName, "products")
      .format(XMLConfiguration.XmlFormat)
      .save("/Development/workspace/test-1/test.xml")
  }

  def getSchema() : StructType =  {
    val nameType = new StructType()
      .add(XMLConfiguration.DefaultTextNodeName, DataTypes.StringType)
    val upcType = new StructType()
      .add("UPC", DataTypes.createArrayType(DataTypes.StringType))
    new StructType()
      .add(XMLConfiguration.defaultAttributeName("id"), DataTypes.StringType)
      .add("name", DataTypes.StringType)
      .add("UPCS", upcType)
  }

  def getSession() : SparkSession = {
    SparkSession.builder()
      .master("local")
      .appName("sample")
      .getOrCreate();
  }
}
