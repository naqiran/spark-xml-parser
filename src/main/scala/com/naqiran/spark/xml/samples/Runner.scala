package com.naqiran.spark.xml.samples

import com.naqiran.spark.xml.XMLConfiguration
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{RowFactory, SparkSession}

object Runner {
    def main(args: Array[String]): Unit = {
      val nameType = new StructType()
        .add(XMLConfiguration.XML_DEFAULT_TEXT_NAME, DataTypes.StringType)
      val schema = new StructType()
        .add(XMLConfiguration.getAttributeName("id"), DataTypes.StringType)
        .add("name", nameType)
      val values = Seq(RowFactory.create("1", RowFactory.create("one")), RowFactory.create("2", null), RowFactory.create("3", null), RowFactory.create("4", null))
      getSession().createDataFrame(getSession().sparkContext.parallelize(values), schema)
        .write
        .option(XMLConfiguration.XML_ELEMENT_NAME, "product")
        .option(XMLConfiguration.XML_SCHEMA_PREFIX, "rrxml")
        .format(XMLConfiguration.XML_FORMAT)
        .save("/Development/workspace/test-1/test.xml")
    }

    def getSession() : SparkSession = {
      SparkSession.builder()
        .master("local")
        .appName("sample")
        .getOrCreate();
    }
}
