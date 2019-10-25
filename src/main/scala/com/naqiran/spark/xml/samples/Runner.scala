package com.naqiran.spark.xml.samples

import com.naqiran.spark.xml.XMLConfiguration
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{RowFactory, SparkSession}

object Runner {
    def main(args: Array[String]): Unit = {
      val nameType = new StructType()
        .add(XMLConfiguration.DefaultTextNodeName, DataTypes.StringType)
      val upcType = new StructType()
        .add("UPC", DataTypes.createArrayType(DataTypes.StringType))
      val schema = new StructType()
        .add(XMLConfiguration.getAttributeName("id"), DataTypes.StringType)
        .add("name", nameType)
        .add("UPCS", upcType)

      val values = Seq(RowFactory.create("1", RowFactory.create("one"), null),
        RowFactory.create("2", null, null),
        RowFactory.create("3", null, null),
        RowFactory.create("4", null, null),
        RowFactory.create("5", RowFactory.create("five"), RowFactory.create(Seq[String]("1", "2", "3"))))
      getSession().createDataFrame(getSession().sparkContext.parallelize(values), schema)
        .write
        .option(XMLConfiguration.XmlRecordElementName, "product")
        .option(XMLConfiguration.XmlSchemaPrefix, "rrxml")
        .format(XMLConfiguration.XmlFormat)
        .save("/Development/workspace/test-1/test.xml")
    }

    def getSession() : SparkSession = {
      SparkSession.builder()
        .master("local")
        .appName("sample")
        .getOrCreate();
    }
}
