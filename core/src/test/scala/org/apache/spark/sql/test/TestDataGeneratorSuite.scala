package org.apache.spark.sql.test

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.test.generator.DataType.{BIT, DECIMAL, DOUBLE, INT, VARCHAR}
import org.apache.spark.sql.test.generator.{Data, DefaultColumn, Key, PrefixColumn, PrimaryKey, TestDataGenerator}

import scala.util.Random

class TestDataGeneratorSuite extends SparkFunSuite {

  test("base test for schema generator") {
    val r = new Random(1234)
    val schema = TestDataGenerator.schemaGenerator(
      "tispark_test",
      "test_table",
      r,
      List(
        (INT, "", "not null primary key"),
        (INT, "", "default null"),
        (DOUBLE, "", "not null default 0.2"),
        (VARCHAR, "50", "default null"),
        (DECIMAL, "20,3", "default null")
      ),
      List(
        Key(List(DefaultColumn(2), DefaultColumn(3))),
        Key(List(PrefixColumn(4, 20))),
        Key(List(DefaultColumn(3), DefaultColumn(5)))
      )
    )
    val schema2 = TestDataGenerator.schemaGenerator(
      "tispark_test",
      "test_table",
      r,
      List(
        (INT, "", "not null"),
        (INT, "", "default null"),
        (DOUBLE, "", "not null default 0.2"),
        (VARCHAR, "50", "default null"),
        (DECIMAL, "20,3", "default null")
      ),
      List(
        PrimaryKey(List(DefaultColumn(1))),
        Key(List(DefaultColumn(2), DefaultColumn(3))),
        Key(List(PrefixColumn(4, 20))),
        Key(List(DefaultColumn(3), DefaultColumn(5)))
      )
    )
    val answer = """CREATE TABLE `tispark_test`.`test_table` (
                   |  `col_int0` int not null,
                   |  `col_int1` int default null,
                   |  `col_double` double not null default 0.2,
                   |  `col_varchar` varchar(50) default null,
                   |  `col_decimal` decimal(20,3) default null,
                   |  PRIMARY KEY (`col_int0`),
                   |  KEY `idx_col_int1_col_double`(`col_int1`,`col_double`),
                   |  KEY `idx_col_varchar`(`col_varchar`(20)),
                   |  KEY `idx_col_double_col_decimal`(`col_double`,`col_decimal`)
                   |) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin""".stripMargin
    assert(schema.toString === answer)
    assert(schema2.toString === answer)
  }

  test("test generate schema") {
    val r = new Random(1234)
    val schema = TestDataGenerator.schemaGenerator(
      "tispark_test",
      "test_table",
      r,
      List(
        (INT, "", "not null primary key"),
        (INT, "", "default null"),
        (BIT, "3", "default null"),
        (DOUBLE, "", "not null default 0.2"),
        (VARCHAR, "50", "default null"),
        (DECIMAL, "10,3", "default null")
      ),
      List(
        Key(List(DefaultColumn(2), DefaultColumn(4))),
        Key(List(PrefixColumn(5, 20))),
        Key(List(DefaultColumn(4), DefaultColumn(5)))
      )
    )
    val data: Data = TestDataGenerator.randomDataGenerator(schema, 10, "tispark-test", r)
    data.save()
  }
}
