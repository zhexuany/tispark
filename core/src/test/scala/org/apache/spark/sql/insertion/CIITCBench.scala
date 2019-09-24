package org.apache.spark.sql.insertion

import com.pingcap.tikv.meta.TiColumnInfo
import com.pingcap.tispark.datasource.BaseDataSourceTest
import com.pingcap.tispark.utils.TiUtil
import org.apache.spark.sql.{BaseTestGenerationSpec, Row, TiSparkTestSpec}
import org.apache.spark.sql.test.generator.{Schema, TestDataGenerator}
import org.apache.spark.sql.test.generator.DataType._
import org.apache.spark.sql.test.generator.TestDataGenerator.{TiRow, generateRandomRows}

class CIITCBench extends BaseDataSourceTest("lccont_tmp") with BaseTestGenerationSpec with TiSparkTestSpec {
  val dataTypesWithDesc = List(
    (VARCHAR, "120", ""),
    (VARCHAR, "20", ""),
    (VARCHAR, "500", ""),
    (VARCHAR, "50", ""),
    (VARCHAR, "50", ""),
    (DATE, "", ""),
    (DATE, "", ""),
    (VARCHAR, "50", "")
  )

  val colNames = List(
    "appntname",
    "sex",
    "appntcerttype",
    "appntcertno",
    "appntscertno",
    "effdate",
    "cdate",
    "excelname"
  )
  val schema = TestDataGenerator.schemaGenerator(
    "test",
    "lccont",
    r,
    dataTypesWithDesc,
    null,
    colNames
  )

  override val rowCount = 50

  private def tiRowToSparkRow(row: TiRow, tiColsInfos: java.util.List[TiColumnInfo]) = {
    val sparkRow = new Array[Any](row.fieldCount())
    for (i <- 0 until row.fieldCount()) {
      val colTp = tiColsInfos.get(i).getType
      val colVal = row.get(i, colTp)
      sparkRow(i) = colVal
    }
    Row.fromSeq(sparkRow)
  }

  private def dropAndCreateTbl(schema: Schema): Unit = {
    // drop table if exits
    dropTable(schema.tableName)

    // create table in tidb first
    jdbcUpdate(schema.toString)
  }

  private def insertAndSelect(schema: Schema): Unit = {
    val tblName = schema.tableName

    val tiTblInfo = getTableInfo(database, tblName)
    val tiColInfos = tiTblInfo.getColumns
    // gen data
    val rows =
      generateRandomRows(schema, rowCount, r).map(row => tiRowToSparkRow(row, tiColInfos))
    // insert data to tikv
    tidbWriteWithTable(rows, TiUtil.getSchemaFromTable(tiTblInfo), tblName)
    // select data from tikv and compare with tidb
    compareTiDBSelectWithJDBCWithTable_V2(tblName = tblName, "effdate")
  }

  // this is only for mute the warning
  override def test(): Unit = {}

  test("test ciit cases") {
    dropAndCreateTbl(schema)
    insertAndSelect(schema)
  }

  override def getTableName(dataTypes: String*): String = "ciit"

  override def getTableNameWithDesc(desc: String, dataTypes: String*): String = ""

  override val testDesc: String = ""
}
