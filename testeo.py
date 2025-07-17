import org.scalatest.funsuite.AnyFunSuite
import com.santander.stresstest.entity.{BoardsParser, BoardsArgs}

class BoardsParserTest extends AnyFunSuite {

  test("should parse all arguments correctly") {
    val args = Array(
      "-c", "/conf/path/file.conf",
      "-d", "target_db",
      "-i", """{"entity":"sample"}""",
      "-f", "my_filter",
      "-w", "false",
      "-r", "credit",
      "-s", "src_db",
      "-p", "k1:v1,k2:v2",
      "-t", "tbl_dest",
      "-y", "tbl_optional",
      "-z", "/data/path/",
      "-o", "dev",
      "-l", "20250717",
      "-x", "202507171200",
      "-u", "tbl_src",
      "-b", "insert",
      "-m", "yes"
    )

    val result = BoardsParser.parse(args, BoardsArgs())

    assert(result.isDefined)
    val config = result.get
    assert(config.file == "/conf/path/file.conf")
    assert(config.targetdb == "target_db")
    assert(config.ingestEntity.contains("entity"))
    assert(config.filter == "my_filter")
    assert(config.isWI == false)
    assert(config.risk == "credit")
    assert(config.sourcedb == "src_db")
    assert(config.partitions == Map("k1" -> "v1", "k2" -> "v2"))
    assert(config.targetTable == "tbl_dest")
    assert(config.targetTableOptionalName == "tbl_optional")
    assert(config.path == "/data/path/")
    assert(config.sandbox == "dev")
    assert(config.data_date_part == "20250717")
    assert(config.data_timestamp_part == "202507171200")
    assert(config.sourceTable == "tbl_src")
    assert(config.process == "insert")
    assert(config.is_incremental == "yes")
  }

  test("should fail if required arguments are missing") {
    val args = Array("-c", "only_config.conf") // falta -d y -s
    val result = BoardsParser.parse(args, BoardsArgs())
    assert(result.isEmpty)
  }

  test("should assign default values when optional flags are omitted") {
    val args = Array("-d", "tgt", "-s", "src")
    val result = BoardsParser.parse(args, BoardsArgs())
    assert(result.isDefined)
    val config = result.get

    assert(config.file.isEmpty)
    assert(config.ingestEntity.isEmpty)
    assert(config.filter.isEmpty)
    assert(config.isWI) // default true
    assert(config.risk.isEmpty)
    assert(config.partitions.isEmpty)
    assert(config.targetTable.isEmpty)
    assert(config.targetTableOptionalName.isEmpty)
    assert(config.path.isEmpty)
    assert(config.sandbox.isEmpty)
    assert(config.data_date_part.isEmpty)
    assert(config.data_timestamp_part.isEmpty)
    assert(config.sourceTable.isEmpty)
    assert(config.process.isEmpty)
    assert(config.is_incremental.isEmpty)
  }
}
