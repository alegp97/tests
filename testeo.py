import org.scalatest.funsuite.AnyFunSuite
import com.santander.stresstest.parser.config.{CompactorParser, CompactorArgs}

class CompactorParserTest extends AnyFunSuite {

  test("should parse all arguments correctly") {
    val args = Array(
      "-s", "256",
      "-h", "8",
      "-i", """{"obj":"value"}""",
      "-f", "filter_string",
      "-d", "db_prod",
      "-t", "tbl_fact"
    )

    val result = CompactorParser.parse(args, CompactorArgs())
    assert(result.isDefined)
    val config = result.get

    assert(config.sizeFile == 256)
    assert(config.thread == 8)
    assert(config.ingestEntity == """{"obj":"value"}""")
    assert(config.filter == "filter_string")
    assert(config.database == "db_prod")
    assert(config.table == "tbl_fact")
  }

  test("should parse only required and use default values") {
    val args = Array("-d", "only_required_db")

    val result = CompactorParser.parse(args, CompactorArgs())
    assert(result.isDefined)
    val config = result.get

    assert(config.database == "only_required_db")
    assert(config.sizeFile == 128)
    assert(config.thread == 1)
    assert(config.ingestEntity.isEmpty)
    assert(config.filter.isEmpty)
    assert(config.table.isEmpty)
  }

  test("should fail when required database is missing") {
    val args = Array("-t", "missing_db_table")
    val result = CompactorParser.parse(args, CompactorArgs())
    assert(result.isEmpty)
  }
}
