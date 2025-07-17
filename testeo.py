import org.scalatest.funsuite.AnyFunSuite
import com.santander.stresstest.config.{HouseKeepingParser, HouseKeepingArgs}

class HouseKeepingParserTest extends AnyFunSuite {

  test("should parse required and optional arguments") {
    val args = Array(
      "-d", "my_db",
      "-t", "my_table",
      "-m", "4"
    )

    val result = HouseKeepingParser.parse(args, HouseKeepingArgs())

    assert(result.isDefined)
    val config = result.get
    assert(config.database == "my_db")
    assert(config.table == "my_table")
    assert(config.maxPartitions == 4)
  }

  test("should parse only required argument and use defaults for optional") {
    val args = Array("-d", "default_db")

    val result = HouseKeepingParser.parse(args, HouseKeepingArgs())

    assert(result.isDefined)
    val config = result.get
    assert(config.database == "default_db")
    assert(config.table.isEmpty)
    assert(config.maxPartitions == 1)
  }

  test("should fail if required argument is missing") {
    val args = Array("-t", "table_only")
    val result = HouseKeepingParser.parse(args, HouseKeepingArgs())
    assert(result.isEmpty)
  }
}
