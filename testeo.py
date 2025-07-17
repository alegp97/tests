import org.scalatest.funsuite.AnyFunSuite
import com.santander.stresstest.config.{NotificationParser, NotificationArgs}

class NotificationParserTest extends AnyFunSuite {

  test("should parse all required arguments correctly") {
    val args = Array(
      "-v", "val_db",
      "-s", "src_db",
      "-d", "stg_db",
      "-p", "/some/path",
      "-l", "20250717",
      "-x", "202507171200",
      "-n", "8080",
      "-h", "localhost",
      "-f", "/tmp/email_info.json"
    )

    val result = NotificationParser.parse(args, NotificationArgs())

    assert(result.isDefined)
    val config = result.get
    assert(config.validationdb == "val_db")
    assert(config.sourcedb == "src_db")
    assert(config.stagingdb == "stg_db")
    assert(config.path == "/some/path")
    assert(config.data_date_part == "20250717")
    assert(config.data_timestamp_part == "202507171200")
    assert(config.port == "8080")
    assert(config.host == "localhost")
    assert(config.file == "/tmp/email_info.json")
  }

  test("should fail when required arguments are missing") {
    val args = Array("-v", "val_db") // faltan muchas flags obligatorias
    val result = NotificationParser.parse(args, NotificationArgs())
    assert(result.isEmpty)
  }
}
