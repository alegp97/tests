test("should parse all arguments including optional fields") {
  val args = Array(
    "-v", "val_db",
    "-s", "src_db",
    "-d", "stg_db",
    "-p", "/some/path",
    "-l", "20250717",
    "-x", "202507171200",
    "-n", "8080",
    "-h", "localhost",
    "-u", "http://hue.url",
    "-r", "admin",
    "-w", "secret",
    "-f", "/tmp/email_info.json",
    "-b", "processA",
    "-t", "target_tbl",
    "-o", "source_tbl",
    "-z", "/logs/full/path/",
    "-e", "dev",
    "-g", "Sheet1"
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
  assert(config.hue_url == "http://hue.url")
  assert(config.user == "admin")
  assert(config.password == "secret")
  assert(config.file == "/tmp/email_info.json")
  assert(config.process == "processA")
  assert(config.targetTable == "target_tbl")
  assert(config.sourceTable == "source_tbl")
  assert(config.log_path == "/logs/full/path/")
  assert(config.env == "dev")
  assert(config.notificationSheetName == "Sheet1")
}
