/** 2 ─ Variante testeable del NotificationUtil con override de withHdfsFile */
    object TestableNotificationUtil extends NotificationUtil {   // <-- reemplaza por tu objeto/trait real
      override def withHdfsFile[T](conf: SparkConf,
                                   path: String)
                                  (fn: HdfsFile => T): T = {
        val is: InputStream =
          new ByteArrayInputStream(jsonContent.getBytes(StandardCharsets.UTF_8))

        // hdfsFile simulado con el stream anterior
        fn(new HdfsFile {
          override def getInputStream: InputStream = is
        })
        // ¡NADA después de fn(hdfsFile)!
      }
    }

    /** 3 ─ Ejecución del método bajo prueba */
    val result: MailsData =
      TestableNotificationUtil.obtainMailsDataByJsonFile(sqlCtx, "/tmp/dummy.json")

    /** 4 ─ Asserts */
    assert(result.from == "test@sender.com")
    assert(result.to == List("recipient1@test.com", "recipient2@test.com"))
    assert(result.co == List("cc1@test.com"))
    assert(result.bc == List("bc1@test.com"))

    /** 5 ─ Limpieza del mock estático para no “contaminar” otros tests */
    fsStatic.close()
  }
