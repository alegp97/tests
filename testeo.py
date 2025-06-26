val azureStatic = mockStatic(classOf[AzureEmailSender])

// 2️⃣  stub: TODOS los argumentos son matchers, firma idéntica
azureStatic
  .when(() =>
    AzureEmailSender.sendEmail(
      anyString(),                       // subject
      anyString(),                       // body
      any(classOf[Array[String]]),       // to
      any(classOf[Array[String]]),       // cc
      any(classOf[Array[String]]),       // bcc
      any(classOf[Array[String]])        // attachments
    )
  )
  .thenAnswer(_ => ())
