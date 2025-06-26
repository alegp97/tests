val azureStatic = mockStatic(classOf[AzureEmailSender])

azureStatic
  .when(() =>                                 // 1️⃣ método estático
    AzureEmailSender.sendEmail(
      any[String],            // subject     (matcher)
      any[String],            // body        (matcher)
      any[Array[String]],     // to          (matcher)
      any[Array[String]],     // cc
      any[Array[String]],     // bcc
      any[Array[String]]      // attachments
    )
  )
  .thenAnswer(_ => ())   
