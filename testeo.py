azureEmailSenderMock.when(() =>
  AzureEmailSender.sendEmail(
    any[String],
    any[String],
    any[Array[String]],
    any[Array[String]],
    any[Array[String]],
    any[Array[String]]
  )
).thenReturn(())
