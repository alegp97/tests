when(usersEmisores.where(any[Column])).thenReturn(usersEmisoresFiltered)
when(usersEmisoresFiltered.limit(anyInt())).thenReturn(usersEmisoresLimited)
when(usersEmisoresLimited.collect()).thenReturn(Array(rowEmisor))
when(rowEmisor.getAs[String]("user_email")).thenReturn("emisor.unidad@test.com")
