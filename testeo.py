val anyRow  = mock[Row]
when(anyRow.getString(anyInt())).thenReturn("")          // <- evita NPE
when(anyDF.collect()).thenReturn(Array(anyRow))
when(anyDF.count()).thenReturn(1L)
when(anyDF.columns).thenReturn(Array("c1","c2"))
