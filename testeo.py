// Configurar Hadoop HOME
  System.setProperty("HADOOP_HOME", "C:\\tmp")
  System.setProperty("hadoop.home.dir", "C:\\tmp")
  new java.io.File("C:\\tmp\\bin").mkdirs()

  // Crear mock profundo de SparkSession
  private val sparkMock: SparkSession = mock[SparkSession](withSettings()
    .defaultAnswer(Answers.RETURNS_DEEP_STUBS))
  
  private val sqlContextMock = mock[SQLContext]
  when(sparkMock.sqlContext).thenReturn(sqlContextMock)

  test("procesarDatos con scope CREDIT") {
    // 1. Mockear tablas base
    val engModelMock = mock[DataFrame]
    val modelVersionsMock = mock[DataFrame]
    val modelMock = mock[DataFrame]
    val engineMock = mock[DataFrame]
    
    when(sqlContextMock.table(anyString)).thenAnswer {
      case s: String if s.contains("sae_scen_eng_model") => engModelMock
      case s: String if s.contains("sae_model_versions") => modelVersionsMock
      case s: String if s.contains("sae_model") => modelMock
      case s: String if s.contains("sae_engine") => engineMock
    }

    // 2. Mockear selects
    when(engModelMock.select(any[Column]*)).thenReturn(engModelMock)
    when(modelVersionsMock.select(any[Column]*)).thenReturn(modelVersionsMock)
    when(modelMock.select(any[Column]*)).thenReturn(modelMock)
    
    // 3. Mockear joins
    val joinComunMock = mock[DataFrame]
    when(engModelMock.join(any[DataFrame], any[Column], anyString)).thenReturn(joinComunMock)
    when(joinComunMock.join(any[DataFrame], any[Column])).thenReturn(joinComunMock)
    when(joinComunMock.drop(any[Column])).thenReturn(joinComunMock)
    
    // 4. Mockear filtros para engines
    val idLogitMock = mock[DataFrame]
    val iderror1Mock = mock[DataFrame]
    val iderror2Mock = mock[DataFrame]
    val idpreciosViviendaMock = mock[DataFrame]
    
    when(engineMock.where(any[Column])).thenReturn(engineMock)
    when(engineMock.select(any[Column])).thenReturn(engineMock)
    when(engineMock.distinct()).thenReturn(engineMock)
    
    // 5. Mockear colecciones
    when(engineMock.map(anyFunction)(anyEncoder)).thenReturn(sparkMock.implicits.localSeqToDatasetHolder(Seq("logit1", "logit2")).toDS())
    when(engineMock.collect()).thenReturn(Array(
      Row("logit1"), Row("logit2"), Row("logit3")
    ))
    
    // 6. Mockear DataFrames intermedios
    val m1Mock = mock[DataFrame]
    val m2Mock = mock[DataFrame]
    val m3Mock = mock[DataFrame]
    val m4Mock = mock[DataFrame]
    
    when(joinComunMock.where(any[Column])).thenAnswer {
      case col: Column if col.toString.contains("logit") => m1Mock
      case col: Column if col.toString.contains("error1") => m2Mock
      case col: Column if col.toString.contains("error2") => m3Mock
      case col: Column if col.toString.contains("precios") => m4Mock
    }
    
    // 7. Mockear selects con alias
    when(m1Mock.select(any[Column]*)).thenReturn(m1Mock)
    when(m2Mock.select(any[Column]*)).thenReturn(m2Mock)
    when(m3Mock.select(any[Column]*)).thenReturn(m3Mock)
    when(m4Mock.select(any[Column]*)).thenReturn(m4Mock)
    
    // 8. Mockear joins finales
    val jm1Mock = mock[DataFrame]
    val jm2Mock = mock[DataFrame]
    val resultMock = mock[DataFrame]
    
    when(m1Mock.join(any[DataFrame], any[Column])).thenReturn(jm1Mock)
    when(jm1Mock.select(any[Column]*)).thenReturn(jm1Mock)
    when(jm1Mock.join(any[DataFrame], any[Column])).thenReturn(jm2Mock)
    when(jm2Mock.join(any[DataFrame], any[Column])).thenReturn(resultMock)
    
    // 9. Ejecutar el método bajo prueba
    val result = procesarDatos(sparkMock, "testdb", "CREDIT")
    
    // 10. Verificaciones básicas
    verify(sqlContextMock, atLeastOnce).table(anyString)
    verify(joinComunMock, atLeast(3)).where(any[Column])
    assert(result != null)
  }
