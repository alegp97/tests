// En tu archivo de producción
trait SparkSessionProvider {
  def spark: SparkSession
}

object BoardDataUtil extends SparkSessionProvider {
  val log = LogManager.getLogger(getClass.getName)
  val DRIVER_APP_NAME = "[$AST] BoardDataUtil"
  
  lazy val spark: SparkSession = SparkSession.builder.appName(DRIVER_APP_NAME)
    .config("hive.exec.dynamic.partition.mode", "nonstrict")
    .config("hive.metastore.try.direct.sql", "true")
    .config("spark.sql.hive.convertMetastoreParquet", "false")
    .enableHiveSupport()
    .getOrCreate()
}

// En tu archivo de pruebas
trait MockSparkSessionProvider extends SparkSessionProvider {
  private val hadoopTmpDir = new java.io.File("C:/spark-temp").getAbsolutePath
  System.setProperty("hadoop.home.dir", hadoopTmpDir)
  new java.io.File(s"$hadoopTmpDir/bin").mkdirs()
  
  // Mock profundo de SparkSession
  override val spark: SparkSession = {
    val mockSession = mock[SparkSession](withSettings()
      .defaultAnswer(Answers.RETURNS_DEEP_STUBS)
      .serializable())
    
    // Configuración básica de mocks necesarios
    when(mockSession.version).thenReturn("3.2.1")
    when(mockSession.sparkContext.appName).thenReturn("test-app")
    
    mockSession
  }
}

class BoardDataUtilTest extends AnyFunSuite with MockitoSugar with MockSparkSessionProvider {
  
  // Sobreescribimos el spark con el mock configurado
  private val testSpark = spark
  
  test("Prueba con spark mockeado") {
    // Configurar comportamiento específico cuando sea necesario
    when(testSpark.sqlContext.sql(anyString)).thenReturn(mock[DataFrame])
    
    // Ejecutar tu prueba
    val result = BoardDataUtil.spark.sql("SELECT 1")
    
    // Verificaciones
    verify(testSpark.sqlContext).sql("SELECT 1")
  }
}
