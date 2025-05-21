import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.mockito.Mockito._
import org.apache.spark.sql.{DataFrame, SQLContext, Row}
import org.apache.spark.sql.functions._
import scala.collection.mutable

class NotificationUtilTest extends AnyFunSuite with BeforeAndAfterEach {

  var sqlContextMock: SQLContext = _
  var usersTable: DataFrame = _
  var dfUsersAndRols: DataFrame = _
  var userUnidad: DataFrame = _
  var usersEmisores: DataFrame = _
  var usersEmisoresFiltered: DataFrame = _
  var usersEmisoresDefault: DataFrame = _
  var rowEmisor: Row = _
  var rowEmisorDefault: Row = _

  override def beforeEach(): Unit = {
    sqlContextMock = mock(classOf[SQLContext])
    usersTable = mock(classOf[DataFrame])
    dfUsersAndRols = mock(classOf[DataFrame])
    userUnidad = mock(classOf[DataFrame])
    usersEmisores = mock(classOf[DataFrame])
    usersEmisoresFiltered = mock(classOf[DataFrame])
    usersEmisoresDefault = mock(classOf[DataFrame])
    rowEmisor = mock(classOf[Row])
    rowEmisorDefault = mock(classOf[Row])
  }

  test("getEmisorNotification debe devolver null cuando no encuentra emisor") {
    when(sqlContextMock.table("staging_db.users_ecresearch")).thenReturn(usersTable)
    when(usersTable.where(any[Column])).thenReturn(dfUsersAndRols)
    when(dfUsersAndRols.select(any[Seq[Column]]: _*)).thenReturn(dfUsersAndRols)
    when(dfUsersAndRols.where(any[Column])).thenReturn(userUnidad)
    when(userUnidad.where(any[Column])).thenReturn(userUnidad)
    when(userUnidad.limit(anyInt())).thenReturn(userUnidad)
    when(userUnidad.collect()).thenReturn(Array.empty[Row])

    val resultado = NotificationUtil.getEmisorNotification(sqlContextMock, "staging_db", "TEST_UNIT")
    assert(resultado == null)
  }

  test("getEmisorNotification debe devolver un emisor válido cuando existe") {
    when(rowEmisor.getAs[String]("user_email")).thenReturn("Emisor@Test.com")
    when(sqlContextMock.table("staging_db.users_ecresearch")).thenReturn(usersTable)
    when(usersTable.where(any[Column])).thenReturn(dfUsersAndRols)
    when(dfUsersAndRols.select(any[Seq[Column]]: _*)).thenReturn(dfUsersAndRols)
    when(dfUsersAndRols.where(any[Column])).thenReturn(userUnidad)
    when(userUnidad.where(any[Column])).thenReturn(userUnidad)
    when(userUnidad.limit(anyInt())).thenReturn(userUnidad)
    when(userUnidad.collect()).thenReturn(Array(rowEmisor))

    val resultado = NotificationUtil.getEmisorNotification(sqlContextMock, "staging_db", "TEST_UNIT")
    assert(resultado == "emisor@test.com")
  }

  test("getEmisorNotificationWithDefault debe devolver emisor de unidad/entidad cuando existe") {
    when(rowEmisor.getAs[String]("user_email")).thenReturn("emisor.unidad@test.com")
    when(sqlContextMock.table("staging_db.users_ecresearch")).thenReturn(usersEmisores)
    when(usersEmisores.select(any[Seq[Column]]: _*)).thenReturn(usersEmisores)
    when(usersEmisores.where(any[Column])).thenReturn(usersEmisoresFiltered)
    when(usersEmisoresFiltered.limit(anyInt())).thenReturn(usersEmisoresFiltered)
    when(usersEmisoresFiltered.collect()).thenReturn(Array(rowEmisor))

    val resultado = NotificationUtil.getEmisorNotificationWithDefault(sqlContextMock, "staging_db", "TEST_UNIT", "TEST_ENTITY")
    assert(resultado == "emisor.unidad@test.com")
  }

  test("getEmisorNotificationWithDefault debe devolver emisor por defecto cuando no existe específico") {
    when(rowEmisorDefault.getAs[String]("user_email")).thenReturn("emisor.default@test.com")
    when(sqlContextMock.table("staging_db.users_ecresearch")).thenReturn(usersEmisores)
    when(usersEmisores.select(any[Seq[Column]]: _*)).thenReturn(usersEmisores)
    when(usersEmisores.where(any[Column])).thenReturn(usersEmisoresFiltered)
    when(usersEmisoresFiltered.limit(anyInt())).thenReturn(usersEmisoresFiltered)
    when(usersEmisoresFiltered.collect()).thenReturn(Array.empty[Row])

    when(usersEmisoresFiltered.where(any[Column])).thenReturn(usersEmisoresDefault)
    when(usersEmisoresDefault.limit(anyInt())).thenReturn(usersEmisoresDefault)
    when(usersEmisoresDefault.collect()).thenReturn(Array(rowEmisorDefault))

    val resultado = NotificationUtil.getEmisorNotificationWithDefault(sqlContextMock, "staging_db", "TEST_UNIT", "TEST_ENTITY")
    assert(resultado == "emisor.default@test.com")
  }

} 
