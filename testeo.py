try {
    // 1. Crear el Workbook desde el archivo Excel
    Workbook wb = null;
    try {
        wb = WorkbookFactory.create(fs.open(excelPath));
    } catch (InvalidFormatException e) {
        LOGGER.error("Formato de archivo Excel no válido: " + inputFile, e);
        throw new IOException("Formato de archivo Excel no válido: " + inputFile, e);
    } catch (EncryptedDocumentException e) {
        LOGGER.error("El archivo Excel está encriptado: " + inputFile, e);
        throw new IOException("El archivo Excel está encriptado: " + inputFile, e);
    } catch (IOException e) {
        LOGGER.error("Error de E/S al leer el archivo Excel: " + inputFile, e);
        throw new IOException("Error de E/S al leer el archivo Excel: " + inputFile, e);
    } catch (Exception e) {
        LOGGER.error("Error inesperado al abrir el archivo Excel: " + inputFile, e);
        throw new IOException("Error inesperado al abrir el archivo Excel: " + inputFile, e);
    }

    // 2. Obtener la configuración JSON desde el Workbook
    MaltsInfo config = null;
    try {
        config = obtainJsonConfig(wb);
    } catch (IllegalArgumentException e) {
        LOGGER.error("Datos inválidos en el archivo Excel: " + inputFile, e);
        throw new IOException("Datos inválidos en el archivo Excel: " + inputFile, e);
    } catch (NullPointerException e) {
        LOGGER.error("Estructura de datos incorrecta en el archivo Excel: " + inputFile, e);
        throw new IOException("Estructura de datos incorrecta en el archivo Excel: " + inputFile, e);
    } catch (Exception e) {
        LOGGER.error("Error inesperado al procesar el archivo Excel: " + inputFile, e);
        throw new IOException("Error inesperado al procesar el archivo Excel: " + inputFile, e);
    }

    // 3. Escribir la configuración JSON en el archivo de salida
    try {
        writeJsonConfig(outputFile, config);
    } catch (IOException e) {
        LOGGER.error("Error de E/S al escribir el archivo JSON: " + outputFile, e);
        throw new IOException("Error de E/S al escribir el archivo JSON: " + outputFile, e);
    } catch (Exception e) {
        LOGGER.error("Error inesperado al escribir el archivo JSON: " + outputFile, e);
        throw new IOException("Error inesperado al escribir el archivo JSON: " + outputFile, e);
    }

} finally {
    // Asegurarse de cerrar el Workbook si está abierto
    if (wb != null) {
        try {
            wb.close();
        } catch (Exception e) {
            LOGGER.warn("Error al cerrar el Workbook: " + inputFile, e);
        }
    }
}
