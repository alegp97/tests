LOGGER.info("[ERESEARCH] - Entra convertExcelToJSON.else");
        String processedFile = inputFile + "_processed_" + Calendar.getInstance().getTimeInMillis();

        LOGGER.info("Intentando abrir el archivo: " + excelPath);

        // --- CLAVE: abrir y ENVOLVER el stream ---
        try (FSDataInputStream raw = fs.open(excelPath);
             BufferedInputStream bin = new BufferedInputStream(raw)) {

            // (Opcional) “sondeo” mínimo de lectura, ahora SÍ con mark/reset sobre el buffer:
            bin.mark(8192);
            int firstByte = bin.read();
            if (firstByte == -1) {
                LOGGER.error("El stream está vacío: " + excelPath);
                throw new IOException("Stream vacío: " + excelPath);
            } else {
                LOGGER.info("El stream NO está vacío: " + excelPath);
                bin.reset();
            }

            // (Opcional) leer unos bytes para comprobar acceso (sobre el buffer)
            byte[] buffer = new byte[4];
            int bytesRead = bin.read(buffer);
            if (bytesRead <= 0) {
                throw new IOException("No se pudo leer del archivo: " + excelPath);
            }
            LOGGER.info("Archivo abierto y leído correctamente, bytes leídos: " + bytesRead);

            // Volver al inicio antes de POI (porque hicimos lecturas)
            bin.reset(); // válido porque marcamos con 8192 antes

            LOGGER.info("[ERESEARCH] - Entra convertExcelToJSON.create");

            // Crear el Workbook desde el stream EN VUELTO
            try (Workbook wb = WorkbookFactory.create(bin)) {
                LOGGER.info("Workbook creado correctamente");
                MailsInfo config = obtainJsonConfig(wb);
                writeJsonConfig(outputFile, config);
            }
        }

        copyFile(inputFile, processedFile, true);
