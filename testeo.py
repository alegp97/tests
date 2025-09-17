protected void convertExcelToJSON(String inputFile, String outputFile) throws Exception {
    org.apache.hadoop.fs.FileSystem fs = null;
    try {
        fs = HDFShandler.getNewFileSystem(inputFile);
        Path excelPath = new Path(inputFile);
        if (!fs.exists(excelPath)) {
            LOGGER.info("[ERESEARCH] - File not exists: " + excelPath);
            if (!fs.exists(new Path(outputFile))) {
                LOGGER.error("File must exist: []" + inputFile);
                throw new IOException("File must exist: " + inputFile);
            }
        } else {
            String timeSuffix = String.valueOf(Calendar.getInstance().getTimeInMillis());
            String processedFile = inputFile + ".processed_" + timeSuffix;
            String errorFile = inputFile + ".error_" + timeSuffix;

            Workbook wb = null;
            try {
                wb = WorkbookFactory.create(fs.open(excelPath));
                MaltsInfo config = obtainJsonConfig(wb);
                writeJsonConfig(outputFile, config);
                // Si todo va bien, movemos el archivo a procesados
                copyFile(inputFile, processedFile, true);
            } catch (InvalidFormatException e) {
                LOGGER.error("Invalid format for Excel file: " + inputFile, e);
                copyFile(inputFile, errorFile, true);
                throw new IOException("Invalid format for Excel file: " + inputFile, e);
            } catch (IOException e) {
                LOGGER.error("I/O error while processing Excel file: " + inputFile, e);
                copyFile(inputFile, errorFile, true);
                throw new IOException("I/O error while processing Excel file: " + inputFile, e);
            } catch (Exception e) {
                LOGGER.error("Unexpected error while processing Excel file: " + inputFile, e);
                copyFile(inputFile, errorFile, true);
                throw new IOException("Unexpected error while processing Excel file: " + inputFile, e);
            } finally {
                if (wb != null) {
                    try {
                        wb.close();
                    } catch (Exception e) {
                        LOGGER.warn("Failed to close workbook", e);
                    }
                }
            }
        }
    } catch (IOException e) {
        LOGGER.error("Could not obtain configuration from excel file " + inputFile);
        throw new IOException("Could not obtain configuration from excel file " + inputFile, e);
    } finally {
        if (fs != null) {
            try {
                fs.close();
            } catch (Exception e) {
                LOGGER.error("Error closing file system");
            }
        }
    }
}
