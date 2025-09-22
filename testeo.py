 public static Workbook create(InputStream inp, String password)
            throws IOException, EncryptedDocumentException {

        // Asegura buffering + mark/reset para FileMagic
        InputStream is = (inp instanceof BufferedInputStream)
                ? inp
                : new BufferedInputStream(inp, 8 * 1024);

        // Comprobación de vacío sin consumir el stream
        is.mark(1);
        if (is.read() == -1) {
            throw new EmptyFileException();
        }
        is.reset();

        // Detectar tipo de fichero
        is = FileMagic.prepareToCheckMagic(is);
        FileMagic fm = FileMagic.valueOf(is);

        if (fm == FileMagic.OOXML) {
            // XLSX (y similares). POI acepta password aquí si procede.
            return org.apache.poi.ss.usermodel.WorkbookFactory.create(is, password);

        } else if (fm == FileMagic.OLE2) {
            // XLS binario o paquete OOXML dentro de OLE (posible cifrado)
            POIFSFileSystem poifs = new POIFSFileSystem(is);
            DirectoryNode root = poifs.getRoot();

            boolean isOOXML =
                    root.hasEntryCaseInsensitive("EncryptedPackage")
                 || root.hasEntryCaseInsensitive("Package");

            if (isOOXML) {
                // OOXML empaquetado en OLE2 (usa password si hace falta)
                return org.apache.poi.ss.usermodel.WorkbookFactory.create(root, password);
            } else {
                // XLS clásico (HSSF). Aquí no aplica password en create(poifs).
                return org.apache.poi.ss.usermodel.WorkbookFactory.create(poifs);
            }

        } else {
            throw new IOException("Can't open workbook - unsupported file type: " + fm);
        }
    }
