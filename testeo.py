package com.tuempresa.poi;

import org.apache.poi.EncryptedDocumentException;
import org.apache.poi.EmptyFileException;
import org.apache.poi.openxml4j.exceptions.OLE2NotOfficeXmlFileException;
import org.apache.poi.poifs.filesystem.DirectoryNode;
import org.apache.poi.poifs.filesystem.POIFSFileSystem;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.apache.poi.util.FileMagic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Callable;

/**
 * Envoltura de WorkbookFactory con logging exhaustivo.
 * No cierra los streams que recibe (responsabilidad del llamador).
 */
public final class WorkbookFactoryLogged {

    private static final Logger log = LoggerFactory.getLogger(WorkbookFactoryLogged.class);

    private WorkbookFactoryLogged() {}

    /* ----------------------------- Sobrecargas públicas ----------------------------- */

    public static Workbook create(InputStream in) throws IOException, EncryptedDocumentException {
        return create(in, null);
    }

    public static Workbook create(InputStream in, String password) throws IOException, EncryptedDocumentException {
        long t0 = System.nanoTime();
        log.info("create(InputStream, password? {}) - inicio", password != null);

        // 1) Preparar stream para detectar FileMagic con mark/reset seguro
        InputStream is = prepareForMagic(in);

        // 2) Chequeo de archivo vacío (sin consumir el stream al llamador)
        is.mark(1);
        int first = is.read();
        if (first == -1) {
            log.warn("Stream vacío - lanzando EmptyFileException");
            throw new EmptyFileException();
        }
        is.reset();

        // 3) Detectar tipo de fichero
        FileMagic fm = FileMagic.valueOf(is);
        log.info("FileMagic detectado: {}", fm);

        try {
            if (fm == FileMagic.OOXML) {
                // XLSX / XSSF
                return withTiming("crear OOXML desde InputStream", () ->
                        org.apache.poi.ss.usermodel.WorkbookFactory.create(is)
                );

            } else if (fm == FileMagic.OLE2) {
                // XLS binario / HSSF o paquete encriptado
                POIFSFileSystem poifs = new POIFSFileSystem(is);
                DirectoryNode root = poifs.getRoot();

                boolean isEncryptedPackage =
                        root.hasEntryCaseInsensitive("EncryptedPackage")
                        || root.hasEntryCaseInsensitive("Package"); // OOXML empaquetado en OLE

                log.debug("OLE2: isEncryptedPackage? {}", isEncryptedPackage);

                if (isEncryptedPackage) {
                    return withTiming("crear OOXML empaquetado (OLE2) " + (password != null ? "con" : "sin") + " password", () ->
                            org.apache.poi.ss.usermodel.WorkbookFactory.create(root, password)
                    );
                } else {
                    return withTiming("crear OLE2 clásico (HSSF)", () ->
                            org.apache.poi.ss.usermodel.WorkbookFactory.create(poifs)
                    );
                }

            } else {
                // Otros tipos poco comunes
                String msg = "Tipo de fichero no soportado por WorkbookFactory: " + fm;
                log.error(msg);
                throw new IOException(msg);
            }

        } catch (OLE2NotOfficeXmlFileException e) {
            log.error("OLE2 detectado pero no es un Office XML válido (¿corrupción o extensión incorrecta?).", e);
            throw e;
        } catch (EncryptedDocumentException e) {
            log.error("El documento parece estar encriptado y requiere password.", e);
            throw e;
        } catch (IOException e) {
            log.error("IO exception creando Workbook.", e);
            throw e;
        } catch (RuntimeException e) {
            log.error("Excepción no controlada creando Workbook.", e);
            throw e;
        } finally {
            long ms = (System.nanoTime() - t0) / 1_000_000;
            log.info("create(InputStream, ...) - fin en {} ms", ms);
        }
    }

    /** Sobrecarga cómoda para File/Path, con logs */
    public static Workbook create(Path path) throws IOException, EncryptedDocumentException {
        log.info("create(Path={})", path);
        try (InputStream in = Files.newInputStream(path)) {
            return create(in);
        }
    }

    public static Workbook create(Path path, String password) throws IOException, EncryptedDocumentException {
        log.info("create(Path={}, password? {})", path, password != null);
        try (InputStream in = Files.newInputStream(path)) {
            return create(in, password);
        }
    }

    public static Workbook create(File file) throws IOException, EncryptedDocumentException {
        return create(file.toPath());
    }

    public static Workbook create(File file, String password) throws IOException, EncryptedDocumentException {
        return create(file.toPath(), password);
    }

    /* ----------------------------- Helpers privados ----------------------------- */

    /** Asegura buffering y soporte de mark/reset con un buffer razonable. */
    private static InputStream prepareForMagic(InputStream in) {
        InputStream wrapped = (in instanceof BufferedInputStream) ? in : new BufferedInputStream(in, 8 * 1024);
        if (!wrapped.markSupported()) {
            log.debug("mark/reset no soportado -> envolviendo en BufferedInputStream");
            wrapped = new BufferedInputStream(wrapped, 8 * 1024);
        }
        return wrapped;
    }

    /** Mide tiempo de ejecución y añade logs alrededor de la operación suministrada. */
    private static <T> T withTiming(String label, Callable<T> op) throws IOException, EncryptedDocumentException {
        long t0 = System.nanoTime();
        log.debug("{} - inicio", label);
        try {
            return op.call();
        } catch (EncryptedDocumentException | IOException e) {
            log.error("{} - error controlado", label, e);
            throw e;
        } catch (Exception e) {
            log.error("{} - error no controlado", label, e);
            // reempaquetar para no cambiar la firma
            if (e instanceof RuntimeException re) throw re;
            throw new RuntimeException(e);
        } finally {
            long ms = (System.nanoTime() - t0) / 1_000_000;
            log.debug("{} - fin en {} ms", label, ms);
        }
    }
}
