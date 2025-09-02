package dbMigration.utils;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import dbMigration.MigrationApp;

public class FileManager {

    private static final Logger logger = LogManager.getLogger(FileManager.class);

    public static void splitFiles(File folder, long maxPartSize) throws IOException {
        if (!folder.isDirectory()) {
            throw new IllegalArgumentException("Provided file is not a directory: " + folder);
        }

        File tmpDir = new File(folder, "TMP");
        if (!tmpDir.exists() && !tmpDir.mkdirs()) {
            throw new IOException("Could not create TMP directory: " + tmpDir.getAbsolutePath());
        }

        File[] files = folder.listFiles((dir, name) -> new File(dir, name).isFile());
        if (files == null) {
            logger.info("No files");
            return;
        }

        logger.info("splitting files");

        for (File file : files) {
            splitFile(file, tmpDir, maxPartSize);
        }

        logger.info("Finished splitting");
    }

    private static void splitFile(File sourceFile, File targetFolder, long maxPartSize) throws IOException {
        byte[] buffer = new byte[8192]; // 8KB buffer
        int partNumber = 1;
        try (BufferedInputStream bis = new BufferedInputStream(new FileInputStream(sourceFile))) {
            long bytesReadInPart = 0;
            FileOutputStream fos = null;

            int bytesRead;
            while ((bytesRead = bis.read(buffer)) != -1) {
                if (fos == null) {
                    File partFile = new File(targetFolder,
                            sourceFile.getName().replace(".csv", "") + ".part" + partNumber + ".csv");
                    fos = new FileOutputStream(partFile);
                }

                if (bytesReadInPart + bytesRead > maxPartSize) {
                    // Write only the portion that fits in this part
                    int bytesToWrite = (int) (maxPartSize - bytesReadInPart);
                    fos.write(buffer, 0, bytesToWrite);
                    fos.close();


                    // Prepare next part
                    partNumber++;
                    File partFile = new File(targetFolder,
                            sourceFile.getName().replace(".csv", "") + ".part" + partNumber + ".csv");
                    fos = new FileOutputStream(partFile);

                    // Write remaining bytes to new part
                    fos.write(buffer, bytesToWrite, bytesRead - bytesToWrite);
                    bytesReadInPart = bytesRead - bytesToWrite;
                } else {
                    fos.write(buffer, 0, bytesRead);
                    bytesReadInPart += bytesRead;
                }
            }

            if (fos != null) {
                fos.close();
            }
        }
    }

    public static void eliminateTmpFiles(File directory) {
        if (directory.isDirectory()) {
            File[] files = directory.listFiles((dir, name) -> new File(dir, name).isFile());
            if (files == null)
                return;
            logger.info("deleting tmp");
            for (File file : files) {
                file.delete();
            }
        }
    }

}
