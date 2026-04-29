package dbMigration.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
        int partNumber = 1;
        long bytesWrittenInPart = 0L;
        BufferedWriter writer = null;
        byte[] newlineBytes = "\n".getBytes(StandardCharsets.UTF_8);

        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(new FileInputStream(sourceFile), StandardCharsets.UTF_8))) {
            String header = reader.readLine();
            if (header == null) {
                return;
            }

            byte[] headerBytes = header.getBytes(StandardCharsets.UTF_8);
            String line;
            while ((line = reader.readLine()) != null) {
                byte[] lineBytes = line.getBytes(StandardCharsets.UTF_8);
                long entrySize = lineBytes.length + newlineBytes.length;

                if (writer == null || (bytesWrittenInPart > 0 && bytesWrittenInPart + entrySize > maxPartSize)) {
                    if (writer != null) {
                        writer.close();
                    }
                    File partFile = new File(targetFolder,
                            sourceFile.getName().replace(".csv", "") + ".part" + partNumber + ".csv");
                    writer = new BufferedWriter(
                            new OutputStreamWriter(new FileOutputStream(partFile), StandardCharsets.UTF_8));
                    writer.write(header);
                    writer.newLine();
                    bytesWrittenInPart = headerBytes.length + newlineBytes.length;
                    partNumber++;
                }

                writer.write(line);
                writer.newLine();
                bytesWrittenInPart += entrySize;
            }

            if (writer == null) {
                File partFile = new File(targetFolder,
                        sourceFile.getName().replace(".csv", "") + ".part" + partNumber + ".csv");
                writer = new BufferedWriter(
                        new OutputStreamWriter(new FileOutputStream(partFile), StandardCharsets.UTF_8));
                writer.write(header);
                writer.newLine();
            }
        } finally {
            if (writer != null) {
                writer.close();
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
