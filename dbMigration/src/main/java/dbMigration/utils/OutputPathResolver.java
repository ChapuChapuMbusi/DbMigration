package dbMigration.utils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public final class OutputPathResolver {

    private OutputPathResolver() {
    }

    public static Path resolveOutputDirectory(String configuredPath) throws IOException {
        if (configuredPath == null || configuredPath.isBlank()) {
            throw new IOException("data_dir is empty");
        }

        String expanded = configuredPath.startsWith("~")
                ? System.getProperty("user.home") + configuredPath.substring(1)
                : configuredPath;

        Path candidate = Path.of(expanded);
        if (!candidate.isAbsolute()) {
            candidate = Path.of(System.getProperty("user.dir")).resolve(candidate);
        } else if (!Files.exists(candidate) && expanded.startsWith(File.separator + "src" + File.separator)) {
            candidate = Path.of(System.getProperty("user.dir")).resolve(expanded.substring(1));
        }

        candidate = candidate.normalize();
        Files.createDirectories(candidate);
        return candidate;
    }
}
