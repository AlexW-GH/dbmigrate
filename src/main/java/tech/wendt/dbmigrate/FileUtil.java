package tech.wendt.dbmigrate;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

class FileUtil {
    static String getFileContent(String file) throws IOException {
        URL resource = FileUtil.class.getResource(file);
        try{
            return getFileContent(Paths.get(resource.toURI()));
        } catch (URISyntaxException e) {
            throw new IOException(String.format("Could not read file: %s", file), e);
        }
    }

    static String getFileContent(Path file) throws IOException {
        return Files.readAllLines(file)
                .stream()
                .reduce((left, right) -> left.concat(right))
                .orElseThrow(() -> new IOException(String.format("Could not read file content of %s", file.toString())));
    }
}
