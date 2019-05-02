package tech.wendt.dbmigrate.impl;

import tech.wendt.dbmigrate.Migration;
import tech.wendt.dbmigrate.MigrationLoader;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ResourceMigrationLoader implements MigrationLoader {

    private static final String FILEPATTERN_UP = "_up.sql";
    private static final String FILEPATTERN_DOWN = "_down.sql";
    private static final String FILEPATTERN_REGEX = "\\d+_{2}[^.]+_(down|up)\\.sql";
    private Path migrationPath;

    public ResourceMigrationLoader(String resourcePath) throws IOException {
        resourcePath = Objects.requireNonNull(resourcePath);
        if (!(resourcePath.startsWith("/"))) {
            resourcePath = "/".concat(resourcePath);
        }
        URL resource = ResourceMigrationLoader.class.getResource(resourcePath);
        if (resource != null) {
            try {
                final Map<String, String> env = new HashMap<>();
                final String[] array = resource.toString().split("!");
                if (array.length > 1) {
                    final FileSystem fs = FileSystems.newFileSystem(URI.create(array[0]), env);
                    this.migrationPath = fs.getPath(array[1]);
                } else {
                    this.migrationPath = Paths.get(resource.toURI());
                }
            } catch (URISyntaxException e) {
                throw new IOException("Could not read migrations", e);
            }
        } else {
            throw new IOException(String.format("Resource directory %s does not exist", resourcePath));
        }
    }

    @Override
    public List<Migration> loadMigrations() throws IOException {
        try (Stream<Path> paths = Files.walk(migrationPath)) {
            List<Path> files = paths
                    .collect(Collectors.toList());
            List<Path> migrations = files.stream()
                    .filter(file -> file.toString().endsWith(FILEPATTERN_UP))
                    .collect(Collectors.toList());
            List<Path> rollbacks = files.stream()
                    .filter(file -> file.toString().endsWith(FILEPATTERN_DOWN))
                    .collect(Collectors.toList());
            if (!migrations.isEmpty()) {
                List<Migration> result = createMigrations(migrations, rollbacks);
                if (rollbacks.isEmpty()) {
                    return result;
                } else {
                    throw new IOException("Down script without matching up script found");
                }
            } else {
                throw new IOException("No migrations found");
            }
        }
    }

    private List<Migration> createMigrations(List<Path> migrations, List<Path> rollbacks) throws IOException {
        List<Migration> result = new ArrayList<>();
        for (int i = 0; i < migrations.size(); ++i) {
            Migration migration = createMigration(migrations.get(i), rollbacks);
            result.add(migration);
        }
        return result.stream()
                .sorted(Comparator.comparingInt(Migration::getOrder))
                .collect(Collectors.toList());
    }

    private Migration createMigration(Path migrationPath, List<Path> rollbacks) throws IOException {
        String migrationFile = migrationPath.getFileName().toString();
        if (migrationFile.matches(FILEPATTERN_REGEX)) {
            String fullMigrationName = migrationFile.substring(0, migrationFile.length() - FILEPATTERN_UP.length());
            Optional<Path> rollback = rollbacks.stream()
                    .filter(path -> path
                            .getFileName()
                            .toString()
                            .startsWith(fullMigrationName))
                    .findAny();
            String[] migrationNameArray = fullMigrationName.split("__");
            int order = Integer.parseInt(migrationNameArray[0]);
            String migrationName = migrationNameArray[1];

            String upSql = Files.readAllLines(migrationPath)
                    .stream()
                    .reduce(String::concat)
                    .orElseThrow(() -> new IOException(String.format("Could not read migration: %s", migrationPath)));
            if (rollback.isPresent()) {
                Path rollbackPath = rollback.get();
                rollbacks.remove(rollback.get());
                String downSql = Files.readAllLines(rollbackPath)
                        .stream()
                        .reduce(String::concat)
                        .orElseThrow(() -> new IOException(String.format("Could not read rollback: %s", rollbackPath)));
                return new Migration(migrationName, order, upSql, downSql);
            } else {
                return new Migration(migrationName, order, upSql);
            }
        } else {
            throw new IOException(String.format("migration file %s does not match regex %s",
                    migrationFile, FILEPATTERN_REGEX));
        }
    }
}
