package ru.kostacie;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;


/**
 * Класс для работы с файловым хранилищем DataNode.
 * Позволяет сохранять, проверять существование файлов и загружать их.
 */


@Slf4j
public class FileStorage {
    private static final String STORAGE_DIR = "storage/";

    /**
     * Создаёт директорию для хранения файлов, если она не существует.
     *
     * @throws IOException если не удалось создать директорию.
     */
    public FileStorage() throws IOException {
        Files.createDirectories(Path.of(STORAGE_DIR));
    }

    /**
     * Проверяет, существует ли файл с данным ID.
     * Возвращает true, если файл существует, иначе false.
     * @param fileId ID файла.
     */
    public synchronized boolean fileExists(String fileId) {
        return Files.exists(Path.of(STORAGE_DIR, fileId));
    }

    /**
     * Сохраняет файл.
     *
     * @param fileId  ID файла.
     * @param content контент файла.
     * @throws IOException если файл уже существует или возникла ошибка при записи.
     */
    public synchronized void saveFile(String fileId, byte[] content) throws IOException {
        Path file = Path.of(STORAGE_DIR, fileId);
        if (Files.exists(file)) {
            throw new IOException("File already exists: " + fileId);
        }
        Files.write(file, content, StandardOpenOption.CREATE_NEW);
        log.info("File saved: {}", file);
    }

    /**
     * Загружает файл из хранилища.
     * Возвращает контент файла.
     *
     * @param fileId ID файла.
     * @throws IOException если файл не найден.
     */
    public synchronized byte[] getFile(String fileId) throws IOException {
        Path filePath = Path.of(STORAGE_DIR, fileId);
        if (!Files.exists(filePath)) {
            throw new IOException("File not found: " + fileId);
        }
        return Files.readAllBytes(filePath);
    }
}

