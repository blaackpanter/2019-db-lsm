package ru.mail.polis.pranova;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.DAO;
import ru.mail.polis.Iters;
import ru.mail.polis.Record;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;


public final class LSMDao implements DAO {
    private static final String SUFFIX = ".dat";
    private static final String TEMP = ".tmp";
    private static final String PREFIX = "PRL";
    private Table memTable = new MemTable();
    private final File base;
    private int generation;
    private final long flushThreshold;
    private final Collection<FileTable> files;

    /**
     * LSM storage.
     *
     * @param base           is root directory
     * @param flushThreshold is max size of storage
     * @throws IOException if an I/O error is thrown by a visitor method
     */
    public LSMDao(@NotNull final File base,
                  @NotNull final long flushThreshold) throws IOException {
        this.base = base;
        assert flushThreshold >= 0L;
        this.flushThreshold = flushThreshold;
        files = new ArrayList<>();
        final EnumSet<FileVisitOption> options = EnumSet.of(FileVisitOption.FOLLOW_LINKS);
        final int maxDeep = 1;
        Files.walkFileTree(base.toPath(), options, maxDeep, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(final Path path, final BasicFileAttributes attrs) throws IOException {
                if (path.getFileName().toString().endsWith(SUFFIX)
                        && path.getFileName().toString().startsWith(PREFIX)) {
                    files.add(new FileTable(path.toFile()));
                }
                return FileVisitResult.CONTINUE;
            }
        });
        generation = files.size() + 1;
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) throws IOException {
        final List<Iterator<Cell>> filesIterators = new ArrayList<>();

        for (final FileTable fileTable : files) {
            filesIterators.add(fileTable.iterator(from));
        }

        filesIterators.add(memTable.iterator(from));
        final Iterator<Cell> alive = getCellsIterator(filesIterators);
        return Iterators.transform(alive, cell -> Record.of(cell.getKey(), cell.getValue().getData()));
    }

    @Override
    public void compact() throws IOException {
        final List<Iterator<Cell>> filesIterators = new ArrayList<>();

        for (final FileTable fileTable : files) {
            filesIterators.add(fileTable.iterator(ByteBuffer.allocate(0)));
        }

        final Iterator<Cell> alive = getCellsIterator(filesIterators);
        final File tmp = new File(base, PREFIX + 1 + TEMP);
        FileTable.write(alive, tmp);

        for (final FileTable fileTable : files) {
            fileTable.deleteFileTable();
        }

        files.clear();
        final File dest = new File(base, PREFIX + 1 + SUFFIX);
        Files.move(tmp.toPath(), dest.toPath(), StandardCopyOption.ATOMIC_MOVE);
        files.add(new FileTable(dest));
        generation = files.size() + 1;
    }

    private Iterator<Cell> getCellsIterator(@NotNull final List<Iterator<Cell>> iterators) {
        final Iterator<Cell> mergedCells = Iterators.mergeSorted(iterators, Cell.COMPARATOR);
        final Iterator<Cell> cells = Iters.collapseEquals(mergedCells, Cell::getKey);
        return Iterators.filter(cells, cell -> !cell.getValue().isRemoved());
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) throws IOException {
        memTable.upsert(key, value);
        if (memTable.sizeInBytes() > flushThreshold) {
            flush();
        }
    }

    private void flush() throws IOException {
        final File tmp = new File(base, PREFIX + generation + TEMP);
        FileTable.write(memTable.iterator(ByteBuffer.allocate(0)), tmp);
        final File dest = new File(base, PREFIX + generation + SUFFIX);
        Files.move(tmp.toPath(), dest.toPath(), StandardCopyOption.ATOMIC_MOVE);
        files.add(new FileTable(dest));
        generation++;
        memTable = new MemTable();
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        memTable.remove(key);
        if (memTable.sizeInBytes() > flushThreshold) {
            flush();
        }
    }

    @Override
    public void close() throws IOException {
        if (memTable.sizeInBytes() != 0) {
            flush();
        }
    }
}
