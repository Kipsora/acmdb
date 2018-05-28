package simpledb;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden
 * @see simpledb.HeapPage#HeapPage
 */
public class HeapFile implements DbFile {
    private File file;
    private TupleDesc tupleDesc;

    public class HeapFileIterator implements DbFileIterator {
        private HeapFile file;
        private TransactionId tid;
        private int pageIndex;
        private Iterator<Tuple> tupleIterator;

        public HeapFileIterator(HeapFile f) {
            this.file = f;
            this.tid = tid;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            this.pageIndex = 0;
            if (file.numPages() == 0) {
                this.tupleIterator = new ArrayList<Tuple>().iterator();
            } else {
                this.tupleIterator = ((HeapPage) Database.getBufferPool().getPage(
                        tid, new HeapPageId(file.getId(), this.pageIndex), Permissions.READ_ONLY)).iterator();
            }
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (tupleIterator == null) {
                return false;
            }
            if (tupleIterator.hasNext()) {
                return true;
            } else if (pageIndex + 1 >= file.numPages()) {
                return false;
            } else {
                Iterator<Tuple> tmp = ((HeapPage) Database.getBufferPool().getPage(
                        tid, new HeapPageId(file.getId(), pageIndex + 1), Permissions.READ_ONLY)).iterator();
                return tmp.hasNext();
            }
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (tupleIterator == null) {
                throw new NoSuchElementException("Iterator is not open");
            }
            while (!tupleIterator.hasNext()) {
                if (pageIndex + 1 >= file.numPages()) {
                    throw new NoSuchElementException();
                }
                pageIndex += 1;
                tupleIterator = ((HeapPage) Database.getBufferPool().getPage(
                        tid, new HeapPageId(file.getId(), pageIndex), Permissions.READ_ONLY)).iterator();
            }
            return tupleIterator.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        @Override
        public void close() {
            tupleIterator = null;
        }
    }

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file = f;
        this.tupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        try {
            RandomAccessFile wrapper = new RandomAccessFile(file, "r");
            byte[] buffer = new byte[BufferPool.getPageSize()];
            wrapper.seek(pid.pageNumber() * BufferPool.getPageSize());
            wrapper.read(buffer);
            wrapper.close();
            return new HeapPage((HeapPageId) pid, buffer);
        } catch (IOException e) {
            e.printStackTrace();
        }
        throw new IllegalArgumentException();
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) Math.ceil(file.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(this);
    }

}

