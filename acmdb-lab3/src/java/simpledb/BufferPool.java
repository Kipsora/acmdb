package simpledb;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 *
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /**
     * Default number of pages passed to the constructor. This is used by
     * other classes. BufferPool should use the numPages argument to the
     * constructor instead.
     */
    public static final int DEFAULT_PAGES = 50;
    /**
     * Bytes per page, including header.
     */
    private static final int PAGE_SIZE = 4096;
    private static int pageSize = PAGE_SIZE;

    private final int numPages;
    private final Map<PageId, Page> pageCache;
    private final Map<PageId, Integer> pageCount;
    private final Queue<PageId> pageOrder;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.numPages = numPages;
        this.pageCache = new HashMap<>();
        this.pageCount = new HashMap<>();
        this.pageOrder = new PriorityQueue<>(Comparator.comparingInt(pageCount::get));
    }

    public static int getPageSize() {
        return pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
        BufferPool.pageSize = pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
        BufferPool.pageSize = PAGE_SIZE;
    }

    public void putPageToCache(PageId pid, Page page) {
        if (pageCache.containsKey(pid)) {
            pageOrder.remove(pid);
            Integer tmp = pageCount.get(pid);
            pageCount.put(pid, tmp + 1);
            pageOrder.add(pid);
        } else {
            while (pageCache.size() >= numPages) {
                evictPage();
            }
            pageCache.put(pid, page);
            pageCount.put(pid, 1);
            pageOrder.add(pid);
        }
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid  the ID of the transaction requesting the page
     * @param pid  the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws DbException {
        // some code goes here
        Page hitPage = pageCache.getOrDefault(pid, null);
        if (hitPage != null) {
            return hitPage;
        }
        hitPage = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
        putPageToCache(pid, hitPage);
        return hitPage;
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Return true if the specified transaction has a lock on the specified page
     */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return false;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid    the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid     the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t       the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t) {
        // some code goes here
        // not necessary for lab1
        try {
            List<Page> pages = Database.getCatalog().getDatabaseFile(tableId).insertTuple(tid, t);
            for (Page page: pages) {
                page.markDirty(true, tid);
                getPage(tid, page.getId(), Permissions.READ_ONLY);
                putPageToCache(page.getId(), page);
            }
        } catch (DbException | IOException | TransactionAbortedException e) {
            e.printStackTrace();
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t   the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t) {
        // some code goes here
        // not necessary for lab1
        try {
            List<Page> pages = Database.getCatalog().getDatabaseFile(
                    t.getRecordId().getPageId().getTableId()).deleteTuple(tid, t);
            for (Page page: pages) {
                page.markDirty(true, tid);
                putPageToCache(page.getId(), page);
            }
        } catch (DbException | IOException | TransactionAbortedException e) {
            e.printStackTrace();
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     * break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() {
        // some code goes here
        // not necessary for lab1
        for (PageId pid: pageCache.keySet()) {
            try {
                flushPage(pid);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Remove the specific page id from the buffer pool.
     * Needed by the recovery manager to ensure that the
     * buffer pool doesn't keep a rolled back page in its
     * cache.
     * <p>
     * Also used by B+ tree files to ensure that deleted pages
     * are removed from the cache so they can be reused safely
     */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        pageCache.remove(pid);
        pageCount.remove(pid);
        pageOrder.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     *
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        Page page = pageCache.get(pid);
        if (page.isDirty() != null) {
            page.markDirty(false, null);
            Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(page);
        }
    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() {
        // some code goes here
        // not necessary for lab1
        PageId evictPageId = pageOrder.poll();
        try {
            flushPage(evictPageId);
        } catch (IOException e) {
            e.printStackTrace();
        }
        pageCache.remove(evictPageId);
        pageOrder.remove(evictPageId);
        pageCount.remove(evictPageId);
    }

}