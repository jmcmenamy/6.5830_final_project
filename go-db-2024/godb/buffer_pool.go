package godb

//BufferPool provides methods to cache pages that have been read from disk.
//It has a fixed capacity to limit the total amount of memory used by GoDB.
//It is also the primary way in which transactions are enforced, by using page
//level locking (you will not need to worry about this until lab3).

import (
	"fmt"
)

var DEBUGBUFFERPOOL = false

func DebugBufferPool(format string, a ...any) (int, error) {
	if DEBUGBUFFERPOOL || GLOBALDEBUG {
		return fmt.Println(fmt.Sprintf(format, a...))
	}
	return 0, nil
}

// Permissions used to when reading / locking pages
type RWPerm int

// This implements the LRU cache. The buffer pool will always keep pointers to the first and last items

// next is towards more recent
// prev is towards less recent
// doubly linked list :-)
type CacheItem struct {
	previousItem *CacheItem
	nextItem     *CacheItem
	page         Page
	pageKey      any
}

const (
	ReadPerm  RWPerm = iota
	WritePerm RWPerm = iota
)

// DBType is the type of a tuple field, in GoDB, e.g., IntType or StringType
type EvictionPolicy int

const (
	MRU EvictionPolicy = iota
	LRU EvictionPolicy = iota
)

type BufferPool struct {
	// TODO: some code goes here
	capacity         int
	numPages         int
	cacheHead        *CacheItem
	cacheTail        *CacheItem
	fileMap          map[any]*CacheItem
	evictionPolicy   EvictionPolicy
	getNum           int
	CanFlushWhenFull bool
}

// Create a new BufferPool with the specified number of pages
func NewBufferPool(numPages int) (*BufferPool, error) {
	return &BufferPool{
		capacity:       numPages,
		numPages:       0,
		cacheHead:      nil,
		cacheTail:      nil,
		fileMap:        make(map[any]*CacheItem),
		evictionPolicy: MRU,
	}, nil
}

func (bp *BufferPool) checkRep() error {

	if bp.numPages > bp.capacity ||
		bp.numPages < 0 ||
		(bp.cacheHead == nil && bp.cacheTail != nil) ||
		(bp.cacheTail == nil && bp.cacheHead != nil) ||
		(bp.numPages == 1 && (bp.cacheHead != bp.cacheTail || bp.cacheHead == nil)) ||
		(bp.numPages > 1 && (bp.cacheHead == bp.cacheTail)) ||
		(bp.numPages == 0 && (bp.cacheHead != nil || bp.cacheTail != nil)) ||
		(bp.numPages != 0 && (bp.cacheHead == nil || bp.cacheTail == nil)) ||
		(bp.numPages != len(bp.fileMap)) {
		DebugBufferPool("Throwing ERRS %v %v %v %v %v %v %v %v %v %v %v",
			bp.numPages > bp.capacity,
			bp.numPages < 0,
			(bp.cacheHead == nil && bp.cacheTail != nil),
			(bp.cacheTail == nil && bp.cacheHead != nil),
			(bp.numPages == 1 && (bp.cacheHead != bp.cacheTail || bp.cacheHead == nil)),
			(bp.numPages > 1 && (bp.cacheHead == bp.cacheTail)),
			(bp.numPages == 0 && (bp.cacheHead != nil || bp.cacheTail != nil)),
			(bp.numPages != 0 && (bp.cacheHead == nil || bp.cacheTail == nil)),
			(bp.numPages != len(bp.fileMap)), bp.numPages, len(bp.fileMap))
		return GoDBError{RepInvariantViolated, fmt.Sprintf("cache rep invariant violated. Buffer pool %v", *bp)}
	}

	return nil
}

// Testing method -- iterate through all pages in the buffer pool
// and flush them using [DBFile.flushPage]. Does not need to be thread/transaction safe.
// Mark pages as not dirty after flushing them.
func (bp *BufferPool) FlushAllPages() {
	// TODO: some code goes here
	cacheItem := bp.cacheHead
	for cacheItem != nil {
		err := cacheItem.page.getFile().flushPage(cacheItem.page)
		if err != nil {
			DebugBufferPool("Trying to flush page, got err %v", err)
		}

		cacheItem.page.setDirty(0, false)
		cacheItem = cacheItem.previousItem
	}
}

// Abort the transaction, releasing locks. Because GoDB is FORCE/NO STEAL, none
// of the pages tid has dirtied will be on disk so it is sufficient to just
// release locks to abort. You do not need to implement this for lab 1.
func (bp *BufferPool) AbortTransaction(tid TransactionID) {
	// TODO: some code goes here
}

// Commit the transaction, releasing locks. Because GoDB is FORCE/NO STEAL, none
// of the pages tid has dirtied will be on disk, so prior to releasing locks you
// should iterate through pages and write them to disk.  In GoDB lab3 we assume
// that the system will not crash while doing this, allowing us to avoid using a
// WAL. You do not need to implement this for lab 1.
func (bp *BufferPool) CommitTransaction(tid TransactionID) {
	// TODO: some code goes here
}

// Begin a new transaction. You do not need to implement this for lab 1.
//
// Returns an error if the transaction is already running.
func (bp *BufferPool) BeginTransaction(tid TransactionID) error {
	// TODO: some code goes here
	return nil
}

// Retrieve the specified page from the specified DBFile (e.g., a HeapFile), on
// behalf of the specified transaction. If a page is not cached in the buffer pool,
// you can read it from disk uing [DBFile.readPage]. If the buffer pool is full (i.e.,
// already stores numPages pages), a page should be evicted.  Should not evict
// pages that are dirty, as this would violate NO STEAL. If the buffer pool is
// full of dirty pages, you should return an error. Before returning the page,
// attempt to lock it with the specified permission.  If the lock is
// unavailable, should block until the lock is free. If a deadlock occurs, abort
// one of the transactions in the deadlock. For lab 1, you do not need to
// implement locking or deadlock detection. You will likely want to store a list
// of pages in the BufferPool in a map keyed by the [DBFile.pageKey].
func (bp *BufferPool) GetPage(file DBFile, pageNo int, tid TransactionID, perm RWPerm) (Page, error) {
	bp.getNum += 1
	DebugBufferPool("get num is %v\n", bp.getNum)
	// TODO Some code goes here

	// Big beefy method, but is readable imo and would be more confusing to break it up, at least for me

	// try to catch errors proactively
	DebugBufferPool("Starting call to getpage for pageNo %v head is %p tail is %p num pages is %v capcity is %v\n", pageNo, bp.cacheHead, bp.cacheTail, bp.numPages, bp.capacity)
	err := bp.checkRep()
	if err != nil {
		DebugBufferPool("here22")
		return nil, err
	}

	// try to grab item from cache
	pageKey := file.pageKey(pageNo)
	cacheItem, ok := bp.fileMap[pageKey]

	// cache hit!!
	if ok {
		DebugBufferPool("cache hit for pageNo %v\n", pageKey)
		DebugBufferPool("got hit cacheItem is %p bp is %v\n", cacheItem, bp)
		// move cache item to the front

		// if already at front, just return
		if bp.cacheHead == cacheItem {
			if cacheItem.nextItem != nil {
				return nil, GoDBError{MalformedDataError, fmt.Sprintf("cache item %v is the head, but has non null next item %v", *cacheItem, *(cacheItem.nextItem))}
			}
			DebugBufferPool("here21")
			return cacheItem.page, bp.checkRep()
		}

		if cacheItem.nextItem == nil {
			return nil, GoDBError{MalformedDataError, fmt.Sprintf("cache item %v is not head, but has nil next item", *cacheItem)}
		}

		// if we're not at end of cache
		if cacheItem.previousItem != nil {
			if bp.cacheTail == cacheItem {
				return nil, GoDBError{MalformedDataError, fmt.Sprintf("cache item %v is the tail, but has non null previous item %v", *cacheItem, *(cacheItem.previousItem))}
			}
			cacheItem.previousItem.nextItem = cacheItem.nextItem
		}

		// if we're at end, mark next as end
		if bp.cacheTail == cacheItem {
			bp.cacheTail = cacheItem.nextItem
		}

		// move to front
		// shouldn't have to check that not at front
		cacheItem.nextItem.previousItem = cacheItem.previousItem
		cacheItem.nextItem = nil
		bp.cacheHead.nextItem = cacheItem
		cacheItem.previousItem = bp.cacheHead
		bp.cacheHead = cacheItem

		DebugBufferPool("here20\n")
		return cacheItem.page, bp.checkRep()
	}
	DebugBufferPool("Cache Miss for pageNo %v\n", pageKey)

	// cache miss, read page and load in cache
	page, err := file.readPage(pageNo)
	if err != nil {
		DebugBufferPool("here11 %v", err)

		return nil, err
	}
	DebugBufferPool("cache miss at get %v, need to evict is %v\n", bp.getNum, bp.numPages == bp.capacity)
	DebugBufferPool("read page for pageno %v", pageNo)

	return bp.AddPage(page, file, pageNo, tid, perm)
}

// Add page to we don't have to flush immediately when new page created
func (bp *BufferPool) AddPage(page Page, file DBFile, pageNo int, tid TransactionID, perm RWPerm) (Page, error) {
	// try to catch errors proactively
	bp.getNum += 1
	DebugBufferPool("Adding page, getnum is %v, need to evict is %v\n", bp.getNum, bp.numPages == bp.capacity)
	DebugBufferPool("Adding page %v numPages is %v capacity is %v file is %v\n", pageNo, bp.numPages, bp.capacity, file.pageKey(pageNo))
	DebugBufferPool("Starting call to getpage head is %p tail is %p num pages is %v capcity is %v", bp.cacheHead, bp.cacheTail, bp.numPages, bp.capacity)
	err := bp.checkRep()
	if err != nil {
		DebugBufferPool("here22")
		return nil, err
	}

	// create cacheItem, add to map
	pageKey := file.pageKey(pageNo)

	cacheItem := &CacheItem{
		nextItem:     nil,
		previousItem: bp.cacheHead,
		page:         page,
		pageKey:      pageKey,
	}
	bp.fileMap[pageKey] = cacheItem
	DebugBufferPool("Put item in cache for page %v %p %p %v", pageNo, cacheItem, bp.cacheHead, pageKey)

	// empty cache, initialize
	if bp.cacheHead == nil {
		bp.cacheHead = cacheItem
		bp.cacheTail = bp.cacheHead
		bp.numPages = 1
		return page, nil
	}

	// check if we must evict something from the map
	if bp.numPages == bp.capacity {
		DebugBufferPool("need to evict page %v %v\n", bp.numPages, len(bp.fileMap))
		switch bp.evictionPolicy {
		// evict the most recently used page. i.e. start from head of cache
		case MRU:
			pageToEvict := bp.cacheHead
			for pageToEvict != nil && pageToEvict.page.isDirty() {
				pageToEvict = pageToEvict.previousItem
			}

			// all pages were dirty, flush all then evict head
			if pageToEvict == nil {
				if !bp.CanFlushWhenFull {
					return nil, GoDBError{BufferPoolFullError, fmt.Sprintf("All %v pages were dirty", bp.numPages)}
				}
				DebugBufferPool("Flushing!!!!!")
				bp.FlushAllPages()
				pageToEvict = bp.cacheHead
				for pageToEvict != nil && pageToEvict.page.isDirty() {
					DebugBufferPool("In loop?")
					pageToEvict = pageToEvict.previousItem
				}
				DebugBufferPool("Uhh ok page to evict is now %v %v\n", pageToEvict, pageToEvict == nil)
			}

			if pageToEvict == nil {
				DebugBufferPool("Bruh %v %v\n", bp.cacheHead, bp.cacheHead.page.isDirty())
				return nil, GoDBError{BufferPoolFullError, fmt.Sprintf("Couldn't find page to evict %v", bp.numPages)}
			}

			// remove from cache and write to disk
			_, ok := bp.fileMap[pageToEvict.pageKey]
			if !ok {
				DebugBufferPool("uh page key not in filemap %v", pageToEvict.pageKey)
				return nil, GoDBError{BufferPoolFullError, fmt.Sprintf("pageKey %v not in map", pageToEvict.pageKey)}
			}
			delete(bp.fileMap, pageToEvict.pageKey)
			DebugBufferPool("Evicted page %v\n", pageToEvict.pageKey)
			pageToEvict.page.getFile().flushPage(pageToEvict.page)

			// happens if capacity = 1
			if pageToEvict == bp.cacheHead && pageToEvict == bp.cacheTail {
				bp.cacheHead = nil
				bp.cacheTail = nil
			} else if pageToEvict == bp.cacheHead {
				bp.cacheHead = bp.cacheHead.previousItem
				bp.cacheHead.nextItem = nil
				cacheItem.previousItem = bp.cacheHead
			} else if pageToEvict == bp.cacheTail {
				bp.cacheTail = bp.cacheTail.nextItem
				bp.cacheTail.previousItem = nil
			} else {
				pageToEvict.nextItem.previousItem = pageToEvict.previousItem
				pageToEvict.previousItem.nextItem = pageToEvict.nextItem
			}

			DebugBufferPool("evicted page %v %v %v", bp.numPages, len(bp.fileMap), pageToEvict.pageKey)
		case LRU:
			// TODO FIX THIS IF WANT TO USE IT FOR PERFORMANCE OPTIMIZATION
			delete(bp.fileMap, bp.cacheTail.pageKey)
			bp.cacheTail = bp.cacheTail.nextItem
			bp.cacheTail.previousItem = nil
		}
	} else {
		bp.numPages++
	}

	DebugBufferPool("ok here head is %p tail is %p item is %p num pages is %v len filemap is %v", bp.cacheHead, bp.cacheTail, cacheItem, bp.numPages, len(bp.fileMap))

	// put item at front of cache
	if bp.cacheHead != nil {
		bp.cacheHead.nextItem = cacheItem
	} else {
		bp.cacheTail = cacheItem
	}
	bp.cacheHead = cacheItem

	DebugBufferPool("ok here 2 head is %p tail is %p", bp.cacheHead, bp.cacheTail)

	return page, bp.checkRep()
}
