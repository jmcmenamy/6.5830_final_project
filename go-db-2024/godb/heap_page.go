package godb

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

var DEBUGHEAPPAGE = false

func DebugHeapPage(format string, a ...any) (int, error) {
	if DEBUGHEAPPAGE || GLOBALDEBUG {
		return fmt.Println(fmt.Sprintf(format, a...))
	}
	return 0, nil
}

/* HeapPage implements the Page interface for pages of HeapFiles. We have
provided our interface to HeapPage below for you to fill in, but you are not
required to implement these methods except for the three methods that the Page
interface requires.  You will want to use an interface like what we provide to
implement the methods of [HeapFile] that insert, delete, and iterate through
tuples.

In GoDB all tuples are fixed length, which means that given a TupleDesc it is
possible to figure out how many tuple "slots" fit on a given page.

In addition, all pages are PageSize bytes.  They begin with a header with a 32
bit integer with the number of slots (tuples), and a second 32 bit integer with
the number of used slots.

Each tuple occupies the same number of bytes.  You can use the go function
unsafe.Sizeof() to determine the size in bytes of an object.  So, a GoDB integer
(represented as an int64) requires unsafe.Sizeof(int64(0)) bytes.  For strings,
we encode them as byte arrays of StringLength, so they are size
((int)(unsafe.Sizeof(byte('a')))) * StringLength bytes.  The size in bytes  of a
tuple is just the sum of the size in bytes of its fields.

Once you have figured out how big a record is, you can determine the number of
slots on on the page as:

remPageSize = PageSize - 8 // bytes after header
numSlots = remPageSize / bytesPerTuple //integer division will round down

To serialize a page to a buffer, you can then:

write the number of slots as an int32
write the number of used slots as an int32
write the tuples themselves to the buffer

You will follow the inverse process to read pages from a buffer.

Note that to process deletions you will likely delete tuples at a specific
position (slot) in the heap page.  This means that after a page is read from
disk, tuples should retain the same slot number. Because GoDB will never evict a
dirty page, it's OK if tuples are renumbered when they are written back to disk.

*/

type heapPage struct {
	// TODO: some code goes here
	NumSlots        int
	NumUsedSlots    int
	NextInsertIndex int
	Desc            *TupleDesc
	Tuples          []*Tuple
	FreeIndices     map[int]bool
	PageNo          int
	File            *HeapFile
	Dirty           bool
}

func (h *heapPage) checkRep() error {
	if h.NumSlots != h.NumUsedSlots+len(h.FreeIndices)+(h.NumSlots-h.NextInsertIndex) ||
		(h.NumUsedSlots == h.NumSlots && h.NextInsertIndex != h.NumSlots) {
		return GoDBError{RepInvariantViolated, fmt.Sprintf("Rep invariant violated for heap page %v", *h)}
	}
	return nil
}

// Construct a new heap page
func newHeapPage(desc *TupleDesc, pageNo int, f *HeapFile) (*heapPage, error) {
	// TODO: some code goes here
	h := &heapPage{
		Desc:            desc,
		PageNo:          pageNo,
		File:            f,
		NumSlots:        f.numSlots,
		Tuples:          make([]*Tuple, f.numSlots),
		FreeIndices:     make(map[int]bool),
		Dirty:           false,
		NextInsertIndex: 0,
	}
	return h, h.checkRep()
}

func (h *heapPage) getNumSlots() int {
	// TODO: some code goes here
	return h.NumSlots
}

// Insert the tuple into a free slot on the page, or return an error if there are
// no free slots.  Set the tuples rid and return it.
func (h *heapPage) insertTuple(t *Tuple) (recordID, error) {
	// TODO: some code goes here
	err := h.checkRep()
	if err != nil {
		return nil, err
	}

	for idx := range h.FreeIndices {
		t.Rid = &recordIDImpl{
			pageNo: h.PageNo,
			slotNo: idx,
		}
		h.Tuples[idx] = t
		h.setDirty(0, true)
		delete(h.FreeIndices, idx)
		h.NumUsedSlots++
		return t.Rid, h.checkRep()
	}

	if h.NumUsedSlots == h.NumSlots {
		return nil, GoDBError{PageFullError, fmt.Sprintf("Page has reached capacity with %v values", h.NumUsedSlots)}
	}

	t.Rid = &recordIDImpl{
		pageNo: h.PageNo,
		slotNo: h.NextInsertIndex,
	}
	h.Tuples[h.NextInsertIndex] = t
	h.setDirty(0, true)
	h.NextInsertIndex++
	h.NumUsedSlots++
	DebugHeapPage("inserting tuple page %v has %v used slots out of %v\n", h.PageNo, h.NumUsedSlots, h.NumSlots)
	return t.Rid, h.checkRep()
}

// Delete the tuple at the specified record ID, or return an error if the ID is
// invalid.
func (h *heapPage) deleteTuple(rid recordID) error {
	// TODO: some code goes here
	err := h.checkRep()
	if err != nil {
		return err
	}

	ridPtr, ok := rid.(*recordIDImpl)
	if !ok {
		return GoDBError{IncompatibleTypesError, fmt.Sprintf("Couldn't convert rid %v into pointer to my record id impl", rid)}
	}

	h.NumUsedSlots--
	h.Tuples[ridPtr.slotNo] = nil
	h.FreeIndices[ridPtr.slotNo] = true
	return h.checkRep()
}

// Page method - return whether or not the page is dirty
func (h *heapPage) isDirty() bool {
	// TODO: some code goes here
	return h.Dirty
}

// Page method - mark the page as dirty
func (h *heapPage) setDirty(tid TransactionID, dirty bool) {
	// TODO: some code goes here
	h.Dirty = dirty
}

// Page method - return the corresponding HeapFile
// for this page.
func (p *heapPage) getFile() DBFile {
	// TODO: some code goes here
	return p.File
}

// Allocate a new bytes.Buffer and write the heap page to it. Returns an error
// if the write to the the buffer fails. You will likely want to call this from
// your [HeapFile.flushPage] method.  You should write the page header, using
// the binary.Write method in LittleEndian order, followed by the tuples of the
// page, written using the Tuple.writeTo method.
func (h *heapPage) toBuffer() (*bytes.Buffer, error) {
	// TODO: some code goes here
	b := new(bytes.Buffer)

	err := binary.Write(b, binary.LittleEndian, int32(h.NumSlots))
	if err != nil {
		return b, err
	}

	err = binary.Write(b, binary.LittleEndian, int32(h.NumUsedSlots))
	if err != nil {
		return b, err
	}

	writtenTuples := 0
	for _, tuple := range h.Tuples {
		if tuple == nil {
			continue
		}

		err = tuple.writeTo(b)
		if err != nil {
			return b, err
		}
		writtenTuples++
	}

	if writtenTuples != h.NumUsedSlots {
		return b, GoDBError{RepInvariantViolated, fmt.Sprintf("Wrote %v tuples to buffer, but numUsedSlots is %v. heapPage: %v", writtenTuples, h.NumUsedSlots, *h)}
	}

	tuplesToPad := h.NumSlots - writtenTuples
	bytesToPad := PageSize - b.Len()

	if tuplesToPad*h.File.tupleSize+(PageSize-HeaderSize-h.File.tupleSize*h.NumSlots) != bytesToPad {
		return b, GoDBError{RepInvariantViolated, fmt.Sprintf("Bytes mismatch tuples to pad is %v tuples size is %v num slots is %v bytes to pad is %v but left side is %v", tuplesToPad, h.File.tupleSize, h.NumSlots, bytesToPad, tuplesToPad*h.File.tupleSize+(PageSize-HeaderSize-h.File.tupleSize*h.NumSlots))}
	}
	if bytesToPad > 0 {
		emptyTuple := make([]byte, bytesToPad)
		binary.Write(b, binary.LittleEndian, emptyTuple)
	}

	return b, nil
}

// Read the contents of the HeapPage from the supplied buffer.
func (h *heapPage) initFromBuffer(buf *bytes.Buffer) error {
	// TODO: some code goes here
	var numSlots int32
	err := binary.Read(buf, binary.LittleEndian, &numSlots)
	if err != nil {
		return err
	}
	h.NumSlots = int(numSlots)

	var numUsedSlots int32
	err = binary.Read(buf, binary.LittleEndian, &numUsedSlots)
	if err != nil {
		return err
	}
	h.NumUsedSlots = int(numUsedSlots)

	DebugHeapPage("init from buffer page %v num used slots is %v\n", h.PageNo, h.NumUsedSlots)

	for i := 0; i < h.NumUsedSlots; i++ {
		tuple, err := readTupleFrom(buf, h.Desc)
		if err != nil {
			return err
		}
		h.Tuples[i] = tuple
		tuple.Rid = &recordIDImpl{
			pageNo: h.PageNo,
			slotNo: i,
		}
	}

	h.NextInsertIndex = h.NumUsedSlots

	return h.checkRep()
}

// Return a function that iterates through the tuples of the heap page.  Be sure
// to set the rid of the tuple to the rid struct of your choosing beforing
// return it. Return nil, nil when the last tuple is reached.
func (p *heapPage) tupleIter() func() (*Tuple, error) {
	// TODO: some code goes here
	i := 0
	return func() (*Tuple, error) {
		for i < p.NumSlots {
			curIdx := i
			i++
			if p.Tuples[curIdx] != nil {
				DebugHeapPage("not nil?? i is %v numslots is %v length is %v tuple is %v\n", curIdx, p.NumSlots, len(p.Tuples), *p.Tuples[curIdx])
				return p.Tuples[curIdx], p.checkRep()
			}
		}
		return nil, p.checkRep()
	}
}
