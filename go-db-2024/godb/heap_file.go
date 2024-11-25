package godb

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"
)

var DEBUGHEAPFILE = false

func DebugHeapFile(format string, a ...any) (int, error) {
	if DEBUGHEAPFILE || GLOBALDEBUG {
		return fmt.Println(fmt.Sprintf(format, a...))
	}
	return 0, nil
}

// A HeapFile is an unordered collection of tuples.
//
// HeapFile is a public class because external callers may wish to instantiate
// database tables using the method [LoadFromCSV]
type HeapFile struct {
	// TODO: some code goes here
	// HeapFile should include the fields below;  you may want to add
	// additional fields
	bufPool            *BufferPool
	desc               *TupleDesc
	numSlots           int
	fileName           string
	metadataFileName   string
	loadedEntireFile   bool
	metadataFile       *os.File
	tupleSize          int
	numPages           int
	file               *os.File
	pagesWithFreeSpace map[int]bool
	numInserted        int
	offSetsLoaded      map[int64]bool
}

// Create a HeapFile.
// Parameters
// - fromFile: backing file for the HeapFile.  May be empty or a previously created heap file.
// - td: the TupleDesc for the HeapFile.
// - bp: the BufferPool that is used to store pages read from the HeapFile
// May return an error if the file cannot be opened or created.
func NewHeapFile(fromFile string, td *TupleDesc, bp *BufferPool, extraArgs ...string) (*HeapFile, error) {
	// TODO: some code goes here

	// awkward because of backwards compatibility
	metadataFileName := ""
	if len(extraArgs) > 0 {
		// fmt.Println("Extra args greater than 0\n")
		metadataFileName = extraArgs[0]
	}
	heapFile := &HeapFile{bufPool: bp, desc: td, fileName: fromFile, pagesWithFreeSpace: make(map[int]bool), metadataFileName: metadataFileName}
	// fmt.Printf("backing file is %v\n", metadataFileName)

	// calculate the number of slots and tuple size
	tupleSize := 0
	for _, fieldType := range td.Fields {
		switch fieldType.Ftype {
		case StringType:
			tupleSize += StringLength
		case IntType:
			tupleSize += Int64Length
		case FloatType:
			tupleSize += Float64Lengh
		}
	}

	// init the rest of the fields
	heapFile.numSlots = (PageSize - HeaderSize) / tupleSize
	heapFile.tupleSize = tupleSize
	file, err := os.OpenFile(fromFile, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	heapFile.file = file
	heapFile.offSetsLoaded = make(map[int64]bool)

	if metadataFileName != "" {
		metadataFile, err := os.OpenFile(metadataFileName, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			return nil, err
		}
		heapFile.metadataFile = metadataFile
		err = heapFile.ProcessMetadataFile(metadataFile)
		if err != nil {
			return nil, err
		}

	}

	return heapFile, nil //replace me
}

func (f *HeapFile) ProcessMetadataFile(file *os.File) error {
	// Define a buffer size for reading chunks (e.g., 4096 bytes = 4 KB)
	chunkSize := 4096
	buffer := make([]byte, chunkSize)
	start := time.Now()

	// Read the file in chunks
	for {
		// Read a chunk of data from the file
		n, err := file.Read(buffer)
		if err != nil && err.Error() != "EOF" {
			fmt.Println("Error reading file:", err)
			break
		}
		if n == 0 {
			break // End of file
		}

		// Process the chunk of data
		// processData(buffer[:n]) // Only pass the portion of the buffer that's been filled
		text := string(buffer[:n])
		numbers := strings.Split(text, ",")

		// Process the numbers as needed
		for _, num := range numbers {
			if num == "" {
				continue
			}
			convertedNum, err := strconv.ParseInt(num, 10, 64)
			if err != nil {
				return err
			}
			f.offSetsLoaded[convertedNum] = true
			// fmt.Println(num) // Example: Print each number
		}

		// If we didn't reach a full chunk, we're at the end of the file
		if err != nil && err.Error() == "EOF" {
			break
		}
	}
	duration := time.Since(start)
	fmt.Printf("Parsed metadatafile in %v seconds, len(f.offsetsLoaded) = %v\n", duration, len(f.offSetsLoaded))
	return nil
}

// Return the name of the backing file
func (f *HeapFile) BackingFile() string {
	// TODO: some code goes here
	return f.fileName
}

// Return the number of pages in the heap file
func (f *HeapFile) NumPages() int {
	// TODO: some code goes here
	return f.numPages
}

// Get the byte offset value for each line in the file
func getLineOffsets(file *os.File) []int64 {
	var offsets []int64
	scanner := bufio.NewScanner(file)
	offset := int64(0)
	for scanner.Scan() {
		offsets = append(offsets, offset)
		offset += int64(len(scanner.Bytes()) + 1) // +1 for newline character
	}

	return offsets
}

// Get a random sample of byte offsets
func getSampledOffsets(offsets []int64, sampleRate float32) []int64 {
	sampleSize := int(float32(len(offsets)) * sampleRate)
	sampledIndices := rand.Perm(len(offsets))[:sampleSize]

	sampledOffsets := make([]int64, sampleSize)
	for i, index := range sampledIndices {
		sampledOffsets[i] = offsets[index]
	}

	return sampledOffsets
}

// Convert a line read from a CSV file to a tuple and insert into heap file
func (f *HeapFile) loadLine(line string, sep string) error {
	fields := strings.Split(line, sep)
	numFields := len(fields)

	desc := f.Descriptor()
	if desc == nil || desc.Fields == nil {
		return GoDBError{MalformedDataError, "Descriptor was nil"}
	}
	if numFields != len(desc.Fields) {
		return GoDBError{MalformedDataError, fmt.Sprintf("LoadFromCSV:  line (%s) does not have expected number of fields (expected %d, got %d)", line, len(f.Descriptor().Fields), numFields)}
	}

	var newFields []DBValue
	for fno, field := range fields {
		switch f.Descriptor().Fields[fno].Ftype {
		case IntType:
			field = strings.TrimSpace(field)
			floatVal, err := strconv.ParseFloat(field, 64)
			if err != nil {
				return GoDBError{TypeMismatchError, fmt.Sprintf("LoadFromCSV: couldn't convert value %s to int", field)}
			}
			intValue := int(floatVal)
			newFields = append(newFields, IntField{int64(intValue)})
		case FloatType:
			field = strings.TrimSpace(field)
			floatVal, err := strconv.ParseFloat(field, 64)
			if err != nil {
				return GoDBError{TypeMismatchError, fmt.Sprintf("LoadFromCSV: couldn't convert value %s to int", field)}
			}
			floatValue := float64(floatVal)
			newFields = append(newFields, FloatField{floatValue})
		case StringType:
			if len(field) > StringLength {
				field = field[0:StringLength]
			}
			newFields = append(newFields, StringField{field})
		}
	}
	newT := Tuple{*f.Descriptor(), newFields, nil}
	tid := NewTID()
	err := f.insertTuple(&newT, tid)
	if err != nil {
		fmt.Printf("error with inserting tuple")
		return err
	}

	return nil
}

// Load the contents of a heap file from a specified CSV file.  Parameters are as follows:
// - hasHeader:  whether or not the CSV file has a header
// - sep: the character to use to separate fields
// - skipLastField: if true, the final field is skipped (some TPC datasets include a trailing separator on each line)
// Returns an error if the field cannot be opened or if a line is malformed
// We provide the implementation of this method, but it won't work until
// [HeapFile.insertTuple] and some other utility functions are implemented
func (f *HeapFile) LoadSomeFromCSV(file *os.File, hasHeader bool, sep string, skipLastField bool) error {
	if f.loadedEntireFile {
		return nil
	}
	f.bufPool.CanFlushWhenFull = true
	defer func() { f.bufPool.CanFlushWhenFull = false }()

	// offsets := getLineOffsets(file)

	samplingThreshold := 1000
	var sampleRate float64 = 0.01
	fileInfo, err := file.Stat()
	if err != nil {
		return err
	}
	newOffsetsLoaded := make(map[int64]bool)
	estimatedLinesInFile := int(fileInfo.Size()) / f.tupleSize
	if estimatedLinesInFile >= samplingThreshold {
		reader := bufio.NewReader(file)
		numSampledLines := 0
		for numSampledLines < int(sampleRate*float64(estimatedLinesInFile)) || len(f.offSetsLoaded) >= estimatedLinesInFile-100 {
			randomOffset := rand.Int63n(fileInfo.Size() - 1)
			file.Seek(randomOffset, io.SeekStart)
			_, err := reader.ReadString('\n')
			if err != nil {
				return fmt.Errorf("error reading line at byte offset %v", randomOffset)
			}
			newOffset, err := file.Seek(0, io.SeekCurrent)
			if err != nil {
				return err
			}
			if f.offSetsLoaded[newOffset] {
				continue
			}
			f.offSetsLoaded[newOffset] = true
			newOffsetsLoaded[newOffset] = true
			numSampledLines++

			line, err := reader.ReadString('\n')
			if err != nil {
				return fmt.Errorf("error reading line at byte offset %v", newOffset)
			}

			f.loadLine(line, sep)

			reader.Reset(file)
		}

		// sampledOffsets := getSampledOffsets(offsets, sampleRate)
		// for _, offset := range sampledOffsets {
		// 	file.Seek(offset, 0)
		// 	line, err := reader.ReadString('\n')
		// 	if err != nil {
		// 		return fmt.Errorf("error reading line at byte offset %v", offset)
		// 	}

		// 	f.loadLine(line, sep)

		// 	reader.Reset(file)
		// }
	} else {
		file.Seek(0, 0)
		scanner := bufio.NewScanner(file)

		for scanner.Scan() {
			line := scanner.Text()
			// Get the current file offset (position)
			offset, err := file.Seek(0, io.SeekCurrent)
			if err != nil {
				fmt.Println("Error getting file offset:", err)
				return err
			}
			if f.offSetsLoaded[offset] {
				continue
			}
			f.offSetsLoaded[offset] = true
			newOffsetsLoaded[offset] = true
			f.loadLine(line, sep)
		}
		f.loadedEntireFile = true
	}
	bp := f.bufPool
	// Force dirty pages to disk. CommitTransaction may not be implemented
	// yet if this is called in lab 1 or 2.
	bp.FlushAllPages()

	newString := ""
	for offset, _ := range newOffsetsLoaded {
		// fmt.Printf("offset is %v, str is %v\n", offset, string(offset))
		newString += fmt.Sprintf("%v,", offset)
	}
	if newString != "" && newString != "," {
		n, err := f.metadataFile.WriteString(newString)
		if err != nil {
			return err
		}
		fmt.Printf("Wrote %v bytes to %v\n", n, f.metadataFileName)
	}
	return nil
}

// Load the contents of a heap file from a specified CSV file.  Parameters are as follows:
// - hasHeader:  whether or not the CSV file has a header
// - sep: the character to use to separate fields
// - skipLastField: if true, the final field is skipped (some TPC datasets include a trailing separator on each line)
// Returns an error if the field cannot be opened or if a line is malformed
// We provide the implementation of this method, but it won't work until
// [HeapFile.insertTuple] and some other utility functions are implemented
func (f *HeapFile) LoadFromCSV(file *os.File, hasHeader bool, sep string, skipLastField bool) error {
	f.bufPool.CanFlushWhenFull = true
	defer func() { f.bufPool.CanFlushWhenFull = false }()
	scanner := bufio.NewScanner(file)
	cnt := 0
	i := 0
	for scanner.Scan() {
		if i%100 == 0 {
			fmt.Printf("Reading row %v of %v\n", i, file.Name())
		}
		line := scanner.Text()
		fields := strings.Split(line, sep)
		if skipLastField {
			fields = fields[0 : len(fields)-1]
		}
		numFields := len(fields)
		cnt++
		desc := f.Descriptor()
		if desc == nil || desc.Fields == nil {
			return GoDBError{MalformedDataError, "Descriptor was nil"}
		}
		if numFields != len(desc.Fields) {
			return GoDBError{MalformedDataError, fmt.Sprintf("LoadFromCSV:  line %d (%s) does not have expected number of fields (expected %d, got %d)", cnt, line, len(f.Descriptor().Fields), numFields)}
		}
		if cnt == 1 && hasHeader {
			continue
		}
		var newFields []DBValue
		for fno, field := range fields {
			switch f.Descriptor().Fields[fno].Ftype {
			case IntType:
				field = strings.TrimSpace(field)
				floatVal, err := strconv.ParseFloat(field, 64)
				if err != nil {
					return GoDBError{TypeMismatchError, fmt.Sprintf("LoadFromCSV: couldn't convert value %s to int, tuple %d", field, cnt)}
				}
				intValue := int(floatVal)
				newFields = append(newFields, IntField{int64(intValue)})
			case FloatType:
				field = strings.TrimSpace(field)
				floatVal, err := strconv.ParseFloat(field, 64)
				if err != nil {
					return GoDBError{TypeMismatchError, fmt.Sprintf("LoadFromCSV: couldn't convert value %s to int, tuple %d", field, cnt)}
				}
				floatValue := float64(floatVal)
				newFields = append(newFields, FloatField{floatValue})
			case StringType:
				if len(field) > StringLength {
					field = field[0:StringLength]
				}
				newFields = append(newFields, StringField{field})
			}
		}
		newT := Tuple{*f.Descriptor(), newFields, nil}
		tid := NewTID()
		err := f.insertTuple(&newT, tid)
		if err != nil {
			return err
		}
		i += 1
	}
	bp := f.bufPool
	// Force dirty pages to disk. CommitTransaction may not be implemented
	// yet if this is called in lab 1 or 2.
	bp.FlushAllPages()
	return nil
}

// Read the specified page number from the HeapFile on disk. This method is
// called by the [BufferPool.GetPage] method when it cannot find the page in its
// cache.
//
// This method will need to open the file supplied to the constructor, seek to
// the appropriate offset, read the bytes in, and construct a [heapPage] object,
// using the [heapPage.initFromBuffer] method.
func (f *HeapFile) readPage(pageNo int) (Page, error) {
	// TODO: some code goes here

	DebugHeapFile("heapFile.ReadPage reading page %v\n", pageNo)
	offset := int64(pageNo * PageSize)
	pageData := make([]byte, PageSize)

	_, err := f.file.ReadAt(pageData, offset)
	if err != nil {
		DebugHeapFile("1 got err %v\n", err)
		return nil, err
	}

	heapPage, err := newHeapPage(f.desc, pageNo, f)
	if err != nil {
		DebugHeapFile("2 got err %v\n", err)

		return nil, err
	}

	err = heapPage.initFromBuffer(bytes.NewBuffer(pageData))

	return heapPage, err
}

// Add the tuple to the HeapFile. This method should search through pages in the
// heap file, looking for empty slots and adding the tuple in the first empty
// slot if finds.
//
// If none are found, it should create a new [heapPage] and insert the tuple
// there, and write the heapPage to the end of the HeapFile (e.g., using the
// [flushPage] method.)
//
// To iterate through pages, it should use the [BufferPool.GetPage method]
// rather than directly reading pages itself. For lab 1, you do not need to
// worry about concurrent transactions modifying the Page or HeapFile. We will
// add support for concurrent modifications in lab 3.
//
// The page the tuple is inserted into should be marked as dirty.
func (f *HeapFile) insertTuple(t *Tuple, tid TransactionID) error {
	// TODO: some code goes here
	DebugHeapFile("here5\n")
	f.numInserted += 1

	// look through each page sequentially for an empty slot
	// TODO potential optimization to keep track of the lowest page No that has a free space
	DebugHeapFile("Starting call to insert tuple last page is %v capcity is %v numPages  file is %v\n", f.bufPool.capacity, f.numPages, f.file.Name())
	for pageNo := range f.pagesWithFreeSpace {
		DebugHeapFile("heapFile.insertTuple getting page\n")
		page, err := f.bufPool.GetPage(f, pageNo, tid, ReadPerm)
		if err != nil {
			DebugHeapFile("here9 %v %v\n", pageNo, f.numPages)

			return err
		}
		heapPage, ok := page.(*heapPage)
		if !ok {
			return GoDBError{TypeMismatchError, fmt.Sprintf("Couldn't convert page to heap page pointer. %v\n", page)}
		}

		// try to insert tuple
		_, err = heapPage.insertTuple(t)
		if err == nil {
			DebugHeapFile("returning lastWritten page file is %v\n", f.file.Name())
			return nil
		}

		// only tolerate error if it's PageFullError, else bubble up
		goDbError, ok := err.(GoDBError)
		if !ok {
			return err
		}
		if goDbError.code != PageFullError {
			DebugHeapFile("returning lastWritten page file is %v\n", f.file.Name())
			return err
		}
		DebugHeapFile("insert num %v, deleting page no %v\n", f.numInserted, pageNo)
		// page no longer has free space
		delete(f.pagesWithFreeSpace, pageNo)
	}

	DebugHeapFile("gonna add another heap page page no %v. tuple size is %v bp capcity is %v\n", f.numPages, f.tupleSize, f.bufPool.capacity)
	// make new heap page
	heapPage, err := newHeapPage(f.desc, f.numPages, f)
	if err != nil {
		DebugHeapFile("here8\n")
		return err
	}

	// insert tuple in new page, then write to disk
	_, err = heapPage.insertTuple(t)
	if err != nil {
		DebugHeapFile("here7\n")
		return err
	}
	DebugHeapFile("flushing heap page with no %v\n", heapPage.PageNo)
	// TODO don't flush it here
	DebugHeapFile("inset num %v, adding page cause all others full. %v\n", f.numInserted, len(f.pagesWithFreeSpace))
	_, err = f.bufPool.AddPage(heapPage, f, f.numPages, tid, ReadPerm)
	if err != nil {
		DebugHeapFile("uhh err is %v\n", err)
		return err
	}
	DebugHeapFile("gonna incremement num pages %v\n", f.numPages)

	f.pagesWithFreeSpace[f.numPages] = true
	f.numPages++
	DebugHeapFile("num pages is now %v\n", f.numPages)
	return nil
}

// Remove the provided tuple from the HeapFile.
//
// This method should use the [Tuple.Rid] field of t to determine which tuple to
// remove. The Rid field should be set when the tuple is read using the
// [Iterator] method, or is otherwise created (as in tests). Note that Rid is an
// empty interface, so you can supply any object you wish. You will likely want
// to identify the heap page and slot within the page that the tuple came from.
//
// The page the tuple is deleted from should be marked as dirty.
func (f *HeapFile) deleteTuple(t *Tuple, tid TransactionID) error {
	// TODO: some code goes here
	ridPtr, ok := t.Rid.(*recordIDImpl)
	if !ok {
		return GoDBError{IncompatibleTypesError, fmt.Sprintf("In Heap file Couldn't convert rid %v into pointer to my record id impl", t.Rid)}
	}

	page, err := f.bufPool.GetPage(f, ridPtr.pageNo, tid, ReadPerm)
	if err != nil {
		return err
	}

	heapPage, ok := page.(*heapPage)
	if !ok {
		return GoDBError{TypeMismatchError, fmt.Sprintf("Couldn't convert page to heap page pointer. %v\n", page)}
	}

	f.pagesWithFreeSpace[ridPtr.pageNo] = true

	return heapPage.deleteTuple(t.Rid)
}

// Method to force the specified page back to the backing file at the
// appropriate location. This will be called by BufferPool when it wants to
// evict a page. The Page object should store information about its offset on
// disk (e.g., that it is the ith page in the heap file), so you can determine
// where to write it back.
func (f *HeapFile) flushPage(p Page) error {
	// TODO: some code goes here
	heapPage, ok := p.(*heapPage)
	if !ok {
		return GoDBError{TypeMismatchError, fmt.Sprintf("Couldn't convert page to heap page pointer. %v\n", p)}
	}

	buf, err := heapPage.toBuffer()
	if err != nil {
		return err
	}

	offset := int64(heapPage.PageNo * PageSize)

	_, err = f.file.WriteAt(buf.Bytes(), offset)
	return err
}

// [Operator] descriptor method -- return the TupleDesc for this HeapFile
// Supplied as argument to NewHeapFile.
func (f *HeapFile) Descriptor() *TupleDesc {
	// TODO: some code goes here
	return f.desc
}

// [Operator] iterator method
// Return a function that iterates through the records in the heap file
// Note that this method should read pages from the HeapFile using the
// BufferPool method GetPage, rather than reading pages directly,
// since the BufferPool caches pages and manages page-level locking state for
// transactions
// You should ensure that Tuples returned by this method have their Rid object
// set appropriate so that [deleteTuple] will work (see additional comments there).
// Make sure to set the returned tuple's TupleDescriptor to the TupleDescriptor of
// the HeapFile. This allows it to correctly capture the table qualifier.
func (f *HeapFile) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	// TODO: some code goes here
	// closure!
	curPage := 0

	// get the next heap page iter
	getNextIter := func(pageNo int) (func() (*Tuple, error), error) {
		page, err := f.bufPool.GetPage(f, pageNo, tid, ReadPerm)
		DebugHeapFile("here1\n")
		if err != nil {
			DebugHeapFile("here2 %v %v\n", pageNo, f.numPages)

			return nil, err
		}
		heapPage, ok := page.(*heapPage)
		if !ok {
			return nil, GoDBError{TypeMismatchError, fmt.Sprintf("Couldn't convert page to heap page pointer. %v\n", page)}
		}

		return heapPage.tupleIter(), nil
	}

	// initialize first iter func
	curIter, err := getNextIter(curPage)
	if err != nil && err != io.EOF {
		DebugHeapFile("here3 %v\n", err)

		return nil, err
	}

	getTuple := func() (*Tuple, error) {

		for {

			// nothing more to return, EOF
			if curIter == nil {
				return nil, nil
			}

			// get next tuple from heapPage
			tuple, err := curIter()
			if tuple != nil || err != nil {
				tuple.Desc = *f.desc
				return tuple, nil
			}

			// reached EOF
			if curPage+1 == f.numPages {
				curIter = nil
				return nil, nil
			}

			// grab next heapPage iterfunc
			curPage++
			curIter, err = getNextIter(curPage)
			if err != nil {
				DebugHeapFile("here4\n")

				return nil, err
			}

		}
	}

	return getTuple, nil
}

// internal structure to use as key for a heap page
type heapHash struct {
	FileName string
	PageNo   int
}

// This method returns a key for a page to use in a map object, used by
// BufferPool to determine if a page is cached or not.  We recommend using a
// heapHash struct as the key for a page, although you can use any struct that
// does not contain a slice or a map that uniquely identifies the page.
func (f *HeapFile) pageKey(pgNo int) any {
	// TODO: some code goes here
	return heapHash{
		FileName: f.fileName,
		PageNo:   pgNo,
	}
}
