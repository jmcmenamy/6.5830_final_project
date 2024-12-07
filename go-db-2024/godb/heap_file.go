package godb

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"strconv"
	"strings"
)

var DEBUGHEAPFILE = false

var MEAN = "mean"
var STDDEV = "standardDeviation"

// from https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Welford's_online_algorithm
var SUMSQUARESDIFF = "SumSquaresDiff"
var N = "n"
var OFFSET = "offset"
var ESTIMATEDLINES = "estimatedLines"

// sentinel value to tell when the entire .tbl file has been loaded
var COMPLETE = "complete"

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
	statsFileName      string
	loadedEntireFile   bool
	metadataFile       *os.File
	statsFile          *os.File
	tupleSize          int
	numPages           int
	file               *os.File
	pagesWithFreeSpace map[int]bool
	numInserted        int
	offSetsLoaded      map[int64]bool
	statNames          []string
	statistics         map[string]map[string]float64
	freezeStats        bool
}

func (f *HeapFile) writeToStatsFile() error {
	// fmt.Printf("Writing here %v %v\n", f.statsFile, len(f.statistics))
	if f.statsFile == nil {
		return nil
	}
	// overwrite entire file to replace stats
	f.statsFile.Seek(0, io.SeekStart)
	statsFileContent := fmt.Sprintf("FieldName,%v,%v,%v,%v\n", MEAN, STDDEV, SUMSQUARESDIFF, N)
	for field, stats := range f.statistics {
		statsFileContent += fmt.Sprintf("%s,%.2f,%.2f,%.2f,%.2f\n",
			field, stats[MEAN], stats[STDDEV], stats[SUMSQUARESDIFF], stats[N])
	}
	_, err := f.statsFile.WriteString(statsFileContent)
	// fmt.Printf("Wrote here %v %v\n", f.statsFile, len(f.statistics))
	if err != nil {
		return err
	}
	return nil
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
	statsFileName := ""
	if len(extraArgs) > 0 {
		// fmt.Println("Extra args greater than 0\n")
		metadataFileName = extraArgs[0]
	}
	if len(extraArgs) > 1 {
		statsFileName = extraArgs[1]
	}
	heapFile := &HeapFile{bufPool: bp, desc: td, fileName: fromFile, pagesWithFreeSpace: make(map[int]bool), metadataFileName: metadataFileName, statsFileName: statsFileName}
	heapFile.statistics = make(map[string]map[string]float64)

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

	if statsFileName != "" {

		// fmt.Printf("got here\n")
		statsFile, err := os.OpenFile(statsFileName, os.O_RDWR, 0644)
		if err != nil {
			// fmt.Printf("got err rrhere %v\n", err)
			if !os.IsNotExist(err) {
				// fmt.Printf("uhh bro %v\n", err)
				return nil, err
			}
			// fmt.Printf("got err here %v\n", err)
			statsFile, err = os.OpenFile(statsFileName, os.O_RDWR|os.O_CREATE, 0644)
			if err != nil {
				// fmt.Printf("uhh bro %v %v\n", err, statsFile)
				return nil, err
			}
			heapFile.statsFile = statsFile
			// fmt.Printf("setting here %v\n", heapFile.statsFile)
			err = heapFile.writeToStatsFile()
			if err != nil {
				// fmt.Printf("got err here %v\n", err)
				return nil, err
			}
		} else {
			// fmt.Printf("nil err/????? %v %v\n", statsFile, err)
		}
		heapFile.statsFile = statsFile
		err = heapFile.ProcessStatsFile(statsFile)
		if err != nil {
			// fmt.Printf("uhh brooo %v %v\n", err, statsFile)
			return nil, err
		}
		// fmt.Printf("look here %v\n", statsFile)
		tblFile, err := os.OpenFile(strings.Replace(fromFile, ".dat", ".tbl", 1), os.O_RDWR, 0644)
		if err != nil {
			return nil, err
		}
		fileInfo, err := tblFile.Stat()
		if err != nil {
			return nil, err
		}
		estimatedLinesInFile := int(fileInfo.Size()) / heapFile.tupleSize
		estLinesStats, ok := heapFile.statistics[ESTIMATEDLINES]
		if !ok {
			heapFile.statistics[ESTIMATEDLINES] = make(map[string]float64)
			estLinesStats = heapFile.statistics[ESTIMATEDLINES]
		}
		// fmt.Printf("Writing estimates lines as %v for %v file size is %v tuple size is %v\n", estimatedLinesInFile, statsFileName, fileInfo.Name(), heapFile.tupleSize)
		estLinesStats[MEAN] = float64(estimatedLinesInFile)
	}

	// fmt.Printf("here stats file is %v %v\n", heapFile.statsFile, statsFileName)
	return heapFile, nil //replace me
}

func (f *HeapFile) Statistics() map[string]map[string]float64 {
	return f.statistics
}

func (f *HeapFile) ProcessStatsFile(file *os.File) error {
	// fmt.Printf("entering %v\n", file)
	scanner := bufio.NewScanner(file)
	scanner.Scan()
	firstLine := scanner.Text()
	statNames := strings.Split(firstLine, ",")
	f.statNames = statNames
	for scanner.Scan() {
		line := scanner.Text()
		vals := strings.Split(line, ",")
		fieldName := vals[0]
		for i, statVal := range vals {
			if i == 0 {
				continue
			}
			fieldStats, ok := f.statistics[fieldName]
			if !ok {
				f.statistics[fieldName] = make(map[string]float64)
				fieldStats = f.statistics[fieldName]
			}
			floatVal, err := strconv.ParseFloat(statVal, 64)
			if err != nil {
				return err
			}
			fieldStats[statNames[i]] = floatVal
		}
	}
	// fmt.Printf("stats are now %v %v\n", f.statistics, file)
	return nil
}

func (f *HeapFile) ProcessMetadataFile(file *os.File) error {
	// Define a buffer size for reading chunks (e.g., 4096 bytes = 4 KB)
	chunkSize := 4096
	buffer := make([]byte, chunkSize)
	// start := time.Now()

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
	// duration := time.Since(start)
	// fmt.Printf("Parsed metadatafile in %v seconds, len(f.offsetsLoaded) = %v\n", duration, len(f.offSetsLoaded))
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
// If fieldStats is nil, do not select for non-outlier rows during sampling.
// Otherwise, return error if line contains an outlier numerical field based on
// mean, standard deviation.
func (f *HeapFile) loadLine(line string, sep string, fieldStats map[string]map[string]float64) error {
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
	nStats, ok := f.statistics[N]
	if !ok {
		f.statistics[N] = make(map[string]float64)
		nStats = f.statistics[N]
	}
	for fno, field := range fields {
		var floatVal float64
		var err error
		switch f.Descriptor().Fields[fno].Ftype {
		case IntType:
			field = strings.TrimSpace(field)
			floatVal, err = strconv.ParseFloat(field, 64)
			if err != nil {
				return GoDBError{TypeMismatchError, fmt.Sprintf("LoadFromCSV: couldn't convert value %s to int", field)}
			}
			intValue := int(floatVal)
			newFields = append(newFields, IntField{int64(intValue)})
			if fieldStats != nil {
				fieldName := desc.Fields[fno].Fname
				stats := fieldStats[fieldName]
				mean, ok := stats[MEAN]
				stddev, ok2 := stats[STDDEV]
				if ok && ok2 && (floatVal > mean+(2*stddev) || floatVal < mean-(2*stddev)) {
					return fmt.Errorf("outlier value %v for field %v (%v, %v). not inserted into database", floatVal, fieldName, mean, stddev)
				}
			}
		case FloatType:
			field = strings.TrimSpace(field)
			floatVal, err = strconv.ParseFloat(field, 64)
			if err != nil {
				return GoDBError{TypeMismatchError, fmt.Sprintf("LoadFromCSV: couldn't convert value %s to int", field)}
			}
			floatValue := float64(floatVal)
			newFields = append(newFields, FloatField{floatValue})
			if fieldStats != nil {
				fieldName := desc.Fields[fno].Fname
				stats := fieldStats[fieldName]
				mean, ok := stats[MEAN]
				stddev, ok2 := stats[STDDEV]
				if ok && ok2 && (floatVal > mean+(2*stddev) || floatVal < mean-(2*stddev)) {
					return fmt.Errorf("outlier value %v for field %v (%v, %v). not inserted into database", floatVal, fieldName, mean, stddev)
				}
			}
		case StringType:
			if len(field) > StringLength {
				field = field[0:StringLength]
			}
			newFields = append(newFields, StringField{field})
		}
		if f.statistics[COMPLETE][MEAN] != 1 {
			n := nStats[MEAN] + 1
			if f.Descriptor().Fields[fno].Ftype == FloatType || f.Descriptor().Fields[fno].Ftype == IntType {
				// update our running statistics
				// using https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Welford's_online_algorithm
				fieldName := desc.Fields[fno].Fname
				fieldStats, ok := f.statistics[fieldName]
				if !ok {
					f.statistics[fieldName] = make(map[string]float64)
					// sentinel value to let us know if we need to compute this later on for optimized queries
					f.statistics[fieldName][STDDEV] = -1
					fieldStats = f.statistics[fieldName]
				}
				newMean := fieldStats[MEAN] + (floatVal-fieldStats[MEAN])/n
				fieldStats[SUMSQUARESDIFF] += (floatVal - fieldStats[MEAN]) * (floatVal - newMean)
				fieldStats[MEAN] = newMean
			}
		}
	}
	nStats[MEAN] += 1

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
// - fieldStats: if nil, do not select for non-outlier rows during sampling.
// otherwise, strictly load into the database rows with numerical fields all
// within two standard deviations of the mean for the corresponding column.
// Returns an error if the field cannot be opened or if a line is malformed
// We provide the implementation of this method, but it won't work until
// [HeapFile.insertTuple] and some other utility functions are implemented
func (f *HeapFile) LoadSomeFromCSV(file *os.File, hasHeader bool, sep string, skipLastField bool, fieldStats map[string]map[string]float64) error {
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
		// fmt.Printf("Entering for loops len(f.offsetsLoaded) = %v and estiamtedLinesinFIle is %v\n", len(f.offSetsLoaded), estimatedLinesInFile)
		for numSampledLines < int(sampleRate*float64(estimatedLinesInFile)) && len(f.offSetsLoaded) <= estimatedLinesInFile-100 {
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
			if f.metadataFile != nil {
				f.offSetsLoaded[newOffset] = true
				newOffsetsLoaded[newOffset] = true
			}
			// numSampledLines++

			line, err := reader.ReadString('\n')
			if err != nil {
				return fmt.Errorf("error reading line at byte offset %v", newOffset)
			}

			err = f.loadLine(line, sep, fieldStats)
			if err == nil {
				numSampledLines++
			}

			reader.Reset(file)
		}
		if numSampledLines == 0 {
			f.loadedEntireFile = true
		}
	} else {
		file.Seek(0, 0)
		scanner := bufio.NewScanner(file)
		// Get the current file offset (position)
		offset, err := file.Seek(0, io.SeekCurrent)
		if err != nil {
			// fmt.Println("Error getting file offset:", err)
			return err
		}
		for scanner.Scan() {
			line := scanner.Text()
			if f.offSetsLoaded[offset] {
				// Get the current file offset (position)
				offset, err = file.Seek(0, io.SeekCurrent)
				if err != nil {
					// fmt.Println("Error getting file offset:", err)
					return err
				}
				continue
			}
			if f.metadataFile != nil {
				f.offSetsLoaded[offset] = true
				newOffsetsLoaded[offset] = true
			}
			f.loadLine(line, sep, nil)
			// Get the current file offset (position)
			offset, err = file.Seek(0, io.SeekCurrent)
			if err != nil {
				// fmt.Println("Error getting file offset:", err)
				return err
			}
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
		_, err := f.metadataFile.WriteString(newString)
		if err != nil {
			return err
		}
		// fmt.Printf("Wrote %v bytes to %v\n", n, f.metadataFileName)
	}
	err = f.writeToStatsFile()
	if err != nil {
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
func (f *HeapFile) LoadSomeFromCSVContiguous(file *os.File, hasHeader bool, sep string, skipLastField bool) error {
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
	contiguousOffset := f.statistics[OFFSET][MEAN]
	file.Seek(int64(contiguousOffset), io.SeekStart)
	// fmt.Printf("gonna start reading from %v. file size is %v tuples size is %v\n", contiguousOffset, fileInfo.Name(), f.tupleSize)
	scanner := bufio.NewScanner(file)
	if estimatedLinesInFile >= samplingThreshold {
		// fmt.Printf("estimated is %v (%v/%v) sampling is %v\n", estimatedLinesInFile, fileInfo.Size(), f.tupleSize, samplingThreshold)
		numSampledLines := 0
		newOffset, err := file.Seek(0, io.SeekCurrent)
		if err != nil {
			return err
		}
		for scanner.Scan() && numSampledLines < int(sampleRate*float64(estimatedLinesInFile)) && len(f.offSetsLoaded) <= estimatedLinesInFile-100 {
			// fmt.Printf("got in here\n")
			line := scanner.Text()
			if f.offSetsLoaded[newOffset] {
				newOffset, err = file.Seek(0, io.SeekCurrent)
				if err != nil {
					return err
				}
				continue
			}
			if f.metadataFile != nil {
				f.offSetsLoaded[newOffset] = true
				newOffsetsLoaded[newOffset] = true
			}

			numSampledLines += 1

			f.loadLine(line, sep, nil)
			newOffset, err = file.Seek(0, io.SeekCurrent)
			if err != nil {
				return err
			}
		}

		contiguousOffset = float64(newOffset)
		// fmt.Printf("loaded %v new lines \n", numSampledLines)
		if numSampledLines == 0 {
			f.loadedEntireFile = true
		}

	} else {
		file.Seek(0, 0)
		scanner := bufio.NewScanner(file)
		offset, err := file.Seek(0, io.SeekCurrent)
		if err != nil {
			// fmt.Println("Error getting file offset:", err)
			return err
		}
		for scanner.Scan() {
			line := scanner.Text()
			if f.offSetsLoaded[offset] {
				offset, err = file.Seek(0, io.SeekCurrent)
				if err != nil {
					// fmt.Println("Error getting file offset:", err)
					return err
				}
				continue
			}
			if f.metadataFile != nil {
				f.offSetsLoaded[offset] = true
				newOffsetsLoaded[offset] = true
			}
			f.loadLine(line, sep, nil)
			// Get the current file offset (position)
			offset, err = file.Seek(0, io.SeekCurrent)
			if err != nil {
				// fmt.Println("Error getting file offset:", err)
				return err
			}
		}
		// fmt.Printf("read entire file?\n")
		f.loadedEntireFile = true
		contiguousOffset = float64(offset)
	}
	bp := f.bufPool
	// Force dirty pages to disk. CommitTransaction may not be implemented
	// yet if this is called in lab 1 or 2.
	bp.FlushAllPages()

	offsetStats, ok := f.statistics[OFFSET]
	if !ok {
		f.statistics[OFFSET] = make(map[string]float64)
		offsetStats = f.statistics[OFFSET]
	}
	offsetStats[MEAN] = contiguousOffset
	// fmt.Printf("offset is now %v\n", contiguousOffset)

	newString := ""
	for offset, _ := range newOffsetsLoaded {
		// fmt.Printf("offset is %v, str is %v\n", offset, string(offset))
		newString += fmt.Sprintf("%v,", offset)
	}
	if newString != "" && newString != "," {
		_, err := f.metadataFile.WriteString(newString)
		if err != nil {
			return err
		}
		// fmt.Printf("Wrote %v bytes to %v\n", n, f.metadataFileName)
	}

	err = f.writeToStatsFile()
	if err != nil {
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
func (f *HeapFile) LoadSomeFromCSVContiguousStratified(file *os.File, hasHeader bool, sep string, skipLastField bool) error {
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
	randomOffset := rand.Int63n(fileInfo.Size() - 1)
	file.Seek(randomOffset, io.SeekStart)
	scanner := bufio.NewScanner(file)
	scanner.Scan()
	if estimatedLinesInFile >= samplingThreshold {
		// fmt.Printf("HEYO! estimated is %v (%v/%v) sampling is %v\n", estimatedLinesInFile, fileInfo.Size(), f.tupleSize, samplingThreshold)
		numSampledLines := 0
		newOffset, err := file.Seek(0, io.SeekCurrent)
		if err != nil {
			return err
		}
		shouldContinue := true
		retries := 1
		for shouldContinue && numSampledLines < int(sampleRate*float64(estimatedLinesInFile)) && len(f.offSetsLoaded) <= estimatedLinesInFile-100 {
			shouldContinue = scanner.Scan()
			if !shouldContinue {
				if retries == 0 {
					// fmt.Printf("breaking here?")
					break
				}
				retries--
				newOffset, err = file.Seek(0, io.SeekStart)
				if err != nil {
					return err
				}
				scanner = bufio.NewScanner(file)
				continue
			}
			// fmt.Printf("got in here\n")
			if f.metadataFile != nil && f.offSetsLoaded[newOffset] {
				newOffset, err = file.Seek(0, io.SeekCurrent)
				if err != nil {
					return err
				}
				continue
			}
			line := scanner.Text()
			if f.metadataFile != nil {
				f.offSetsLoaded[newOffset] = true
				newOffsetsLoaded[newOffset] = true
			}

			numSampledLines += 1

			f.loadLine(line, sep, nil)
			newOffset, err = file.Seek(0, io.SeekCurrent)
			if err != nil {
				return err
			}
		}

		// fmt.Printf("loaded %v new lines \n", numSampledLines)
		if numSampledLines == 0 {
			f.loadedEntireFile = true
		}

	} else {
		file.Seek(0, 0)
		scanner := bufio.NewScanner(file)
		offset, err := file.Seek(0, io.SeekCurrent)
		if err != nil {
			// fmt.Println("Error getting file offset:", err)
			return err
		}
		for scanner.Scan() {
			line := scanner.Text()
			if f.offSetsLoaded[offset] {
				offset, err = file.Seek(0, io.SeekCurrent)
				if err != nil {
					// fmt.Println("Error getting file offset:", err)
					return err
				}
				continue
			}
			if f.metadataFile != nil {
				f.offSetsLoaded[offset] = true
				newOffsetsLoaded[offset] = true
			}
			f.loadLine(line, sep, nil)
			// Get the current file offset (position)
			offset, err = file.Seek(0, io.SeekCurrent)
			if err != nil {
				// fmt.Println("Error getting file offset:", err)
				return err
			}
		}
		// fmt.Printf("read entire file?\n")
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
		_, err := f.metadataFile.WriteString(newString)
		if err != nil {
			return err
		}
		// fmt.Printf(" HEYO ! Wrote %v bytes to %v\n", n, f.metadataFileName)
	}

	err = f.writeToStatsFile()
	if err != nil {
		return err
	}

	return nil
}

func computeStats(fieldValues map[string][]float64) map[string]map[string]float64 {
	result := make(map[string]map[string]float64)

	for key, values := range fieldValues {
		numValues := float64(len(values))

		// Compute the mean
		sum := 0.0
		for _, v := range values {
			sum += v
		}
		mean := sum / numValues

		// Compute the standard deviation
		varianceSum := 0.0
		for _, v := range values {
			varianceSum += (v - mean) * (v - mean)
		}
		stddev := math.Sqrt(varianceSum / numValues)

		result[key] = map[string]float64{MEAN: mean, STDDEV: stddev}
	}
	return result
}

// Read CSV file and write to file statFilename per-column mean, stddev.
func (f *HeapFile) StatFromCSV(file *os.File, hasHeader bool, sep string, skipLastField bool, statFilename string) error {
	f.bufPool.CanFlushWhenFull = true
	defer func() { f.bufPool.CanFlushWhenFull = false }()
	scanner := bufio.NewScanner(file)
	cnt := 0

	fieldValues := map[string][]float64{}

	for scanner.Scan() {
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
		for fno, field := range fields {
			switch f.Descriptor().Fields[fno].Ftype {
			case IntType:
				field = strings.TrimSpace(field)
				floatVal, err := strconv.ParseFloat(field, 64)
				if err != nil {
					return GoDBError{TypeMismatchError, fmt.Sprintf("LoadFromCSV: couldn't convert value %s to int, tuple %d", field, cnt)}
				}
				fieldName := desc.Fields[fno].Fname
				fieldValues[fieldName] = append(fieldValues[fieldName], floatVal)
			case FloatType:
				field = strings.TrimSpace(field)
				floatVal, err := strconv.ParseFloat(field, 64)
				if err != nil {
					return GoDBError{TypeMismatchError, fmt.Sprintf("LoadFromCSV: couldn't convert value %s to int, tuple %d", field, cnt)}
				}
				fieldName := desc.Fields[fno].Fname
				fieldValues[fieldName] = append(fieldValues[fieldName], floatVal)
			}
		}
	}

	fieldStats := computeStats(fieldValues)

	file, err := os.OpenFile(statFilename, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to open or create file: %w", err)
	}
	defer file.Close()

	_, err = file.WriteString(fmt.Sprintf("FieldName,%v,%v\n", MEAN, STDDEV))
	if err != nil {
		return err
	}

	// mark as complete so it's not updated
	fieldStats[COMPLETE] = map[string]float64{MEAN: 1}
	fieldStats[ESTIMATEDLINES] = map[string]float64{MEAN: float64(cnt - 1)}
	// fmt.Printf("Done scanning count is %v for %v\n", cnt-1, statFilename)

	for field, stats := range fieldStats {
		line := fmt.Sprintf("%s,%.2f,%.2f\n",
			field, stats[MEAN], stats[STDDEV])
		_, err := file.WriteString(line)
		if err != nil {
			return fmt.Errorf("failed to write to file: %w", err)
		}
	}
	f.statistics = fieldStats
	return nil
}

// Return a map mapping field names to [mean, stddev] from a CSV file containing
// comma-delimited stats.
func LoadStat(statFilename string) (map[string]map[string]float64, error) {
	file, err := os.Open(statFilename)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %v", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	// skip the first line
	// reader.Read()
	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("failed to read CSV: %v", err)
	}

	var statNames []string
	fieldStats := make(map[string]map[string]float64)
	for i, record := range records {
		if i == 0 {
			statNames = make([]string, len(record))
			copy(statNames, record)
			continue
		}
		// if len(record) != 3 {
		// 	return nil, fmt.Errorf("invalid record format: %v", record)
		// }
		fieldStats[record[0]] = make(map[string]float64)
		for j, strVal := range record {
			if j == 0 {
				continue
			}
			val, err := strconv.ParseFloat(strVal, 64)
			if err != nil {
				return nil, fmt.Errorf("failed to parse %v for key %v: %v", statNames[j], strVal, err)
			}
			fieldStats[record[0]][statNames[j]] = val
		}
	}

	return fieldStats, nil
}

// Multi-pass random sampling.
//
// In the first pass, compute and write to a file per-column mean, standard
// deviation without loading any data into the database. In the second pass, for
// each row in the CSV file, determine whether all numerical fields are within
// two standard deviations. Repeatedly randomly sample until a sufficient subset
// of (non-outlier) data is loaded into the database.
func (f *HeapFile) StatAndLoadFromCSV(file *os.File, hasHeader bool, sep string, skipLastField bool, statFilename string) error {
	// Get stats in initial read-only pass through CSV file
	err := f.StatFromCSV(file, hasHeader, sep, skipLastField, statFilename)
	if err != nil {
		return fmt.Errorf("failed to compute stats from file %v", file)
	}

	// Read stats from CSV file and load into map
	fieldStats, err := LoadStat(statFilename)
	if err != nil {
		return fmt.Errorf("failed to read stats from file %v. %v", statFilename, err)
	}

	// Randomly sample rows (with non-outlier values) to insert into database
	_ = f.LoadSomeFromCSV(file, hasHeader, sep, skipLastField, fieldStats)

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
			// fmt.Printf("Reading row %v of %v\n", i, file.Name())
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
	f.loadedEntireFile = true
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
