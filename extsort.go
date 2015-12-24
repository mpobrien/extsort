package extsort

import (
	"bufio"
	//"container/heap"
	"encoding"
	"io"
	"io/ioutil"
	"log"
	"os"
	"sort"
	"sync"
)

const (
	// Default number of sortable entries to store per
	// file on disk.
	DefaultItemsPerExtFile = 3
)

var logger *log.Logger

func init() {
	logger = log.New(os.Stdout, "extsort: ", log.Lshortfile|log.Ldate)
}

// Any type that satisfies Interface can be sorted externally (on disk)
// using this package. This interface simply indicates that
// the types can be compared to each other, and serialized/deserialized to binary.
type Interface interface {
	LessThan(i Interface) bool
	encoding.BinaryMarshaler
	encoding.BinaryUnmarshaler
}

// Struct to manage external sorting
type ExternalSorter struct {
	// Channel which will be used to receive the (unsorted) input data.
	input <-chan Interface

	// Number of goroutines to use for sorting.
	poolSize int
}

type sortableList []Interface

func (ims sortableList) Len() int {
	return len(ims)
}

func (ims sortableList) Less(i, j int) bool {
	return ims[i].LessThan(ims[j])
}

func (ims sortableList) Swap(i, j int) {
	ims[i], ims[j] = ims[j], ims[i]
}

func (ims sortableList) Marshal(out io.Writer) (int, error) {
	written := 0
	var n int
	for _, item := range ims {
		b, err := item.MarshalBinary()
		if err != nil {
			return written, err
		}
		n, err = out.Write(encodeUInt32(uint32(len(b))))
		written += n
		if err != nil {
			return written, err
		}
		n, err = out.Write(b)
		written += n
		if err != nil {
			return written, err
		}
	}
	return written, nil
}

type extSorterResult struct {
	file      string
	exhausted bool
	error
}

func newIterator(files []string) *Iterator {
	return &Iterator{files, nil, nil, 0, nil, nil}
}

type Iterator struct {
	files        []string
	streams      []subsetReader
	current      io.Reader
	currentIndex int
	err          error
	mergerHeap   sortableList
}

func (it *Iterator) init() error {
	//TODO panic if already init'd
	it.mergerHeap = sortableList(make([]Interface, len(it.files)))
	it.streams = make([]subsetReader, 0, len(it.files))
	for _, file := range it.files {
		newstream := subsetReader{file, nil}
		err := newstream.Open()
		if err != nil {
			return err
		}
		it.streams = append(it.streams, newstream)
	}
	return nil
}

/*
func (it *Iterator) Next(into Interface) bool {
	if current == nil {
		if currentIndex >= files.length {
			return false
		}
		current, err := os.Open(files[current])
		if err != nil {
			it.err = err
			return false
		}
	}
		logger.Println("got file", filename)
		sortedBlockReader := subsetReader{filename, nil}
		err := sortedBlockReader.Open()
		if err != nil {
			// TODO proper error handling here
			logger.Println("error from reader", err)
			return
		}
}
*/

func (es ExternalSorter) Sort() *Iterator {
	sortedChunks := make(chan sortableList)
	results := make(chan string)
	sorterPoolSize := 1
	writerPoolSize := 1

	// Goroutine pool for sorting groups of incoming data
	sorterWG := sync.WaitGroup{}
	for i := 0; i < sorterPoolSize; i++ {
		logger.Println("starting sort worker", i)
		sorter := subsetSorter{es.input, sortedChunks, 0}
		sorterWG.Add(1)
		go func() {
			sorter.do()
			sorterWG.Done()
		}()
	}

	// Goroutine pool for writing grouped blocks to disk
	writerWG := sync.WaitGroup{}
	for i := 0; i < writerPoolSize; i++ {
		logger.Println("starting write worker", i)
		writer := subsetWriter{sortedChunks, results}
		writerWG.Add(1)
		go func() {
			writer.Do()
			writerWG.Done()
		}()
	}

	go func() {
		sorterWG.Wait()
		close(sortedChunks)
	}()

	go func() {
		writerWG.Wait()
		close(results)
	}()

	collectedResults := []string{}
	// Goroutine for collecting location of serialized blocks
	for filename := range results {
		collectedResults = append(collectedResults, filename)
	}

	//TODO Refactor so that no workers start until the first call to iterator.Next()
	return Iterator{collectedResults}

	return nil
}

type subsetWriter struct {
	in  <-chan sortableList
	out chan<- string
}

type subsetReader struct {
	filename string
	r        io.ReadCloser
}

func (ssr subsetReader) Open() error {
	var err error
	ssr.r, err = os.Open(filename)
	if err != nil {
		return err
	}
}

func (ssr subsetReader) Close() error {
	return ssr.r.Close()
}

func (ssr subsetReader) Next(dest Interface) error {
	// TODO allocate this buffer once per reader, and re-use it to avoid excessive allocations
	into := make([]byte, 4)

	// read object size (a 4 byte integer)
	_, err := io.ReadAtLeast(ssr.r, into[0:4], 4)
	if err != nil {
		if err != io.EOF {
			return nil, err
		}
		// we hit EOF right away, so we're at the end of the stream.
		bs.err = nil
		return nil, nil
	}
	size := decodeUInt32(into)

	// TODO We can optimize this by allocating only one slice per reader, to the maximum size
	// that will be needed for all items in  the file. This could be calculated during the
	// sorting phase, then stored per segment in a header, and read back during Open().
	raw := make([]byte, 0, size)
	n, err = io.ReadAtLeast(ssr.r, raw, size)
	if err != nil {
		if err != io.EOF {
			return nil, err
		}
		// we hit EOF but didn't get a full item from the stream, so something is f'd up.
		return nil.fmt.Errorf("Corrupt stream: expected %v bytes but got only %v", size, n)
	}

	err = dest.UnmarshalBinary(raw)
	return err
}

func (ssw subsetWriter) Do() error {
	for sortedBlock := range ssw.in {
		logger.Println("got a block to write!")
		// write them to disk
		f, err := ioutil.TempFile("", "extsort_")
		if err != nil {
			return err
		}
		w := bufio.NewWriter(f)
		_, err = sortedBlock.Marshal(w)
		if err != nil {
			logger.Println("got err", err)
			f.Close()
			return err
		}
		w.Flush()
		f.Close()
		ssw.out <- f.Name()
	}
	logger.Println("writer exiting!")
	return nil
}

type subsetSorter struct {
	in       <-chan Interface
	out      chan sortableList
	maxItems int
}

func (sss subsetSorter) do() {
	if sss.maxItems <= 0 {
		sss.maxItems = DefaultItemsPerExtFile
	}
	container := sortableList(make([]Interface, 0, DefaultItemsPerExtFile))
	numItems := 0
	exhausted := true
	for {
		numFound := 0
		// read up to maxItems from the channel
		for x := range sss.in {
			numFound++
			numItems++
			logger.Println("got an input", numItems)
			container = append(container, x)
			if numItems >= sss.maxItems {
				logger.Println("got enough, breaking", numItems)
				exhausted = false
				break
			}
		}
		if numFound == 0 {
			return
		}
		logger.Println("sorting!")

		// sort them
		sort.Sort(container)

		sss.out <- container
		if exhausted {
			return
		}
	}
}

// TODO use protobuf ints instead.
func encodeUInt32(i uint32) []byte {
	return []byte{
		byte((i >> 24) & 0xFF),
		byte((i >> 16) & 0xFF),
		byte((i >> 8) & 0xFF),
		byte((i) & 0xFF),
	}
}

func decodeUInt32(b []byte) uint32 {
	return (uint32(b[0]) << 24) |
		(uint32(b[1]) << 16) |
		(uint32(b[2]) << 8) |
		(uint32(b[3]))

}
