package extsort

import "bufio"
import "sync"
import "sort"
import "encoding"
import "fmt"
import "io/ioutil"

const (
	DefaultItemsPerExtFile = 3
)

// Any type that satisfies Interface can be sorted externally (on disk)
// using this package. This interface simply indicates that
// the types can be compared to each other, and serialized/deserialized to binary.
type Interface interface {
	LessThan(i Interface) bool
	encoding.BinaryMarshaler
	encoding.BinaryUnmarshaler
}

type ExternalSorter struct {
	input    <-chan Interface
	poolSize int
}

type inMemSorter []Interface

func (ims inMemSorter) Len() int {
	return len(ims)
}

func (ims inMemSorter) Less(i, j int) bool {
	return ims[i].LessThan(ims[j])
}

func (ims inMemSorter) Swap(i, j int) {
	ims[i], ims[j] = ims[j], ims[i]
}

type extSorterResult struct {
	file      string
	exhausted bool
	error
}

func (es ExternalSorter) Sort(out <-chan Interface) error {
	results := make(chan extSorterResult)
	workerWG := sync.WaitGroup{}
	// Start up a bunch of workers

	poolSize := 1
	for i := 0; i < poolSize; i++ {
		fmt.Println("starting worker", i)
		sorter := subsetSorter{es.input, 0}
		workerWG.Add(1)
		go runSortWorker(sorter, results)
	}

	// set up channel to be notified when all workers are done (i.e. the input channel is empty)
	allWorkersDone := make(chan struct{})
	go func() {
		fmt.Println("waiting on the WG")
		workerWG.Wait()
		close(results)
		allWorkersDone <- struct{}{}
	}()

	collectedResults := []extSorterResult{}
	resultsDone := make(chan struct{})
	// Collect results as they are delivered from the workers.
	go func() {
		fmt.Println("starting loop over results!")
		inputFinished := false
		for {
			select {
			case r := <-results:
				fmt.Println("got a result!")
				collectedResults = append(collectedResults, r)
				if r.exhausted {
					inputFinished = false
					workerWG.Done()
				} else {
					if !inputFinished {
						// there could be more input data, so start a new worker in the pool
						sorter := subsetSorter{es.input, 0}
						go runSortWorker(sorter, results)
					}
				}
			case <-allWorkersDone:
				resultsDone <- struct{}{}
				return
			}
		}
	}()

	<-resultsDone
	fmt.Printf("results are %#v\n", collectedResults)
	return nil

}

func runSortWorker(sorter subsetSorter, out chan extSorterResult) {
	fmt.Println("running worker!")
	outfile, exhausted, err := sorter.do()
	fmt.Println("worker is done!")
	out <- extSorterResult{outfile, exhausted, err}
	fmt.Println("done in worker helper")
}

type subsetSorter struct {
	in       <-chan Interface
	maxItems int
}

func (sss subsetSorter) do() (outfile string, exhausted bool, err error) {
	if sss.maxItems <= 0 {
		sss.maxItems = DefaultItemsPerExtFile
	}
	container := inMemSorter(make([]Interface, 0, DefaultItemsPerExtFile))
	numItems := 0
	exhausted = true
	// read up to maxItems from the channel
	for x := range sss.in {
		numItems++
		fmt.Println("got an input")
		container = append(container, x)
		if numItems >= sss.maxItems {
			exhausted = false
			break
		}
	}
	fmt.Println("sorting!")

	// sort them
	sort.Sort(container)

	// write them to disk
	f, err := ioutil.TempFile("", "extsort_")
	if err != nil {
		return "", exhausted, err
	}
	w := bufio.NewWriter(f)
	fmt.Println("writing file to ", f.Name())

	defer func() {
		// Always try to close the file when returning,
		// but when already returning a non-nil error, always
		// return that first instead of any error from .Close().
		cerr := f.Close()
		if err == nil {
			err = cerr
		}
	}()

	for _, i := range container {
		b, err := i.MarshalBinary()
		if err != nil {
			return "", exhausted, err
		}
		_, err = w.Write(encodeUInt32(uint32(len(b))))
		if err != nil {
			return f.Name(), exhausted, err
		}
		_, err = w.Write(b)
		if err != nil {
			return f.Name(), exhausted, err
		}
	}
	err = w.Flush()
	if err != nil {
		return f.Name(), exhausted, err
	}
	return f.Name(), exhausted, nil

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
