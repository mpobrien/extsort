package extsort

import "fmt"
import "math/rand"
import "testing"

type MyExtSorter string

func (mes *MyExtSorter) LessThan(i Interface) bool {
	other := i.(*MyExtSorter)
	return string(*mes) < string(*other)
}

func (mes *MyExtSorter) MarshalBinary() ([]byte, error) {
	return []byte(string(*mes)), nil
}

func (mes *MyExtSorter) UnmarshalBinary(data []byte) error {
	*mes = MyExtSorter(string(data))
	return nil
}

func TestExtSort(t *testing.T) {
	inputChan := make(chan Interface)
	go func() {
		for i := 0; i < 10; i++ {
			test := new(MyExtSorter)
			*test = MyExtSorter(fmt.Sprintf("item %v", rand.Float64()))
			inputChan <- test
		}
		close(inputChan)
		fmt.Println("done starting input")
	}()

	extSorter := ExternalSorter{inputChan, 3}
	out := make(chan Interface)
	err := extSorter.Sort(out)
	fmt.Println("got results: ", err)

}
