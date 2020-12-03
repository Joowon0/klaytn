package klaytn

import (
	"fmt"
	"sync"
	"testing"
)

var once sync.Once
var once2 sync.Once

func TestOnce(t *testing.T) {
	for i := 0; i < 10; i++ {
		once.Do(func() {
			fmt.Println("1")
		})
		once2.Do(func() {
			fmt.Println("2")
		})
	}
}
