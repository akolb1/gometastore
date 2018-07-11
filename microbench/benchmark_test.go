package microbench

import (
	"fmt"

	"github.com/akolb1/gometastore/microbench"
)

func ExampleMeasureSimple() {
	microbench.MeasureSimple(func() {
		fmt.Print("hello ")
	}, 1, 2)
	// Output: hello hello hello
}

func ExampleMeasure() {
	microbench.Measure(
		func() {
			fmt.Print(" pretest")
		},
		func() {
			fmt.Print(" test")
		},
		func() {
			fmt.Print(" cleanup")
		}, 1, 1)
	// Output: pretest test cleanup pretest test cleanup
}

func ExampleStats_Max() {
	stats := microbench.MakeStats()
	stats.Add(1.0)
	stats.Add(2.0)
	fmt.Println("max =", stats.Max())
	// Output max = 2.0
}

func ExampleStats_Min() {
	stats := microbench.MakeStats()
	stats.Add(1.0)
	stats.Add(2.0)
	fmt.Println("min =", stats.Min())
	// Output max = 1.0
}
