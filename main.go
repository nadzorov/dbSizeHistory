package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
)

type tableSpace struct {
	dbName      string
	tsName      string
	gbFreeOfMax string
	date        string
}

func parseCsvFile(file *os.File) []tableSpace {
	ts := []tableSpace{}
	csvReader := csv.NewReader(file)
	for {
		record, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		// debug
		// fmt.Printf("%#v \n", record)
		// for i, col := range record {
		// 	fmt.Printf("%#v %#v \n", i, strings.TrimSpace(col))
		// }
		ts = append(ts, tableSpace{
			dbName:      strings.TrimSpace(record[1]),
			tsName:      strings.TrimSpace(record[2]),
			gbFreeOfMax: strings.TrimSpace(record[9]),
			date:        strings.TrimSpace(record[0]),
		})
	}
	return ts
}

func main() {
	// Open file
	file, err := os.Open("./data/cftwork.log")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	allTs := parseCsvFile(file)
	for _, ts := range allTs {
		fmt.Printf("%#v \n", ts)
	}

}
