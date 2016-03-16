package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
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

		// skip headers
		if strings.TrimSpace(record[1]) == "DBNAME" {
			continue
		}

		ts = append(ts, tableSpace{
			dbName:      strings.TrimSpace(record[1]),
			tsName:      strings.TrimSpace(record[2]),
			gbFreeOfMax: strings.TrimSpace(record[9]),
			date:        strings.TrimSpace(record[0]),
		})
	}
	return ts
}

// called from filepath.Walk
func parseCsvFileWithOpen(fileName string) []tableSpace {
	// Open file
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

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

		// skip headers
		if strings.TrimSpace(record[1]) == "DBNAME" {
			continue
		}

		ts = append(ts, tableSpace{
			dbName:      strings.TrimSpace(record[1]),
			tsName:      strings.TrimSpace(record[2]),
			gbFreeOfMax: strings.TrimSpace(record[9]),
			date:        strings.TrimSpace(record[0]),
		})
	}
	return ts
}

// Run parseCsvFile with one hardcoded file
func testParseCsvFile() {
	// Open file
	file, err := os.Open("data/cftwork.log")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	allTs := parseCsvFile(file)
	for _, ts := range allTs {
		fmt.Printf("%#v \n", ts)
	}
}

func testFilepathWalk() {
	allTs := []tableSpace{}
	filepath.Walk("./data", func(path string, fi os.FileInfo, err error) error {
		// skip directories
		if fi.IsDir() {
			return nil
		}

		// TODO: skip all hidden files or read only *.log files
		if path == "data/.DS_Store" {
			return nil
		}

		// TODO: why contains "," in PCT_FREE_OF_MAX ?
		if path == "data/INFORMER.log" {
			return nil
		}
		// TODO: why contains "," in PCT_FREE_OF_MAX ?
		if path == "data/SMSGATE.log" {
			return nil
		}

		// debug
		// fmt.Printf("%#v \n", path)

		allTs = append(allTs, parseCsvFileWithOpen(path)...)
		return nil

	})

	for _, ts := range allTs {
		fmt.Printf("%#v \n", ts)
	}
}

func main() {

	// testParseCsvFile()
	testFilepathWalk()

}
