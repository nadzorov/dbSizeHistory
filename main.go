package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
)

type tableSpace struct {
	DbName      string `json:"dbname"`
	TsName      string `json:"tsname"`
	GbFreeOfMax string `json:"gbfreeofmax"`
	Date        string `json:"Date"`
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
			DbName:      strings.TrimSpace(record[1]),
			TsName:      strings.TrimSpace(record[2]),
			GbFreeOfMax: strings.TrimSpace(record[9]),
			Date:        strings.TrimSpace(record[0]),
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
			// log.Fatal(err)

			// possible line in files
			// ERROR:
			// ORA-00257: archiver error. Connect internal only, until freed.
			// ERROR:
			// ORA-28000: the account is locked
			// print in error stream like: 2016/03/17 09:08:59 line 55, column 0: wrong number of fields in line
			log.Println(err)
			continue
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
			DbName:      strings.TrimSpace(record[1]),
			TsName:      strings.TrimSpace(record[2]),
			GbFreeOfMax: strings.TrimSpace(record[9]),
			Date:        strings.TrimSpace(record[0]),
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
		// print only first line
		// break
	}
}

// parse all files in dir and return slice of tableSpace type
func parseCsvToTableSpace() []tableSpace {
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

	// debug
	// for _, ts := range allTs {
	// 	fmt.Printf("%#v \n", ts)
	// 	// print only first line
	// 	// break
	// }

	return allTs
}

// convert slice of tableSpace to json
func tableSpaceToJson(allTs []tableSpace) {
	for _, ts := range allTs {
		// debug
		// fmt.Printf("%#v \n", ts)

		// encode version with write to stdout
		// json.NewEncoder(os.Stdout).Encode(ts)

		// encode version with write to string or array of strings
		// ?

		// debug
		break

		// marshal version
		j, _ := json.Marshal(ts)
		fmt.Printf("%#v \n", string(j))
	}

	j, _ := json.Marshal(allTs)
	fmt.Printf("%#v \n", string(j))
}

// get uniq database list
func getDbList(allTs []tableSpace) map[string]int {
	dblist := make(map[string]int)
	for _, ts := range allTs {
		dblist[ts.DbName] += 1

		// debug
		// break
	}

	// debug
	// fmt.Printf("%#v \n", dblist)

	return dblist
}

func filterByDbName(allTs []tableSpace, dbname string) []tableSpace {
	filteredTs := []tableSpace{}
	for _, ts := range allTs {
		if ts.DbName == dbname {
			filteredTs = append(filteredTs, ts)
		}

	}

	return filteredTs
}

func main() {

	// testParseCsvFile()
	// testFilepathWalk()
	allTs := parseCsvToTableSpace()

	allTs = filterByDbName(allTs, "FS")

	tableSpaceToJson(allTs)

	dbList := getDbList(allTs)
	for db, _ := range dbList {
		fmt.Printf("%#v \n", db)
	}

}
