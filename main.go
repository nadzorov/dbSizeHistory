package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

type tableSpace struct {
	DbName      string `json:"dbname"`
	TsName      string `json:"tsname"`
	GbFreeOfMax int    `json:"gbfreeofmax"`
	Date        string `json:"Date"`
	GbAlloc     int    `json:"gballoc"`
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

		GbFreeOfMax, _ := strconv.Atoi(strings.TrimSpace(record[9]))
		GbAlloc, _ := strconv.Atoi(strings.TrimSpace(record[3]))
		ts = append(ts, tableSpace{
			DbName:      strings.TrimSpace(record[1]),
			TsName:      strings.TrimSpace(record[2]),
			GbFreeOfMax: GbFreeOfMax,
			Date:        strings.TrimSpace(record[0]),
			GbAlloc:     GbAlloc,
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

		GbFreeOfMax, _ := strconv.Atoi(strings.TrimSpace(record[9]))
		GbAlloc, _ := strconv.Atoi(strings.TrimSpace(record[3]))
		ts = append(ts, tableSpace{
			DbName:      strings.TrimSpace(record[1]),
			TsName:      strings.TrimSpace(record[2]),
			GbFreeOfMax: GbFreeOfMax,
			Date:        strings.TrimSpace(record[0]),
			GbAlloc:     GbAlloc,
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

	// marshal all version
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

func filterByTsName(allTs []tableSpace, tsname string) []tableSpace {
	filteredTs := []tableSpace{}
	for _, ts := range allTs {
		if ts.TsName == tsname {
			filteredTs = append(filteredTs, ts)
		}

	}

	return filteredTs
}

func filterByDate(allTs []tableSpace, date string) []tableSpace {
	filteredTs := []tableSpace{}
	for _, ts := range allTs {
		if ts.Date == date {
			filteredTs = append(filteredTs, ts)
		}

	}

	return filteredTs
}

func getDbSize(allTs []tableSpace) int {
	dbsize := 0
	for _, ts := range allTs {
		dbsize += ts.GbAlloc

	}

	return dbsize
}

func printDbSizeForAll() {
	fmt.Println("Start printDbSizeForAll()")
	allTs := parseCsvToTableSpace()
	for db, _ := range getDbList(allTs) {
		tsDb := filterByDbName(allTs, db)
		tsDbDate := filterByDate(tsDb, "2016-03-17")
		dbsize := getDbSize(tsDbDate)
		fmt.Printf("%#v %#v \n", db, dbsize)
	}
}

func getDbSizeForAll() map[string]int {
	fmt.Println("Start getDbSizeForAll()")
	dbSizeMap := make(map[string]int)
	allTs := parseCsvToTableSpace()
	for db, _ := range getDbList(allTs) {
		tsDb := filterByDbName(allTs, db)
		tsDbDate := filterByDate(tsDb, "2016-03-17")
		dbsize := getDbSize(tsDbDate)
		fmt.Printf("%#v %#v \n", db, dbsize)
		dbSizeMap[db] = dbsize
	}

	return dbSizeMap
}

func dbListHandler(w http.ResponseWriter, r *http.Request) {

	http.ServeFile(w, r, "dblist.html")
}

func tsListHandler(w http.ResponseWriter, r *http.Request) {

	http.ServeFile(w, r, "tslist.html")
}

func dbListJsonHandler(w http.ResponseWriter, r *http.Request) {

	w.Header().Set("Content-Type", "application/json")

	type row struct {
		DbName string `json:"dbname"`
		DbSize int    `json:"dbsize"`
	}

	dbSizeMap := getDbSizeForAll()

	tableRows := []row{}
	for db, size := range dbSizeMap {
		// fmt.Printf("%#v \n", db)
		tableRows = append(tableRows, row{DbName: db, DbSize: size})
	}

	json.NewEncoder(w).Encode(tableRows)

}

func tsListJsonHandler(w http.ResponseWriter, r *http.Request) {

	vars := mux.Vars(r)
	date := vars["date"]
	dbname := vars["dbname"]
	fmt.Printf("%#v \n", date)
	// for parameter like ?date=...
	// fmt.Printf("%#v \n", r.FormValue("date"))

	w.Header().Set("Content-Type", "application/json")

	allTs := parseCsvToTableSpace()
	if date != "" {
		allTs = filterByDate(allTs, date)
		// allTs = filterByDate(allTs, "2016-03-17")
	}
	if dbname != "" {
		allTs = filterByDbName(allTs, dbname)
		// allTs = filterByDate(allTs, "2016-03-17")
	}

	json.NewEncoder(w).Encode(allTs)
}

func chartHandler(w http.ResponseWriter, r *http.Request) {
	// fmt.Println("chart.html")
	http.ServeFile(w, r, "chart.html")
}

func chartJsonHandler(w http.ResponseWriter, r *http.Request) {

	chartLine := []int{}

	allTs := parseCsvToTableSpace()
	allTs = filterByDbName(allTs, "CFTWORK")
	allTs = filterByTsName(allTs, "I_USR")

	for _, ts := range allTs {
		chartLine = append(chartLine, ts.GbAlloc)
	}

	json.NewEncoder(w).Encode(chartLine)
}

func webServer() {

	routes := mux.NewRouter().StrictSlash(false)

	routes.HandleFunc("/dblist", dbListHandler).Methods("GET")
	routes.HandleFunc("/dblist.json", dbListJsonHandler).Methods("GET")

	routes.HandleFunc("/tslist", tsListHandler).Methods("GET")
	routes.HandleFunc("/tslist/", tsListHandler).Methods("GET")
	routes.HandleFunc("/tslist/dbname/{dbname}", tsListHandler).Methods("GET")
	routes.HandleFunc("/tslist/date/{date}", tsListHandler).Methods("GET")
	routes.HandleFunc("/api/tslist", tsListJsonHandler).Methods("GET")
	routes.HandleFunc("/api/tslist/", tsListJsonHandler).Methods("GET")
	routes.HandleFunc("/api/tslist/dbname/{dbname}", tsListJsonHandler).Methods("GET")
	routes.HandleFunc("/api/tslist/date/{date}", tsListJsonHandler).Methods("GET")

	routes.HandleFunc("/chart", chartHandler).Methods("GET")
	routes.HandleFunc("/chart.json", chartJsonHandler).Methods("GET")

	fmt.Println("Start listening...")
	http.ListenAndServe(":8080", routes)
}

func main() {

	webServer()
	// debug
	fmt.Println("after webServer()")

	// testParseCsvFile()
	// testFilepathWalk()
	allTs := parseCsvToTableSpace()

	allTs = filterByDbName(allTs, "FS")
	allTs = filterByDate(allTs, "2016-03-17")

	dbsize := getDbSize(allTs)
	fmt.Printf("%#v GB \n", dbsize)

	tableSpaceToJson(allTs)

	dbList := getDbList(allTs)
	for db, _ := range dbList {
		fmt.Printf("%#v \n", db)
	}

	printDbSizeForAll()
}
