package main

import (
	"bufio"
	"database/sql"
	_ "github.com/mattn/go-sqlite3"
	"gopkg.in/yaml.v3"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"
)

var Conf Config
var DB *sql.DB

type Config struct {
	SourceDirectory  string `yaml:"SourceDirectory"`
	LogDirectory     string `yaml:"LogDirectory"`
	DatabaseFileName string `yaml:"DatabaseFileName"`
}

func init() {
	Conf = NewConfig()
	var err error
	DB, err = sql.Open("sqlite3", Conf.DatabaseFileName)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Initialization complete")
}

func main() {

	files, err := getFiles()
	check(err)
	log.Printf("%v files found in: %v \r\n", len(files), Conf.SourceDirectory)

	err = truncate()
	check(err)
	log.Println("Buffer cleared")

	err = dumpFiles(files)
	check(err)
	log.Println("Files dumped")

	err = fillNewFiles()
	check(err)
	log.Println("New files filled")

	newFiles, err := GetNewFiles()
	check(err)

	for _, file := range newFiles {
		log.Printf("Found file: %v\r\n", file)
		err := processNewFile(file)
		check(err)
	}

	//accC19 := GetAccessionToSearch()
}

func processNewFile(filename string) (err error) {

	file, err := os.Open(filename)

	check(err)
	defer file.Close()

	scanner := bufio.NewScanner(file)
	counter := 0
	for scanner.Scan() {
		if counter != 0 {
			res := scanner.Text()
			log.Println(res)
			splits := strings.Split(res, ",")
			if splits[1] == "C19" || splits[1] == "C19R" {
				err = insertResult(filename, splits[0], splits[1], splits[2])
				check(err)
			}
		}
		counter++
	}

	if scanner.Err() != nil {
		log.Fatal(scanner.Err())
	}
	err = commitFile(filename)
	check(err)
	return
}

func commitFile(filename string) (err error) {
	_, err = DB.Exec("update files set IS_PROCESSED = 1 where name = ?", filename)
	if err != nil {
		return err
	}
	return nil
}

func insertResult(filename string, accession string, testName string, testResult string) (err error) {
	_, err = DB.Exec("insert into RESULTS (FILENAME, ACCESSION, TESTNAME, TESTRESULT, DT, IS_PROCESSED) values (?,?,?,?,?,?)", filename, accession, testName, testResult, time.Now().Format("2006-01-02 15:04:05"), 0)
	if err != nil {
		return err
	}
	return nil
}

func GetNewFiles() (files []string, err error) {
	rows, err := DB.Query("select NAME from FILES where IS_PROCESSED = 0")
	check(err)
	defer rows.Close()

	for rows.Next() {
		var name string
		err = rows.Scan(&name)
		check(err)
		files = append(files, name)
	}

	return
}

func fillNewFiles() (err error) {
	_, err = DB.Exec(`insert into FILES (NAME, IS_PROCESSED) select NAME, 0 from FILES_BUFFER fb where fb.NAME not in (select NAME from FILES)`)
	if err != nil {
		return err
	}
	return nil
}

func dumpFiles(files []string) (err error) {
	for _, file := range files {
		_, err := DB.Exec("insert into FILES_BUFFER (NAME) values(?);", file)
		if err != nil {
			return err
		}
	}
	return nil
}

func truncate() (err error) {
	_, err = DB.Exec("delete from FILES_BUFFER;")
	if err != nil {
		return err
	}
	return nil
}

func getFiles() (files []string, err error) {
	DirectoryEntry, err := os.ReadDir(Conf.SourceDirectory)
	if err != nil {
		log.Fatal(err)
	}

	for _, entry := range DirectoryEntry {
		if !entry.IsDir() {
			files = append(files, filepath.Join(Conf.SourceDirectory, entry.Name()))
		}
	}
	return
}

func NewConfig() Config {

	c := Config{}

	bytes, err := os.ReadFile("config.yml")

	if err != nil {
		log.Fatal(err)
	}

	err = yaml.Unmarshal(bytes, &c)
	if err != nil {
		log.Fatal(err)
	}
	return c
}

func check(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
