package main

import (
	"database/sql"
	"github.com/alhaos/RegNoteLoader/cfxFiles"
	_ "github.com/denisenkom/go-mssqldb"
	_ "github.com/mattn/go-sqlite3"
	"gopkg.in/yaml.v3"
	"log"
	"os"
)

var conf Config
var LiteDB *sql.DB

type Config struct {
	SourceDirectory  string `yaml:"SourceDirectory"`
	LogDirectory     string `yaml:"LogDirectory"`
	DatabaseFileName string `yaml:"DatabaseFileName"`
}

func init() {
	conf = NewConfig()

	var err error
	LiteDB, err = sql.Open("sqlite3", conf.DatabaseFileName)
	if err != nil {
		log.Fatal(err)
	}
	cfxFiles.LiteDB = LiteDB
}

func main() {

	log.Println("RegNoteLoader start")

	// Get all file paths from source directory
	files, err := cfxFiles.GetFiles(conf.SourceDirectory)
	check(err)

	// Clear FILES_BUFFER table
	err = cfxFiles.ClearBufferTable()
	check(err)

	// Fill FILES_BUFFER table
	err = cfxFiles.DumpFiles(files)
	check(err)

	// Records with new files are added to the FILES table
	err = cfxFiles.FillNewFiles()
	check(err)

	// return new file paths from database
	newFiles, err := cfxFiles.GetNewFiles()
	check(err)

	// extracts tests from new files and puts them in a table
	rawTests, err := cfxFiles.ExtractTests(newFiles)
	check(err)

	err = cfxFiles.DumpTests(rawTests)
	check(err)

	log.Println("RegNoteLoader end")
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
