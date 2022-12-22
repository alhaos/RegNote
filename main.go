package main

import (
	"database/sql"
	_ "github.com/mattn/go-sqlite3"
	"gopkg.in/yaml.v3"
	"log"
	"os"
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

}

func main() {

	files, err := getFiles()
	check(err)

	err = truncate()
	check(err)

	err = dumpFiles(files)
	check(err)

	err = fillNewFiles()
	check(err)
}

func fillNewFiles() error {
	_, err := DB.Exec(`insert into FILES (NAME, IS_PROCESSED) select NAME, 0 from FILES_BUFFER fb where fb.NAME not in (select NAME from FILES)`)
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
			files = append(files, entry.Name())
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
