package cfxFiles

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// RawTest test from csv files
type RawTest struct {
	Filename   string
	Accession  string
	TestName   string
	TestResult string
}

var LiteDB *sql.DB

// GetFiles get a list of all files from the source directory
func GetFiles(sourceDirectory string) ([]string, error) {

	DirectoryEntry, err := os.ReadDir(sourceDirectory)

	if err != nil {
		log.Fatal(err)
	}

	var files []string
	for _, entry := range DirectoryEntry {
		if !entry.IsDir() {
			files = append(files, filepath.Join(sourceDirectory, entry.Name()))
		}
	}
	log.Printf("%v files found in: %v \r\n", len(files), sourceDirectory)
	return files, nil
}

// ClearBufferTable delete all records from FILES_BUFFER table
func ClearBufferTable() error {
	_, err := LiteDB.Exec("delete from FILES_BUFFER;")
	if err != nil {
		return err
	}
	log.Println("Table FILES_BUFFER cleared")
	return nil
}

// DumpFiles fill FILES_BUFFER table
func DumpFiles(files []string) error {
	var c string
	sb := strings.Builder{}

	for _, file := range files {
		c = fmt.Sprintf("insert into FILES_BUFFER (NAME) values('%v');", file)
		sb.WriteString(c)
	}

	_, err := LiteDB.Exec(sb.String())
	if err != nil {
		return err
	}
	log.Printf("All file names inserted into FILES_BUFFER")
	return nil
}

// FillNewFiles records with new files are added to the FILES table
func FillNewFiles() (err error) {
	_, err = LiteDB.Exec(`insert into FILES (NAME, IS_PROCESSED) select NAME, 0 from FILES_BUFFER fb where fb.NAME not in (select NAME from FILES)`)
	if err != nil {
		return err
	}
	return nil
}

// GetNewFiles return new file paths from database
func GetNewFiles() ([]string, error) {

	var files []string
	rows, err := LiteDB.Query("select NAME from FILES where IS_PROCESSED = 0")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		err = rows.Scan(&name)
		if err != nil {
			return nil, err
		}
		files = append(files, name)
	}
	log.Printf("%v new file(s) found", len(files))
	return files, nil
}

// ExtractTests extracts tests from new files and puts them in a table
func ExtractTests(files []string) ([]RawTest, error) {

	var RawTests []RawTest

	for _, file := range files {

		log.Printf("Process file: %v", file)

		f, err := os.Open(file)
		if err != nil {
			log.Fatal("Unable to read file "+file, err)
		}

		r := csv.NewReader(f)

		records, err := r.ReadAll()
		if err != nil {
			log.Fatal("Unable to read records from file "+file, err)
		}

		f.Close()

		for i := 0; i < len(records); i++ {
			if records[i][1] == "C19" || records[i][1] == "C19R" {
				rawTest := RawTest{
					Filename:   file,
					Accession:  records[i][0],
					TestName:   records[i][1],
					TestResult: records[i][2],
				}
				RawTests = append(RawTests, rawTest)
			}
			err = commitFile(file)
			if err != nil {
				log.Fatal("commitFile error")
			}
		}
	}
	return RawTests, nil
}

// DumpTests dump tests to table
func DumpTests(tests []RawTest) error {

	sb := strings.Builder{}
	for _, test := range tests {
		log.Printf("Found test %v, %v, %v",
			test.Accession,
			test.TestName,
			test.TestResult)
		cmd := fmt.Sprintf(
			"insert into RESULTS (FILENAME, ACCESSION, TESTNAME, TESTRESULT, DT, IS_PROCESSED) values ('%v','%v','%v','%v','%v','%v');\r\n",
			test.Filename,
			test.Accession,
			test.TestName,
			test.TestResult,
			time.Now().Format("2006-01-02 15:04:05"),
			0,
		)
		sb.WriteString(cmd)
	}
	_, err := LiteDB.Exec(sb.String())
	if err != nil {
		return err
	}
	return nil
}

// commitFile sets value 1 to field IS_PROCESSED in table FILES for processed records
func commitFile(filename string) (err error) {
	_, err = LiteDB.Exec("update FILES set IS_PROCESSED = 1 where name = ?", filename)
	if err != nil {
		return err
	}
	return nil
}
