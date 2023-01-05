package cfxFileProvider

import "C"
import (
	"database/sql"
	"log"
	"os"
	"path/filepath"
)

var DB *sql.DB

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
	_, err := DB.Exec("delete from FILES_BUFFER;")
	if err != nil {
		return err
	}
	return nil
}
