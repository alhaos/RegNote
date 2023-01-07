package main

import (
	"database/sql"
	_ "github.com/denisenkom/go-mssqldb"
	"log"
)

func main() {

	var field1 int
	rows := MSSQL.QueryRow("select 1")
	if rows.Err() != nil {
		log.Fatalln("Query failed")
	}

	err := rows.Scan(&field1)
	if err != nil {
		log.Fatalln("FATAL: Scan error")
	}

	defer func(MSSQL *sql.DB) {
		err := MSSQL.Close()
		if err != nil {
			log.Fatalln("ERROR: Database close failed")
		}
	}(MSSQL)
}

type Config struct {
	MSSQLConnString  string
	SQLiteConnString string
}

var Conf Config

var MSSQL *sql.DB

func init() {
	Conf = Config{
		"sqlserver://acif:cipher-84T9u@192.168.101.222:1433?database=FINANCE",
		"jdbc:sqlite:C:/repositories/RegNote/RegNoteDB/RegNote.db",
	}

	var err error
	MSSQL, err = sql.Open("mssql", Conf.MSSQLConnString)
	if err != nil {
		log.Fatalf("ERROR: SQL Sever connection error: " + err.Error())
	}

	err = MSSQL.Ping()
	if err != nil {
		log.Fatalln("ERROR: Database ping failed")
	}

	log.Println("Database open")
}
