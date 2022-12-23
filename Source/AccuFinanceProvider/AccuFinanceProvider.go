package AccuFinanceProvider

import (
	"database/sql"
	"fmt"
)

type AccuFinanceProvider struct {
	DB *sql.DB
}

func New() *AccuFinanceProvider {
	afp := &AccuFinanceProvider{}
	afp.DB = sql.Open("")

	sql.Open("mssql", database.ConnectionString)
	if connectionError != nil {
		fmt.Println(fmt.Errorf("error opening database: %v", connectionError))
	}
	return afp
}
