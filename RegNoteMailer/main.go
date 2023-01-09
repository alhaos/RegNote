package main

import (
	"bytes"
	"database/sql"
	"fmt"
	"github.com/alhaos/RegNote/RegNoteMailer/mailer"
	_ "github.com/denisenkom/go-mssqldb"
	_ "github.com/mattn/go-sqlite3"
	"gopkg.in/yaml.v2"
	"html/template"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type Config struct {
	MSSQLConnString  string `yaml:"MSSQLConnString"`
	SQLiteConnString string `yaml:"SQLiteConnString"`
	LogDirectory     string `yaml:"LogDirectory"`
}

type DemographicInformation struct {
	Accession       string
	FinalReportDate string
	FirstName       string
	LastName        string
	MiddleName      string
	DOB             string
	ClientID        string
	ClientName      string
	PhysName        string
	TestCode        string
	TestName        string
	TestResult      string
	PatientAddress  string
	PatientCity     string
	PatientState    string
	PatientZip      string
	PatientPhone    string
}

type ClientEMail struct {
	TypeID   string
	Address  string
	ClientID string
}

var ClientEMails []ClientEMail

var Conf Config

var MSSQL *sql.DB

var SQLiteDB *sql.DB

var Mailer *mailer.Mailer

// init
func init() {

	// Config init

	Conf = NewConfig()

	// Log init
	logFileName := fmt.Sprintf(`%v.log`, time.Now().Format("2006-01-02"))
	LogPath := filepath.Join("Log", logFileName)

	logFile, err := os.OpenFile(LogPath, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666)
	if err != nil {
		log.Fatalln("FATAL: Unable create log file")
	}

	mw := io.MultiWriter(logFile, os.Stdout)
	log.SetOutput(mw)
	log.Println("Mailer start")

	//Conf = Config{}
	//Conf.MSSQLConnString = `sqlserver://acif:cipher-84T9u@192.168.101.222:1433?database=FINANCE`
	//Conf.SQLiteConnString = `file:D:\repository\RegNote\RegNoteDB\RegNote.db?version=3'`
	//Conf.LogDirectory = "Logs"

	// init MSSQL database
	MSSQL, err = sql.Open("mssql", Conf.MSSQLConnString)
	if err != nil {
		log.Fatalf("FATAL: SQL Sever connection error: " + err.Error())
	}

	err = MSSQL.Ping()
	if err != nil {
		log.Fatalln("FATAL: Database ping failed", err)
	}

	// init SQLiteDB database
	SQLiteDB, err = sql.Open("sqlite3", Conf.SQLiteConnString)
	if err != nil {
		log.Fatalln("FATAL: Open SQLite database", err)
	}

	err = SQLiteDB.Ping()
	if err != nil {
		log.Fatalln("FATAL: SQLite database ping failed", err)
	}

	// Load client email to ClientEMails variable
	rows, err := SQLiteDB.Query("select TYPE_ID, ADDRESS, CLIENT_ID from CLIENT_EMAIL")

	for rows.Next() {
		ce := ClientEMail{}
		err = rows.Scan(&ce.TypeID, &ce.Address, &ce.ClientID)
		if err != nil {
			log.Fatalln("FATAL: unable scan client emails ", err)
		}
		ClientEMails = append(ClientEMails, ce)
	}

	// init mailer
	Mailer = mailer.New()
}

// main
func main() {

	WaitingAccessions, err := GetWaitingAccessions()
	if err != nil {
		log.Fatalln("Fatal: GetWaitingAccessions, ", err)
	}

	if len(WaitingAccessions) == 0 {
		log.Println("No waiting accessions found")
		log.Fatal("Mailer end")
	}

	di, err := GetIncomingDemographicInformation(WaitingAccessions)
	if err != nil {
		log.Fatalln("FATAL: GetIncomingDemographicInformation ", err)
	}

	for _, information := range di {
		err = SendMail(information)
		if err != nil {
			log.Fatalln("FATAL: Unable send EMail ", err)
		}
		CommitAccession(information.Accession)
	}

	log.Println("Mailer end")
}

// GetWaitingAccessions query unfinished accessions from SQLiteDB database
func GetWaitingAccessions() ([]string, error) {
	var WaitingAccessions []string

	rows, err := SQLiteDB.Query(`select ACCESSION from RESULTS where IS_PROCESSED = 0`)
	if err != nil {
		log.Fatalln("Unable to prepare stmt", err)
	}
	defer rows.Close()

	for rows.Next() {
		var acc string
		err = rows.Scan(&acc)
		if err != nil {
			log.Fatal("FATAL: Unable scan rows from SQLite", err)
		}
		WaitingAccessions = append(WaitingAccessions, acc)
	}

	log.Printf("Found %v WaitingAccessions", len(WaitingAccessions))

	return WaitingAccessions, nil
}

// GetIncomingDemographicInformation query incoming demographic information from MSSQL server
func GetIncomingDemographicInformation(waitingAccessions []string) ([]DemographicInformation, error) {

	var IncomingDemographicInformation []DemographicInformation

	sb := strings.Builder{}

	sb.WriteString(`
            select
            l.Accession                                     [Accession]
            , l.[Final Report Date]                         [Final Report Date]
            , RTRIM(pd.[First Name])                        [First Name]
            , RTRIM(pd.[Last Name])                         [Last Name]
            , RTRIM(pd.[Middle Initial])                    [Middle Name]
            , pd.DOB                                        [DOB]
            , l.[Client ID]                                 [Client ID]
            , l.[Client Name]                               [Client Name]
            , l.[Phys  Name]                                [Phys Name]
            , l.[Test Code]                                 [Test Code]
            , 'SARS CoV-2, SWAB (PCR)'                      [Test Name]
            , RTRIM(LTRIM(l.Result))                        [Test Result]
            , pd.Address                                    [Patient Address]
            , pd.City                                       [Patient City]
            , pd.[State]                                    [Patient State]
            , pd.Zip                                        [Patient Zip]
            , pd.Phone                                      [Patient Phone]
        from logtest_history l
        join PL_Patient_Demographics pd on l.Accession = pd.Accession
       where [Test Code] in ('950Z', '960Z')
         and l.Result != 'ON'
         and l.Accession in
(`)

	for i, accession := range waitingAccessions {
		if i == 0 {
			sb.WriteString("'" + accession + "'")
		} else {
			sb.WriteString(",'" + accession + "'")
		}
	}

	sb.WriteString(");")

	query := sb.String()

	rows, err := MSSQL.Query(query)
	if err != nil {
		log.Fatalln("FATAL: unable query demographic information: ", err)
	}

	for rows.Next() {
		var di DemographicInformation
		err = rows.Scan(
			&di.Accession,
			&di.FinalReportDate,
			&di.FirstName,
			&di.LastName,
			&di.MiddleName,
			&di.DOB,
			&di.ClientID,
			&di.ClientName,
			&di.PhysName,
			&di.TestCode,
			&di.TestName,
			&di.TestResult,
			&di.PatientAddress,
			&di.PatientCity,
			&di.PatientState,
			&di.PatientZip,
			&di.PatientPhone,
		)

		di.FinalReportDate = time.Now().Format(`01/02/2006`)
		di.TestResult = interpretTestResult(di.TestResult)
		IncomingDemographicInformation = append(IncomingDemographicInformation, di)
	}

	log.Printf("Found %v incoming demographic information", len(IncomingDemographicInformation))

	return IncomingDemographicInformation, nil
}

// SendMail
func SendMail(di DemographicInformation) error {

	var (
		to  []string
		cc  []string
		bcc []string
	)

	for i := range ClientEMails {
		if ClientEMails[i].ClientID == di.ClientID {
			switch ClientEMails[i].TypeID {
			case "to":
				to = append(to, ClientEMails[i].Address)
			case "cc":
				cc = append(cc, ClientEMails[i].Address)
			case "bcc":
				bcc = append(bcc, ClientEMails[i].Address)
			}
		}
	}

	if len(to)+len(cc)+len(bcc) == 0 {
		log.Printf("No EMail config found for client: %v, %v", di.ClientID, di.ClientName)
		return nil
	}

	subj := "Client " + di.ClientID + " COVID-19 Result [secure]"

	Mailer.SendMail(MakeBody(di), subj, to, cc, bcc)

	return nil
}

// MakeBody create string mail body from instance of DemographicInformation structure
func MakeBody(information DemographicInformation) string {

	t, err := template.New("MailBody").Parse(`
<!doctype html>
<html xmlns="http://www.w3.org/1999/xhtml">
<head>
    <title>Client report</title>
    <style>
        BODY {
            font-family: Arial;
            font-size: 10pt;
        }
        
        TABLE {
            border: 1px solid black;
            border-collapse: collapse;
        }
        
        TH {
            border: 1px solid black;
            background: #dddddd;
            padding: 5px;
        }
        
        TD {
            border: 1px solid black;
            padding: 5px;
        }
    </style>
</head>
<body>
    <table>
        <thead>
            <tr>
                <th>ACCESSION</th>
                <th>Final Report Date</th>
                <th>First Name</th>
                <th>Last Name</th>
                <th>Middle Name</th>
                <th>DOB</th>
                <th>Client ID</th>
                <th>Client Name</th>
                <th>Phys Name</th>
                <th>Test Code</th>
                <th>Test Name</th>
                <th>Test Result</th>
                <th>Patient Address</th>
                <th>Patient City</th>
                <th>Patient State</th>
                <th>Patient Zip</th>
                <th>Patient Phone</th>
            </tr>
        </thead>
        <tbody>
            <tr>
                <td>{{ .Accession }}</td>
                <td>{{ .FinalReportDate }}</td>
                <td>{{ .FirstName }}</td>
                <td>{{ .LastName }}</td>
                <td>{{ .MiddleName }}</td>
                <td>{{ .DOB }}</td>
                <td>{{ .ClientID }}</td>
                <td>{{ .ClientName }}</td>
                <td>{{ .PhysName }}</td>
                <td>{{ .TestCode }}</td>
                <td>{{ .TestName }}</td>
                <td>{{ .TestResult }}</td>
                <td>{{ .PatientAddress }}</td>
                <td>{{ .PatientCity }}</td>
                <td>{{ .PatientState }}</td>
                <td>{{ .PatientZip }}</td>
                <td>{{ .PatientPhone }}</td>
            </tr>
        </tbody>
    </table>
</body>
</html>
`)
	if err != nil {
		log.Fatalln("FATAL: unable parse template: ", err)
	}

	var tpl bytes.Buffer
	err = t.Execute(&tpl, information)
	if err != nil {
		log.Fatalln("FATAL: Unable execute template, ", err)
	}

	return tpl.String()
}

// CommitAccession set is_processed = 1 in RESULT table on records with passed accession
func CommitAccession(acc string) {
	_, err := SQLiteDB.Exec("update RESULTS set IS_PROCESSED = '1' where ACCESSION = ?", acc)
	if err != nil {
		log.Fatalf("FATAL: Unable commit accession %v, %v", acc, err)
	}
	log.Printf("Accession %v commited", acc)
}

// interpretTestResult
func interpretTestResult(r string) string {
	switch r {
	case "D":
		return "Detected"
	case "ND":
		return "Not detected"
	case "INV":
		return "Invalid"
	case "IN":
		return "Presumptive positive"
	default:
		return r
	}
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
