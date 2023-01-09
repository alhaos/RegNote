package main

import (
	"database/sql"
	"fmt"
	"github.com/alhaos/RegNote/RegNoteMailer/mailer"
	_ "github.com/denisenkom/go-mssqldb"
	_ "github.com/mattn/go-sqlite3"
	"gopkg.in/yaml.v2"
	"io"
	"log"
	"os"
	"path/filepath"
	"regexp"
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

type Result struct {
	Filename   string
	Accession  string
	TestResult string
	DT         string
}

type ClientAddresses struct {
	to  []string
	cc  []string
	bcc []string
}

type ClientEMail struct {
	TypeID   string
	Address  string
	ClientID string
}

var ClientsMap map[string][]DemographicInformation

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
	rows.Close()

	// init mailer
	Mailer = mailer.New()

	// init ClientsMap
	ClientsMap = map[string][]DemographicInformation{}
}

func main() {
	err := ProcessWaitingC19Results()
	if err != nil {
		log.Fatalln("FATAL: ProcessWaitingC19Results")
	}

	err = ProcessWaitingC19RResults()
	if err != nil {
		log.Fatalln("FATAL: ProcessWaitingC19RResults")
	}

	if len(ClientsMap) == 0 {
		log.Println("Nothing to report")
		log.Fatalln("Mailer end")
	}

	log.Println("Now reporting")
	for id, dInfos := range ClientsMap {
		err = ReportClient(id, dInfos)
		if err != nil {
			log.Fatalln("FATAL ReportClient:", id)
		}
	}

	log.Println("Mailer end")
}

// CommitAccession set is_processed = 1 in RESULT table on records with passed accession
func CommitAccession(acc string) {
	_, err := SQLiteDB.Exec("update RESULTS set IS_PROCESSED = '1' where ACCESSION = ?", acc)
	if err != nil {
		log.Fatalf("FATAL: Unable commit accession %v, %v", acc, err)
	}
	log.Printf("Accession %v commited", acc)
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

// ProcessWaitingC19Results
func ProcessWaitingC19Results() error {

	rows, err := SQLiteDB.Query(`select FILENAME, ACCESSION, TESTRESULT, DT from RESULTS where IS_PROCESSED = 0 and TESTNAME = 'C19'`)
	if err != nil {
		log.Fatalln("Unable to prepare stmt", err)
	}
	defer rows.Close()

	bill := 0
	for rows.Next() {
		bill++
		var r Result
		err = rows.Scan(&r.Filename, &r.Accession, &r.TestResult, &r.DT)
		if err != nil {
			log.Fatal("FATAL: Unable scan rows from SQLite", err)
		}

		log.Printf("Found waiting C19 result with accession: %v and result: %v", r.Accession, r.TestResult)
		di := FoundC19DemographicInformation(r)
		if di != nil {
			ClientsMap[di.ClientID] = append(ClientsMap[di.ClientID], *di)
		}
	}
	if bill == 0 {
		log.Println("No C19 waiting accessions found")
	}
	return nil
}

func FoundC19DemographicInformation(result Result) *DemographicInformation {

	var di DemographicInformation
	query := `
            select
              ISNULL(l.Accession, '')                                   [Accession]
            , ISNULL(l.[Final Report Date], '')                         [Final Report Date]
            , ISNULL(RTRIM(pd.[First Name]), '')                        [First Name]
            , ISNULL(RTRIM(pd.[Last Name]), '')                         [Last Name]
            , ISNULL(RTRIM(pd.[Middle Initial]), '')                    [Middle Name]
            , ISNULL(pd.DOB, '')                                        [DOB]
            , ISNULL(l.[Client ID], '')                                 [Client ID]
            , ISNULL(l.[Client Name], '')                               [Client Name]
            , ISNULL(l.[Phys  Name], '')                                [Phys Name]
            , ISNULL(l.[Test Code], '')                                 [Test Code]
            , ISNULL('SARS CoV-2, SWAB (PCR)', '')                      [Test Name]
            , ISNULL(RTRIM(LTRIM(l.Result)), '')                        [Test Result]
            , ISNULL(pd.Address, '')                                    [Patient Address]
            , ISNULL(pd.City, '')                                       [Patient City]
            , ISNULL(pd.[State], '')                                    [Patient State]
            , ISNULL(pd.Zip, '')                                        [Patient Zip]
            , ISNULL(pd.Phone, '')                                      [Patient Phone]
        from logtest_history l
        join PL_Patient_Demographics pd on l.Accession = pd.Accession
       where [Test Code] in ('950Z')
         and l.Result != 'ON'
         and l.Accession = ?`
	row := MSSQL.QueryRow(query, result.Accession)

	err := row.Scan(
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
	switch err {
	case nil:
		break
	case sql.ErrNoRows:
		log.Printf("No C19 result with accession %v found in Finance database", result.Accession)
		return nil
	default:
		log.Fatalf("FATAL: Unable scan c19 demographic information, %v", err)
	}
	di.DOB = parseDob(di.DOB)
	di.PatientPhone = phoneFix(di.PatientPhone)
	di.TestResult = result.TestResult
	di.FinalReportDate = time.Now().Format(`01/02/2006`)

	log.Printf("Found C19 result with acession %v in Finance database", result.Accession)
	return &di
}

func ProcessWaitingC19RResults() error {

	rows, err := SQLiteDB.Query(`select FILENAME, ACCESSION, TESTRESULT, DT from RESULTS where IS_PROCESSED = 0 and TESTNAME = 'C19R'`)
	if err != nil {
		log.Fatalln("Unable to prepare stmt", err)
	}
	defer rows.Close()

	bill := 0
	for rows.Next() {
		bill++
		var r Result
		err = rows.Scan(&r.Filename, &r.Accession, &r.TestResult, &r.DT)
		if err != nil {
			log.Fatal("FATAL: Unable scan rows from SQLite", err)
		}

		log.Printf("Found waiting C19 result with accession: %v and result: %v", r.Accession, r.TestResult)
		di := FoundC19RDemographicInformation(r)
		if di != nil {
			ClientsMap[di.ClientID] = append(ClientsMap[di.ClientID], *di)
		}
	}
	if bill == 0 {
		log.Println("No C19 waiting accessions found")
	}
	return nil
}

func FoundC19RDemographicInformation(result Result) *DemographicInformation {

	var di DemographicInformation
	query := `
            select
              ISNULL(l.Accession, '')                                   [Accession]
            , ISNULL(l.[Final Report Date], '')                         [Final Report Date]
            , ISNULL(RTRIM(pd.[First Name]), '')                        [First Name]
            , ISNULL(RTRIM(pd.[Last Name]), '')                         [Last Name]
            , ISNULL(RTRIM(pd.[Middle Initial]), '')                    [Middle Name]
            , ISNULL(pd.DOB, '')                                        [DOB]
            , ISNULL(l.[Client ID], '')                                 [Client ID]
            , ISNULL(l.[Client Name], '')                               [Client Name]
            , ISNULL(l.[Phys  Name], '')                                [Phys Name]
            , ISNULL(l.[Test Code], '')                                 [Test Code]
            , ISNULL('SARS CoV-2, SWAB (PCR)', '')                      [Test Name]
            , ISNULL(RTRIM(LTRIM(l.Result)), '')                        [Test Result]
            , ISNULL(pd.Address, '')                                    [Patient Address]
            , ISNULL(pd.City, '')                                       [Patient City]
            , ISNULL(pd.[State], '')                                    [Patient State]
            , ISNULL(pd.Zip, '')                                        [Patient Zip]
            , ISNULL(pd.Phone, '')                                      [Patient Phone]
        from logtest_history l
        join PL_Patient_Demographics pd on l.Accession = pd.Accession
       where [Test Code] in ('960Z')
         and l.Result != 'ON'
         and l.Accession = ?`

	stmt, err := MSSQL.Prepare(query)
	if err != nil {
		log.Fatalln("Fatal: unable prepare query,", err)
	}
	row := stmt.QueryRow(result.Accession)

	err = row.Scan(
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
	switch err {
	case nil:
		break
	case sql.ErrNoRows:
		log.Printf("No C19R result with accession %v found in Finance database", result.Accession)
		return nil
	default:
		log.Fatalf("FATAL: Unable scan C19R demographic information, %v", err)
	}
	di.DOB = parseDob(di.DOB)
	di.PatientPhone = phoneFix(di.PatientPhone)
	di.TestResult = result.TestResult
	di.FinalReportDate = time.Now().Format(`01/02/2006`)

	log.Printf("Found C19R result with acession %v in Finance database", result.Accession)
	return &di
}

func ReportClient(clientID string, dInfos []DemographicInformation) error {

	ca, ok := GetClientAddresses(clientID)

	if ok {
		log.Println("Found EMail configuration for client:", clientID)
	} else {
		log.Println("No EMail configuration found for client:", clientID)
		for _, info := range dInfos {
			CommitAccession(info.Accession)
		}
		return nil
	}

	sb := strings.Builder{}

	sb.WriteString("To:")
	for i, to_ := range ca.to {
		if i == 0 {
			sb.WriteString(to_)
		} else {
			sb.WriteString(";" + to_)
		}
	}
	sb.WriteString("\n")

	sb.WriteString("Cc:")
	for i, cc_ := range ca.cc {
		if i == 0 {
			sb.WriteString(cc_)
		} else {
			sb.WriteString(";" + cc_)
		}
	}
	sb.WriteString("\n")

	sb.WriteString("Bcc:")
	for i, bcc_ := range ca.bcc {
		if i == 0 {
			sb.WriteString(bcc_)
		} else {
			sb.WriteString(";" + bcc_)
		}
	}

	sb.WriteString("\n")
	sb.WriteString("Subject: Client " + clientID + " COVID-19 Result [secure]\n")
	sb.WriteString("MIME-version: 1.0;\nContent-Type: text/html; charset=\"UTF-8\";\n\n")
	sb.WriteString(`<!doctype html>
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
`)
	for _, info := range dInfos {
		sb.WriteString(`            <tr>
                <td>` + info.Accession + `</td>
                <td>` + info.FinalReportDate + `</td>
                <td>` + info.FirstName + `</td>
                <td>` + info.LastName + `</td>
                <td>` + info.MiddleName + `</td>
                <td>` + info.DOB + `</td>
                <td>` + info.ClientID + `</td>
                <td>` + info.ClientName + `</td>
                <td>` + info.PhysName + `</td>
                <td>` + info.TestCode + `</td>
                <td>` + info.TestName + `</td>
                <td>` + info.TestResult + `</td>
                <td>` + info.PatientAddress + `</td>
                <td>` + info.PatientCity + `</td>
                <td>` + info.PatientState + `</td>
                <td>` + info.PatientZip + `</td>
                <td>` + info.PatientPhone + `</td>
            </tr>`)
	}
	sb.WriteString(`        </tbody>
    </table>
</body>
</html>
`)

	Mailer.SendMail(sb.String(), ca.to, ca.cc, ca.bcc)

	for _, info := range dInfos {
		CommitAccession(info.Accession)
	}

	log.Println("Send EMail to", clientID)
	return nil
}

func GetClientAddresses(clientID string) (ClientAddresses, bool) {

	var ca ClientAddresses

	for i := range ClientEMails {
		if ClientEMails[i].ClientID == clientID {
			switch ClientEMails[i].TypeID {
			case "to":
				ca.to = append(ca.to, ClientEMails[i].Address)
			case "cc":
				ca.cc = append(ca.cc, ClientEMails[i].Address)
			case "bcc":
				ca.bcc = append(ca.bcc, ClientEMails[i].Address)
			}
		}
	}

	if len(ca.to)+len(ca.cc)+len(ca.bcc) == 0 {
		return ClientAddresses{}, false
	}

	return ca, true
}

func parseDob(t string) string {
	ti, err := time.Parse("2006-01-02T15:04:05Z", t)
	if err != nil {
		return "n/a"
	}
	return ti.Format("2006-01-02")
}

func phoneFix(ph string) string {
	re := regexp.MustCompile(`\D`)
	s := re.ReplaceAllString(ph, ``)
	if len(s) == 0 {
		return ""
	}
	re = regexp.MustCompile(`(\d{3})(\d{3})(\d{4})`)
	s = re.ReplaceAllString(s, `($1)$2-$3`)
	return s
}
