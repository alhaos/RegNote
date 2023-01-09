package mailer

import (
	"crypto/tls"
	"errors"
	"html/template"
	"log"
	"net/smtp"
)

type Mailer struct {
	conf Conf
}

type Conf struct {
	Server   string
	Host     string
	From     string
	UserName string
	Password string
}

func New() *Mailer {
	return &Mailer{
		Conf{
			Server:   "acex.ac.com:587",
			Host:     "acex.ac.com",
			From:     "accu-note@accureference.com",
			UserName: "accu-note@ac.com",
			Password: "widen-qmgBMw#",
		},
	}
}

func (m *Mailer) SendMail(body string, to []string, cc []string, bcc []string) {

	auth := LoginAuth(m.conf.UserName, m.conf.Password)

	tlsconfig := &tls.Config{
		InsecureSkipVerify: true,
		ServerName:         m.conf.Host,
	}

	c, err := smtp.Dial(m.conf.Server)
	if err != nil {
		log.Panic(err)
	}

	c.StartTLS(tlsconfig)

	// Auth
	if err = c.Auth(auth); err != nil {
		log.Panic(err)
	}

	// To && From
	if err = c.Mail(m.conf.From); err != nil {
		log.Panic(err)
	}

	for _, s := range to {
		if err = c.Rcpt(s); err != nil {
			log.Panic(err)
		}
	}

	for _, s := range cc {
		if err = c.Rcpt(s); err != nil {
			log.Panic(err)
		}
	}

	for _, s := range bcc {
		if err = c.Rcpt(s); err != nil {
			log.Panic(err)
		}
	}

	// Data
	w, err := c.Data()
	if err != nil {
		log.Panic(err)
	}

	tmpl, err := template.New("test").Parse(body)
	if err != nil {
		panic(err)
	}

	err = tmpl.Execute(w, nil)
	if err != nil {
		log.Panic(err)
	}

	err = w.Close()
	if err != nil {
		log.Panic(err)
	}

	err = c.Quit()
	if err != nil {
		panic(err)
	}
}

type loginAuth struct {
	username, password string
}

// LoginAuth is used for smtp login auth
func LoginAuth(username, password string) smtp.Auth {
	return &loginAuth{username, password}
}

func (a *loginAuth) Start(server *smtp.ServerInfo) (string, []byte, error) {
	return "LOGIN", []byte(a.username), nil
}

func (a *loginAuth) Next(fromServer []byte, more bool) ([]byte, error) {
	if more {
		switch string(fromServer) {
		case "Username:":
			return []byte(a.username), nil
		case "Password:":
			return []byte(a.password), nil
		default:
			return nil, errors.New("Unknown from server")
		}
	}
	return nil, nil
}
