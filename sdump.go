// Command sdump dumps an sqlite database to a folder of tab-separated .txt
// files.
//
// It depends on the command sqlite3 being in the PATH.
package main

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
)

func fatal(args ...interface{}) {
	fmt.Fprintln(os.Stderr, args...)
	os.Exit(2)
}

var sqlite3 = "sqlite3"

type dbc string

func (db dbc) Command(sql string, flags ...string) *exec.Cmd {
	args := append([]string{string(db)}, flags...)
	args = append(args, sql)
	return exec.Command(sqlite3, args...)
}

func (db dbc) Exec(sql string, flags ...string) ([]byte, error) {
	return db.Command(sql, flags...).Output()
}

func (db dbc) Test() error {
	out, err := db.Exec(".schema")
	if err == nil && len(out) == 0 {
		return errors.New("no schema/database missing (" + string(db) + ")")
	}
	return err
}

func (db dbc) Tables() ([]string, error) {
	out, err := db.Exec(".tables")
	if err != nil {
		return nil, err
	}
	return strings.Fields(string(out)), nil
}

func (db dbc) Fname() string { return string(db) }

func main() {
	if (len(os.Args) != 3 && len(os.Args) != 2) || os.Args[1] == "-h" {
		fmt.Fprintln(os.Stderr, "usage:", os.Args[1], `<sqlitedb> [<outputdirectory>]`)
		os.Exit(1)
	}
	db, outdir := dbc(os.Args[1]), os.Args[1]+".dump"
	if len(os.Args) >= 3 {
		outdir = os.Args[2]
	}
	if err := os.MkdirAll(outdir, os.ModeDir|os.ModePerm); err != nil {
		fatal("could not create directory", outdir, " --", err)
	}
	if err := db.Test(); err != nil {
		fatal("db error:", err)
	}
	tables, err := db.Tables()
	if err != nil {
		fatal("error getting tables:", err.Error())
	}
	for _, table := range tables {
		var err error
		outfname := outdir + "/" + table + ".tsv"
		outf, err := os.Create(outfname)
		if err != nil {
			fatal("could not create output file", outfname, err)
		}
		cmd := exec.Command(sqlite3, "-header", db.Fname())
		cmd.Stdin = strings.NewReader(".mode tabs\n" + "SELECT * FROM " + table + ";\n")
		cmd.Stdout = outf
		if err = cmd.Run(); err != nil {
			fatal("error dumping table", table, err)
		}
		fmt.Println(outfname)
	}
}
