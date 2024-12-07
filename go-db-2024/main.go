package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/chzyer/readline"
	"github.com/srmadden/godb"
)

var helpText = `Enter a SQL query terminated by a ; to process it.  Commands prefixed with \ are processed as shell commands.

Available shell commands:
	\h : This help
	\c path/to/catalog : Change the current database to a specified catalog file
	\d : List tables and fields in the current database
	\f : List available functions for use in queries
	\a : Toggle aligned vs csv output
    \o : Toggle query optimization
	\l table path/to/file [sep] [hasHeader]: Append csv file to end of table.  Default to sep = ',', hasHeader = 'true'
	\i path/to/file [useMetaDataFile] [useStatFile] [mode] [extension] [sep] [hasHeader]: Change the current database to a specified catalog file, and load from csv-like files in same directory as catalog file, with given separator Default to mode = 'Some' (Options 'All', 'Some', 'Diagnostic'), extension = 'tbl', sep = '|', hasHeader = 'true'
		- mode 'All' loads all the data from the csv
		- mode 'Some' loads only some of the data from the csv, randomly seeking to each line
		- mode 'Contiguous' loads only some of the data from the csv, in an in-order contiguous manner
		- mode 'Stratified' loads only some of the data from the csv, in reading contiguously starting from a random offset
		- useMetaDataFile will store the offsets that have been loaded in order to not load them again
		- useStatFile will store statistics for each numerical column in order to make queries more accurate

	\z : Compute statistics for the database`

func printCatalog(c *godb.Catalog) {
	s := c.CatalogString()
	fmt.Printf("\033[34m%s\n\033[0m", s)
}

func main() {
	alarm := make(chan int, 1)

	go func() {
		c := make(chan os.Signal)

		signal.Notify(c, os.Interrupt, syscall.SIGINT)
		go func() {
			for {
				<-c
				alarm <- 1
				fmt.Println("Interrupted query.")
			}
		}()

	}()

	bp, err := godb.NewBufferPool(10200)
	if err != nil {
		log.Fatal(err.Error())
	}

	catName := "catalog.txt"
	catPath := "godb"
	mode := "Contiguous"
	useMetaDataFile := true
	useStatFile := true
	extension := "tbl"
	sep := "|"
	hasHeader := false

	c, err := godb.NewCatalogFromFile(catName, bp, catPath)
	if err != nil {
		fmt.Printf("failed load catalog, %s", err.Error())
		return
	}
	// fmt.Println("YOOO")
	rl, err := readline.New("> ")
	if err != nil {
		panic(err)
	}
	defer rl.Close()

	fmt.Printf("\033[35;1m")
	fmt.Println(`Welcome to

	[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m[38;5;16;48;5;16m[0m
	[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;93;48;5;54m░[38;5;55;48;5;54m░[38;5;55;48;5;54m░[38;5;93;48;5;54m░[38;5;55;48;5;54m░[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;55;48;5;54m░[38;5;55;48;5;54m░[38;5;55;48;5;54m░[38;5;55;48;5;54m░[38;5;55;48;5;54m░[38;5;55;48;5;54m░[38;5;55;48;5;54m░[38;5;55;48;5;54m░[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;55;48;5;54m░[38;5;55;48;5;54m░[38;5;93;48;5;54m░[38;5;55;48;5;54m░[38;5;55;48;5;54m░[38;5;55;48;5;54m░[38;5;55;48;5;54m░[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m[38;5;16;48;5;16m[0m
	[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;92;48;5;54m░[38;5;92;48;5;54m░[38;5;92;48;5;54m░[38;5;92;48;5;54m░[38;5;92;48;5;54m░[38;5;92;48;5;54m░[38;5;92;48;5;54m░[38;5;92;48;5;54m░[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;92;48;5;54m░[38;5;92;48;5;54m░[38;5;92;48;5;54m░[38;5;92;48;5;54m░[38;5;92;48;5;54m░[38;5;92;48;5;54m░[38;5;92;48;5;54m░[38;5;92;48;5;54m░[38;5;92;48;5;54m░[38;5;92;48;5;54m░[38;5;92;48;5;54m░[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;92;48;5;54m░[38;5;92;48;5;54m░[38;5;92;48;5;54m░[38;5;92;48;5;54m░[38;5;92;48;5;54m░[38;5;92;48;5;54m░[38;5;92;48;5;54m░[38;5;92;48;5;54m░[38;5;92;48;5;54m░[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m[38;5;16;48;5;16m[0m
	[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;91;48;5;54m░[38;5;128;48;5;54m░[38;5;91;48;5;54m░[38;5;128;48;5;54m░[38;5;91;48;5;54m░[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;128;48;5;54m░[38;5;91;48;5;54m░[38;5;91;48;5;54m░[38;5;91;48;5;54m░[38;5;128;48;5;54m░[38;5;129;48;5;232m [38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;128;48;5;54m░[38;5;91;48;5;54m░[38;5;91;48;5;54m░[38;5;128;48;5;54m░[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;91;48;5;54m░[38;5;128;48;5;54m░[38;5;91;48;5;54m░[38;5;91;48;5;54m░[38;5;91;48;5;54m░[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;91;48;5;54m░[38;5;128;48;5;54m░[38;5;91;48;5;54m░[38;5;128;48;5;54m░[38;5;16;48;5;16m▓[38;5;91;48;5;54m░[38;5;128;48;5;54m░[38;5;91;48;5;54m░[38;5;91;48;5;54m░[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m[38;5;16;48;5;16m[0m
	[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;170;48;5;5m░[38;5;170;48;5;5m░[38;5;170;48;5;5m░[38;5;170;48;5;5m░[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;170;48;5;5m░[38;5;170;48;5;5m░[38;5;170;48;5;5m░[38;5;170;48;5;5m░[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;170;48;5;5m░[38;5;170;48;5;5m░[38;5;170;48;5;5m░[38;5;170;48;5;5m░[38;5;170;48;5;5m░[38;5;170;48;5;5m░[38;5;170;48;5;5m░[38;5;170;48;5;5m░[38;5;170;48;5;5m░[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;170;48;5;5m░[38;5;170;48;5;5m░[38;5;170;48;5;5m░[38;5;170;48;5;5m░[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;53;48;5;53m░[38;5;170;48;5;5m░[38;5;170;48;5;5m░[38;5;170;48;5;5m░[38;5;170;48;5;5m░[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;170;48;5;5m░[38;5;170;48;5;5m░[38;5;170;48;5;5m░[38;5;170;48;5;5m░[38;5;170;48;5;5m░[38;5;170;48;5;5m░[38;5;170;48;5;5m░[38;5;53;48;5;53m░[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m[38;5;16;48;5;16m[0m
	[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;163;48;5;90m░[38;5;163;48;5;90m░[38;5;163;48;5;90m░[38;5;163;48;5;90m░[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;163;48;5;90m░[38;5;163;48;5;90m░[38;5;163;48;5;90m░[38;5;163;48;5;90m░[38;5;163;48;5;5m░[38;5;163;48;5;5m░[38;5;163;48;5;90m░[38;5;163;48;5;90m░[38;5;163;48;5;90m░[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;163;48;5;90m░[38;5;163;48;5;90m░[38;5;163;48;5;90m░[38;5;163;48;5;90m░[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;163;48;5;90m░[38;5;163;48;5;90m░[38;5;163;48;5;90m░[38;5;163;48;5;90m░[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;163;48;5;90m░[38;5;163;48;5;90m░[38;5;163;48;5;90m░[38;5;163;48;5;90m░[38;5;200;48;5;89m░[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;163;48;5;90m░[38;5;163;48;5;90m░[38;5;163;48;5;90m░[38;5;163;48;5;90m░[38;5;163;48;5;90m░[38;5;163;48;5;90m░[38;5;163;48;5;90m░[38;5;163;48;5;90m░[38;5;163;48;5;90m░[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m[38;5;16;48;5;16m[0m
	[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;199;48;5;126m░[38;5;199;48;5;126m░[38;5;199;48;5;126m░[38;5;199;48;5;126m░[38;5;199;48;5;126m░[38;5;199;48;5;126m░[38;5;199;48;5;126m░[38;5;199;48;5;126m░[38;5;199;48;5;126m░[38;5;199;48;5;126m░[38;5;199;48;5;126m░[38;5;16;48;5;16m▓[38;5;199;48;5;126m░[38;5;199;48;5;126m░[38;5;199;48;5;126m░[38;5;199;48;5;126m░[38;5;199;48;5;126m░[38;5;199;48;5;126m░[38;5;199;48;5;126m░[38;5;199;48;5;126m░[38;5;199;48;5;126m░[38;5;199;48;5;126m░[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;199;48;5;126m░[38;5;199;48;5;126m░[38;5;199;48;5;126m░[38;5;199;48;5;126m░[38;5;199;48;5;126m░[38;5;199;48;5;126m░[38;5;199;48;5;126m░[38;5;199;48;5;126m░[38;5;199;48;5;126m░[38;5;199;48;5;126m░[38;5;199;48;5;126m░[38;5;199;48;5;126m░[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;199;48;5;126m░[38;5;199;48;5;126m░[38;5;199;48;5;126m░[38;5;199;48;5;126m░[38;5;212;48;5;53m░[38;5;199;48;5;126m░[38;5;199;48;5;126m░[38;5;199;48;5;126m░[38;5;199;48;5;126m░[38;5;199;48;5;126m░[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m[38;5;16;48;5;16m[0m
	[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;162;48;5;126m░[38;5;125;48;5;126m░[38;5;162;48;5;126m░[38;5;162;48;5;126m░[38;5;162;48;5;126m░[38;5;162;48;5;126m░[38;5;162;48;5;126m░[38;5;162;48;5;126m░[38;5;162;48;5;126m░[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;162;48;5;126m░[38;5;162;48;5;126m░[38;5;162;48;5;126m░[38;5;162;48;5;126m░[38;5;162;48;5;126m░[38;5;125;48;5;126m░[38;5;162;48;5;126m░[38;5;162;48;5;89m░[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;162;48;5;126m░[38;5;162;48;5;126m░[38;5;162;48;5;126m░[38;5;162;48;5;126m░[38;5;162;48;5;126m░[38;5;162;48;5;126m░[38;5;162;48;5;126m░[38;5;162;48;5;126m░[38;5;162;48;5;126m░[38;5;162;48;5;126m░[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;162;48;5;126m░[38;5;162;48;5;126m░[38;5;162;48;5;126m░[38;5;162;48;5;126m░[38;5;162;48;5;126m░[38;5;162;48;5;126m░[38;5;162;48;5;126m░[38;5;162;48;5;126m░[38;5;162;48;5;126m░[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m[38;5;16;48;5;16m[0m
	[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m[38;5;16;48;5;16m[0m
	[38;5;92;48;48;5;16m						 	  [0m
	[38;5;92;48;48;5;16m		██████╗ ███████╗ █████╗[38;5;16;48;5;16m 		  [0m
	[38;5;129;48;48;5;16m		██╔══██╗██╔════╝██╔══██╗[38;5;16;48;5;16m		  [0m
	[38;5;165;48;48;5;16m		██████╔╝███████╗███████║[38;5;16;48;5;16m		  [0m
	[38;5;201;48;48;5;16m		██╔═══╝ ╚════██║██╔══██║[38;5;16;48;5;16m		  [0m
	[38;5;163;48;48;5;16m		██║     ███████║██║  ██║[38;5;16;48;5;16m		  [0m
	[38;5;5;48;48;5;16m		╚═╝     ╚══════╝╚═╝  ╚═╝[38;5;16;48;5;16m		  [0m
	[38;5;92;48;48;5;16m						 	  [0m
[35;1mType \h for help`)
	fmt.Printf("\033[0m\n")
	query := ""
	var autocommit bool = true
	var tid godb.TransactionID
	aligned := true
	for {
		text, err := rl.Readline()
		// fmt.Printf("READ A LINE %v\n", text)
		if err != nil { // io.EOF
			break
		}
		text = strings.TrimSpace(text)
		if len(text) == 0 {
			continue
		}

		start := time.Now()
		if text[0] == '\\' {
			switch text[1] {
			case 'd':
				printCatalog(c)
			case 'c':
				if len(text) <= 3 {
					fmt.Printf("Expected catalog file name after \\c")
					continue
				}
				rest := text[3:]
				pathAr := strings.Split(rest, "/")
				catName = pathAr[len(pathAr)-1]
				catPath = strings.Join(pathAr[0:len(pathAr)-1], "/")
				c, err = godb.NewCatalogFromFile(catName, bp, catPath)
				if err != nil {
					fmt.Printf("failed load catalog, %s\n", err.Error())
					continue
				}
				fmt.Printf("Loaded %s/%s\n", catPath, catName)
				printCatalog(c)
			case 'f':
				fmt.Println("Available functions:")
				fmt.Print(godb.ListOfFunctions())
			case 'a':
				aligned = !aligned
				if aligned {
					fmt.Println("Output aligned")
				} else {
					fmt.Println("Output unaligned")
				}
			case 'o':
				godb.EnableJoinOptimization = !godb.EnableJoinOptimization
				if godb.EnableJoinOptimization {
					fmt.Println("\033[32;1mOptimization enabled\033[0m\n\n")
				} else {
					fmt.Println("\033[32;1mOptimization disabled\033[0m\n\n")
				}
			case 'z':
				c.ComputeTableStats()
				fmt.Printf("\033[32;1mAnalysis Complete\033[0m\n\n")
			case '?':
				fallthrough
			case 'h':
				fmt.Println(helpText)
			case 'l':
				splits := strings.Split(text, " ")
				table := splits[1]
				path := splits[2]
				sep := ","
				hasHeader := true
				if len(splits) > 3 {
					sep = splits[3]
				}
				if len(splits) > 4 {
					hasHeader = splits[4] != "false"
				}

				//todo -- following code assumes data is in heap files
				hf, err := c.GetTable(table)
				if err != nil {
					fmt.Printf("\033[31;1m%s\033[0m\n", err.Error())
					continue
				}
				heapFile := hf.(*godb.HeapFile)
				f, err := os.Open(path)
				if err != nil {
					fmt.Printf("\033[31;1m%s\033[0m\n", err.Error())
					continue
				}
				err = heapFile.LoadFromCSV(f, hasHeader, sep, false)
				if err != nil {
					fmt.Printf("\033[31;1m%s\033[0m\n", err.Error())
					continue
				}
				bp.FlushAllPages() //gross, if in a transaction, but oh well!
				fmt.Printf("\033[32;1mLOAD\033[0m\n\n")
			case 'i':
				bp.CanFlushWhenFull = true
				// load the catalog file
				if len(text) <= 3 {
					fmt.Printf("Expected catalog file name after \\c")
					continue
				}
				splits := strings.Split(text, " ")
				// [useMetaDataFile] [useStatFile] [mode] [extension] [sep] [hasHeader]
				path := splits[1]
				if len(splits) > 2 {
					useMetaDataFile = splits[2] != "false"
				}
				if len(splits) > 3 {
					useStatFile = splits[3] != "false"
				}
				if len(splits) > 4 {
					mode = splits[4]
				}
				if len(splits) > 5 {
					extension = splits[5]
				}
				if len(splits) > 6 {
					sep = splits[6]
				}
				if len(splits) > 7 {
					hasHeader = splits[7] != "false"
				}
				pathAr := strings.Split(path, "/")
				catName = pathAr[len(pathAr)-1]
				catPath = strings.Join(pathAr[0:len(pathAr)-1], "/")
				// fmt.Printf("catName is %v, catPath is %v\n", catName, catPath)
				c, err = godb.NewCatalogFromFile(catName, bp, catPath, useMetaDataFile, useStatFile)
				if err != nil {
					fmt.Printf("failed load catalog, %s\n", err.Error())
					continue
				}
				fmt.Printf("Loaded %s/%s %v\n", catPath, catName, c.TableNames())
				printCatalog(c)

				// load the csv files to each table

				if mode == "Some" || mode == "Contiguous" || mode == "Stratified" {
					continue
				}

				if mode == "Stat" {
					// In first (read) pass, compute mean, std dev for full
					// dataset.
					// In second (write) pass, insert to database tuples
					// containing numerical fields all within two standard
					// deviations from the mean.
					for _, tableName := range c.TableNames() {
						fmt.Printf("Processing table: %v\n", tableName)
						hf, err := c.GetTable(tableName)
						if err != nil {
							fmt.Printf("\033[31;1m%s\033[0m\n", err.Error())
							continue
						}
						heapFile := hf.(*godb.HeapFile)
						f, err := os.Open(fmt.Sprintf("%v/%v.%v", catPath, tableName, extension))
						if err != nil {
							fmt.Printf("\033[31;1m%s\033[0m\n", err.Error())
							continue
						}
						statFilename := catPath + "/" + tableName + "Stat.txt"
						err = heapFile.StatAndLoadFromCSV(f, hasHeader, sep, false, statFilename)
						if err != nil {
							fmt.Printf("\033[31;1m%s\033[0m\n", err.Error())
							continue
						}
					}

					continue
				}

				for _, tableName := range c.TableNames() {
					//todo -- following code assumes data is in heap files
					hf, err := c.GetTable(tableName)
					if err != nil {
						fmt.Printf("\033[31;1m%s\033[0m\n", err.Error())
						continue
					}
					heapFile := hf.(*godb.HeapFile)
					f, err := os.Open(fmt.Sprintf("%v/%v.%v", catPath, tableName, extension))
					if err != nil {
						fmt.Printf("\033[31;1m%s\033[0m\n", err.Error())
						continue
					}
					err = heapFile.LoadFromCSV(f, hasHeader, sep, false)
					if err != nil {
						fmt.Printf("\033[31;1m%s\033[0m\n", err.Error())
						continue
					}
				}
				bp.FlushAllPages() //gross, if in a transaction, but oh well!
				fmt.Printf("\033[32;1mLOAD\033[0m\n\n")
				bp.CanFlushWhenFull = false
				// fmt.Printf("\033[32;1m(%d results)\033[0m\n", nresults)
				duration := time.Since(start)
				fmt.Printf("\033[32;1m%v\033[0m\n\n", duration)
			}

			query = ""
			continue
		}
		if text[len(text)-1] != ';' {
			query = query + " " + text
			continue
		}
		query = strings.TrimSpace(query + " " + text[0:len(text)-1])

		explain := false
		if strings.HasPrefix(strings.ToLower(query), "explain") {
			queryParts := strings.Split(query, " ")
			query = strings.Join(queryParts[1:], " ")
			explain = true
		}

		tableNames, queryType, plan, err := godb.Parse(c, query)
		query = ""
		nresults := 0

		if err != nil {
			errStr := err.Error()
			if strings.Contains(errStr, "position") {
				positionPos := strings.LastIndex(errStr, "position")
				positionPos += 9

				spacePos := strings.Index(errStr[positionPos:], " ")
				if spacePos == -1 {
					spacePos = len(errStr) - positionPos
				}

				posStr := errStr[positionPos : spacePos+positionPos]
				pos, err := strconv.Atoi(posStr)
				if err == nil {
					s := strings.Repeat(" ", pos)
					fmt.Printf("\033[31;1m%s^\033[0m\n", s)
				}
			}
			fmt.Printf("\033[31;1mInvalid query (%s)\033[0m\n", err.Error())
			continue
		}

		if mode == "Some" || mode == "Contiguous" || mode == "Stratified" || mode == "Stat" {
			// load more from each table
			for tableName, _ := range tableNames {
				//todo -- following code assumes data is in heap files
				hf, err := c.GetTable(tableName)
				if err != nil {
					fmt.Printf("\033[31;1m%s\033[0m\n", err.Error())
					continue
				}
				heapFile := hf.(*godb.HeapFile)
				f, err := os.Open(fmt.Sprintf("%v/%v.%v", catPath, tableName, extension))
				if err != nil {
					fmt.Printf("\033[31;1m%s\033[0m\n", err.Error())
					continue
				}
				if mode == "Some" {
					err = heapFile.LoadSomeFromCSV(f, hasHeader, sep, false, nil)
				} else if mode == "Stat" {
					err = heapFile.LoadSomeFromCSV(f, hasHeader, sep, false, heapFile.Statistics())

				} else if mode == "Contiguous" {
					err = heapFile.LoadSomeFromCSVContiguous(f, hasHeader, sep, false)
				} else if mode == "Stratified" {
					err = heapFile.LoadSomeFromCSVContiguousStratified(f, hasHeader, sep, false)
				}
				if err != nil {
					fmt.Printf("\033[31;1m%s\033[0m\n", err.Error())
					continue
				}
				fmt.Printf("loaded more info from table %v\n", tableName)
			}
		}

		switch queryType {
		case godb.UnknownQueryType:
			fmt.Printf("\033[31;1mUnknown query type\033[0m\n")
			continue

		case godb.IteratorType:
			if explain {
				fmt.Printf("\033[32m")
				godb.PrintPhysicalPlan(plan, "")
				fmt.Printf("\033[0m\n")
				break
			}
			if autocommit {
				tid = godb.NewTID()
				err := bp.BeginTransaction(tid)
				if err != nil {
					fmt.Printf("\033[31;1m%s\033[0m\n", err.Error())
					continue
				}
			}

			iter, err := plan.Iterator(tid)
			if err != nil {
				fmt.Printf("\033[31;1m%s\033[0m\n", err.Error())
				continue
			}

			fmt.Printf("\033[32;4m%s\033[0m\n", plan.Descriptor().HeaderString(aligned))

			for {
				tup, err := iter()
				if err != nil {
					fmt.Printf("%s\n", err.Error())
					break
				}
				if tup == nil {
					break
				} else {
					fmt.Printf("\033[32m%s\033[0m\n", tup.PrettyPrintString(aligned))
				}
				nresults++
				select {
				case <-alarm:
					fmt.Println("Aborting")
					goto outer
				default:
				}
			}
			if autocommit {
				bp.CommitTransaction(tid)
			}
		outer:
			fmt.Printf("\033[32;1m(%d results)\033[0m\n", nresults)
			duration := time.Since(start)
			fmt.Printf("\033[32;1m%v\033[0m\n\n", duration)

		case godb.BeginXactionType:
			if !autocommit {
				fmt.Printf("\033[31;1m%s\033[0m\n", "Cannot start transaction while in transaction")
				continue
			}
			tid = godb.NewTID()
			err := bp.BeginTransaction(tid)
			if err != nil {
				fmt.Printf("\033[31;1m%s\033[0m\n", err.Error())
				continue
			}
			autocommit = false
			fmt.Printf("\033[32;1mBEGIN\033[0m\n\n")

		case godb.AbortXactionType:
			if autocommit {
				fmt.Printf("\033[31;1m%s\033[0m\n", "Cannot abort transaction unless in transaction")
				continue
			}
			bp.AbortTransaction(tid)
			autocommit = true
			fmt.Printf("\033[32;1mABORT\033[0m\n\n")
		case godb.CommitXactionType:
			if autocommit {
				fmt.Printf("\033[31;1m%s\033[0m\n", "Cannot commit transaction unless in transaction")
				continue
			}
			bp.CommitTransaction(tid)
			autocommit = true
			fmt.Printf("\033[32;1mCOMMIT\033[0m\n\n")
		case godb.CreateTableQueryType:
			fmt.Printf("\033[32;1mCREATE\033[0m\n\n")
			err := c.SaveToFile(catName, catPath)
			if err != nil {
				fmt.Printf("\033[31;1m%s\033[0m\n", err.Error())
			}
		case godb.DropTableQueryType:
			fmt.Printf("\033[32;1mDROP\033[0m\n\n")
			err := c.SaveToFile(catName, catPath)
			if err != nil {
				fmt.Printf("\033[31;1m%s\033[0m\n", err.Error())
			}
		}
	}
}
