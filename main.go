package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
)

func main() {
	files, err := ioutil.ReadDir("./")
	if err != nil {
		panic(err)
	}
	var csvs []string
	for _, f := range files {
		if strings.HasSuffix(f.Name(), ".csv") {
			csvs = append(csvs, f.Name())
		}
	}

	var rf [][]string
	// rf = append(rf, csvs)

	for _, c := range csvs {
		var ff []string
		f, err := os.Open(c)
		if err != nil {
			panic(err)
		}
		cr := csv.NewReader(bufio.NewReader(f))
		records, err := cr.ReadAll()
		if err != nil {
			panic(err)
		}
		for j := range records {
			if records[j][1] != "Inters." {
				// fmt.Printf("%s\t", records[j][1])
				ff = append(ff, records[j][1])
			}
		}
		rf = append(rf, ff)
	}
	// fmt.Println(len(rf), len(rf[0]))

	// for i := len(rf); i < len(rf[0]); i++ {
	// 	pl := make([]string, 20)
	// 	rf = append(rf, pl)
	// }
	// fmt.Println(len(rf), len(rf[0]))

	// for i := 0; i < len(rf)/2; i++ {
	// 	for j := 0; j < i; j++ {
	// 		rf[i][j], rf[j][i] = rf[j][i], rf[i][j]
	// 	}
	// }

	nrf := make([][]string, len(rf[0]))
	for i := 0; i < len(rf[0]); i++ {
		nrf[i] = make([]string, len(rf))
		for j := 0; j < len(rf); j++ {
			nrf[i][j] = rf[j][i]
		}
	}
	var nnrf [][]string
	nnrf = append(nnrf, csvs)
	nnrf = append(nnrf, nrf...)

	for i := range nnrf {
		for j := range nnrf[i] {
			fmt.Printf("%s\t", nnrf[i][j])
		}
		fmt.Println()
	}

}
