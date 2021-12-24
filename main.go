package main

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"
	"time"
)

func main() {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/csv/column/", extractColumns) // "/" 结尾能匹配子路由

	log.Fatal(http.ListenAndServe(":12071", mux))
}

func extractColumns(w http.ResponseWriter, req *http.Request) {
	f, fh, err := req.FormFile("oneCSVzipFile")
	if err != nil {
		log.Println("read form file error:", err)
		jsonReply(w, http.StatusBadRequest, err.Error())
		return
	}
	defer f.Close()
	fn := fh.Filename
	if !strings.HasSuffix(fn, ".zip") {
		jsonReply(w, http.StatusBadRequest, "should be a .zip file")
		return
	}
	columnNumber, err := strconv.Atoi(path.Base(req.URL.Path))
	if err != nil {
		log.Println("column number must be an integer")
		jsonReply(w, http.StatusBadRequest, err.Error())
		return
	}
	ctx, cancel := context.WithTimeout(req.Context(), 30*time.Second)
	ctx = context.WithValue(ctx, columnNumberKey, columnNumber)
	defer cancel()

	result, err := processZipFile(ctx, f, fh)
	if err != nil {
		jsonReply(w, http.StatusBadRequest, err.Error())
		return
	}
	buf, err := saveAsCSV(result)
	if err != nil {
		jsonReply(w, http.StatusInternalServerError, err.Error())
		return
	}
	// 聚合结果得到的 csv 以 .zip 的名字来命名
	csvReply(w, fn, buf)
}

func processZipFile(ctx context.Context, mf multipart.File, mfh *multipart.FileHeader) ([][]string, error) {
	zipfn := mfh.Filename
	tmpfn := fmt.Sprintf("./tmp/%s", zipfn)
	// https://astaxie.gitbooks.io/build-web-application-with-golang/content/en/04.5.html
	f, err := os.OpenFile(tmpfn, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		log.Println("open file error:", err)
		return nil, err
	}
	// 删除暂存的 zip 文件
	defer func() {
		if err := f.Close(); err != nil {
			log.Println(err)
		}
		if err := os.Remove(tmpfn); err != nil {
			log.Println(err)
		}
	}()

	_, err = io.Copy(f, mf)
	if err != nil {
		return nil, err
	}

	zr, err := zip.OpenReader(tmpfn)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := zr.Close(); err != nil {
			log.Println(err)
		}
	}()
	return processZipContent(ctx, zipfn, zr)
}

func processZipContent(ctx context.Context, resultfn string, rc *zip.ReadCloser) ([][]string, error) {
	if len(rc.File) == 0 {
		return nil, fmt.Errorf("no csv file in zip file")
	}

	// 获取 zip 文件的后缀，预设所有文件的后缀都相同
	for _, f := range rc.File { // 包括目录
		if f.FileInfo().IsDir() {
			continue
		}
		switch {
		case strings.HasSuffix(f.Name, ".csv"):
			return processCSVs(ctx, rc.File)
		default:
			return nil, fmt.Errorf("unsupported file type")
		}

	}
	return nil, fmt.Errorf("should not have executed")
}

func processCSVs(ctx context.Context, zfs []*zip.File) ([][]string, error) {
	if len(zfs) == 0 {
		return nil, nil
	}
	targetColumnNumber, ok := ctx.Value(columnNumberKey).(int)
	if !ok {
		return nil, fmt.Errorf("column nubmer must be an integer")
	}
	var targetColumnTitle string
	var csvfns []string
	var rawData [][]string // 需要转置
	for _, f := range zfs {
		if f.FileInfo().IsDir() {
			continue
		}
		csvfns = append(csvfns, f.Name)
		rc, err := f.Open()
		if err != nil {
			return nil, err
		}
		csvrd := csv.NewReader(rc)
		csvData, err := csvrd.ReadAll()
		if err != nil {
			rc.Close()
			return nil, err
		}
		if len(csvfns) == 1 && len(csvData) > 0 {
			if len(csvData[0]) < targetColumnNumber {
				return nil, fmt.Errorf("requested column number if bigger than the csv's maximun column number")
			}
			targetColumnTitle = csvData[0][targetColumnNumber-1]
		}
		rawData = append(rawData, gatherColumn(csvData, targetColumnNumber-1))

		rc.Close()
	}
	rawData = transpose(rawData)
	var fr [][]string
	fr = append(fr, []string{fmt.Sprintf("Aggregated results for column %q:", targetColumnTitle)})
	fr = append(fr, csvfns)
	fr = append(fr, rawData...)

	return fr, nil
}

func gatherColumn(table [][]string, columnNumber int) []string {
	var r []string
	for _, row := range table {
		if !isNumber(row[columnNumber]) { // 只返回数值
			continue
		}
		r = append(r, row[columnNumber])
	}
	return r
}

func isNumber(s string) bool {
	if _, err := strconv.ParseInt(s, 10, 64); err == nil {
		return true
	}
	if _, err := strconv.ParseFloat(s, 64); err == nil {
		return true
	}
	return false
}

func transpose(d [][]string) [][]string {
	res := make([][]string, len(d[0])) // dd 的列是 d 的行
	for i := 0; i < len(d[0]); i++ {
		res[i] = make([]string, len(d))
		for j := 0; j < len(d); j++ {
			res[i][j] = d[j][i]
		}
	}
	return res
}

func saveAsCSV(data [][]string) (*bytes.Buffer, error) {
	var buf bytes.Buffer
	wr := csv.NewWriter(&buf)
	if err := wr.WriteAll(data); err != nil {
		return nil, err
	}
	return &buf, nil
}

type Response struct {
	StatusCode int    `json:"status_code`
	Message    string `json:"message"`
}

func jsonReply(w http.ResponseWriter, sc int, msg string) error {
	w.Header().Set("Content-Type", "application/json")
	// w.Write([]byte(""))
	return json.NewEncoder(w).Encode(Response{
		StatusCode: sc,
		Message:    msg,
	})
}

func csvReply(w http.ResponseWriter, fn string, buf *bytes.Buffer) error {
	w.Header().Set("Content-Type", "text/csv")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment;filename=%s.csv", fn))
	_, err := w.Write(buf.Bytes())
	return err
}

type key int

const columnNumberKey key = 0

/*
curl -X POST \
  http://127.0.0.1:12071/api/csv/column/2 \
  -H 'Content-Type: multipart/form-data' \
  -F oneCSVzipFile=@data.zip \
  -o data.csv
*/

/*
TODO: 日志模块，客户端身份校验，支持递归处理，矩阵转置优化，支持并发处理，单元测试

context
检查文件关闭
*/
