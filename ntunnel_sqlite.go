// Copyright (c) 2026 ntunnel_sqlite contributors
// SPDX-License-Identifier: MIT

// ntunnel_sqlite.go 是 Navicat 的 ntunnel_sqlite.php HTTP 隧道脚本的 Go 移植版本。
//
// 作用：Navicat 通过 HTTP POST 把操作发到本服务（form 字段 actn/dbfile/q 等），
// 本服务操作服务器上的 SQLite 文件，并按下面的二进制格式把结果写回响应。
//
// 二进制格式与 PHP 脚本逐字节一致，所有整数均为大端序：
//
//	header        : long 1111 | short versionNum | long errno | 6 字节 0
//	connection    : block(libversion) 重复 3 次
//	result set    : long errno | long affectedrows | long insertid |
//	                long numfields | long numrows | 12 字节 0
//	fields        : block(name) | block("") | long type | long flag | long length
//	data          : 每个值: (NULL 时为 0xFF，否则 block(value)) | long columnType
//	row separator : 结果集之间写 0x01，最后一个结果集写 0x00
//
// block 的编码：长度 < 254 时用 1 字节长度；否则写 0xFE + 4 字节长度，后面紧跟
// 原始字节。type 取值与 SQLite 存储类型一致：1=INTEGER、2=FLOAT、3=TEXT、
// 4=BLOB、5=NULL。
//
// 请求动作 actn：
//
//	C  连接测试（返回 SQLite 版本）
//	Q  执行一条或多条查询（查询内容放在 q/q[] 字段里）
//	3  新建数据库文件（dbfile 必须不存在）
//	2  新建 SQLite2 数据库（已不支持，固定返回 203）
package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"html"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	_ "modernc.org/sqlite"
)

// tunnelVersion 是隧道脚本版本号，Navicat 会把它和对端期望的版本比较。
// 与 PHP 脚本的 $versionNum 保持一致（204），修改前请确认客户端要求。
const tunnelVersion = 204

// 数据库文件头，用来判断文件是 SQLite2、SQLite3 还是别的格式。
const (
	sqlite2Signature = "** This file contains an SQLite 2.1 database **"
	sqlite3Signature = "SQLite format 3"
)

// detectFileStatus 的返回值，与 PHP 脚本里的 FileDoesNotExist 等常量一一对应。
const (
	fileDoesNotExist   = iota // 文件不存在（或不是普通文件）
	fileCannotBeOpened        // 文件存在但没有读取权限
	fileIsSQLite2             // SQLite 2 格式（本程序不支持）
	fileIsSQLite3             // SQLite 3 格式
	fileIsInvalid             // 加密或非法文件
)

// SQLite 存储类型，与 PHP 的 SQLITE3_XXX 常量一致（SQLite3Result::columnType）。
const (
	typeInteger = 1 // SQLITE3_INTEGER
	typeFloat   = 2 // SQLITE3_FLOAT
	typeText    = 3 // SQLITE3_TEXT
	typeBlob    = 4 // SQLITE3_BLOB
	typeNull    = 5 // SQLITE3_NULL
)

var (
	debugLog   bool   // -debug 参数：是否输出 SQL 全文、响应 hex 和样本行
	requestSeq uint64 // 请求自增编号，日志前缀 [#N] 使用
)

func main() {
	// 监听地址与调试开关都可以通过命令行参数修改。
	addr := flag.String("addr", ":8080", "HTTP listen address")
	flag.BoolVar(&debugLog, "debug", false, "log full SQL text and raw response bytes")
	flag.Parse()

	log.Printf("Navicat SQLite HTTP tunnel listening on %s", *addr)
	log.Fatal(http.ListenAndServe(*addr, http.HandlerFunc(handle)))
}

// ---------------------------------------------------------------- 二进制协议辅助

// getLongBinary 输出 4 字节大端无符号整数（对应 PHP 的 pack("N", $num)）。
func getLongBinary(num uint32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, num)
	return b
}

// getShortBinary 输出 2 字节大端无符号整数（对应 PHP 的 pack("n", $num)）。
func getShortBinary(num uint16) []byte {
	b := make([]byte, 2)
	binary.BigEndian.PutUint16(b, num)
	return b
}

// getDummy 输出 count 个 0x00，用于协议中的保留字段。
func getDummy(count int) []byte {
	return make([]byte, count)
}

// getBlock 编码一个带长度前缀的字节块：
//
//	长度 < 254 : 1 字节长度 + 数据
//	长度 >= 254: 0xFE + 4 字节大端长度 + 数据
func getBlock(val []byte) []byte {
	var b bytes.Buffer
	if len(val) < 254 {
		b.WriteByte(byte(len(val)))
	} else {
		b.WriteByte(0xFE)
		b.Write(getLongBinary(uint32(len(val))))
	}
	b.Write(val)
	return b.Bytes()
}

// getBlockString 是 getBlock 的字符串版；长度按字节数计算（中文等多字节字符也正确）。
func getBlockString(val string) []byte {
	return getBlock([]byte(val))
}

// echoHeader 写隧道响应头：魔数 1111、脚本版本号、错误码、6 字节保留位。
// errno 为 0 表示成功；非 0 时后面会紧跟一个错误消息块（见 echoError）。
func echoHeader(w io.Writer, errno int) {
	var b bytes.Buffer
	b.Write(getLongBinary(1111))
	b.Write(getShortBinary(uint16(tunnelVersion)))
	b.Write(getLongBinary(uint32(errno)))
	b.Write(getDummy(6))
	w.Write(b.Bytes())
}

// echoConnInfo 写连接信息。PHP 脚本里把库版本号重复写了三次，这里保持一致。
func echoConnInfo(w io.Writer, version string) {
	var b bytes.Buffer
	b.Write(getBlockString(version))
	b.Write(getBlockString(version))
	b.Write(getBlockString(version))
	w.Write(b.Bytes())
}

// echoResultSetHeader 写单个结果集的头：
// 错误码、影响行数、自增 ID、字段数、行数，最后 12 字节保留位。
// Navicat 依赖这里的字段数/行数来分配表格，所以必须准确。
func echoResultSetHeader(w io.Writer, errno, affectRows, insertID, numFields, numRows int64) {
	var b bytes.Buffer
	b.Write(getLongBinary(uint32(errno)))
	b.Write(getLongBinary(uint32(affectRows)))
	b.Write(getLongBinary(uint32(insertID)))
	b.Write(getLongBinary(uint32(numFields)))
	b.Write(getLongBinary(uint32(numRows)))
	b.Write(getDummy(12))
	w.Write(b.Bytes())
}

// echoFieldsHeader 写字段元数据。每个字段依次是：
// 字段名块、空串块（原表名，SQLite 脚本里固定为空）、类型、标志位、长度。
// 类型固定写 typeNull(5)，与 PHP 脚本的 SQLITE3_NULL 行为一致。
func echoFieldsHeader(w io.Writer, columns []string) {
	var b bytes.Buffer
	for _, name := range columns {
		b.Write(getBlockString(name))
		b.Write(getBlockString(""))
		b.Write(getLongBinary(typeNull)) // 与 PHP 脚本一致：字段头类型恒为 SQLITE3_NULL
		b.Write(getLongBinary(0))
		b.Write(getLongBinary(0))
	}
	w.Write(b.Bytes())
}

// cell 是结果集中的一个值：原始字节、协议类型以及是否为 NULL。
type cell struct {
	value  []byte
	typ    uint32
	isNull bool
}

// echoData 按行写结果数据。每个值的格式是：
//
//	NULL    : 单字节 0xFF + 4 字节类型(5)
//	非 NULL : 值块 + 4 字节类型
//
// 这与 PHP 的 EchoData3() 一致。
func echoData(w io.Writer, rows [][]cell) {
	var b bytes.Buffer
	for _, row := range rows {
		for _, c := range row {
			if c.isNull {
				b.WriteByte(0xFF)
			} else {
				b.Write(getBlock(c.value))
			}
			b.Write(getLongBinary(c.typ))
		}
	}
	w.Write(b.Bytes())
}

// echoError 写错误响应：带 errno 的头部 + 一个错误消息块。
func echoError(w io.Writer, errno int, message string) {
	echoHeader(w, errno)
	w.Write(getBlockString(message))
}

// ---------------------------------------------------------------- 数据库文件检查

// detectFileStatus 通过文件头判断 dbfile 的类型，结果与 PHP 脚本完全对应：
//   - 读前 46 字节（SQLite2 头长度）
//   - 以 SQLite2 头开头 -> fileIsSQLite2
//   - 空文件或以 "SQLite format 3" 开头 -> fileIsSQLite3（空文件允许后续初始化）
//   - 其他 -> fileIsInvalid（可能被加密或不是数据库）
//
// 注意：这里只做粗略判断，真正的打开/校验在 serveSQLite3 里由 SQLite 驱动完成。
func detectFileStatus(path string) int {
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() {
		return fileDoesNotExist
	}

	f, err := os.Open(path)
	if err != nil {
		return fileCannotBeOpened
	}
	defer f.Close()

	// 文件可能比 sqlite2Signature 短，ReadFull 返回实际读到的字节数。
	buf := make([]byte, len(sqlite2Signature))
	n, _ := io.ReadFull(f, buf)
	buf = buf[:n]

	switch {
	case bytes.HasPrefix(buf, []byte(sqlite2Signature)):
		return fileIsSQLite2
	case len(buf) == 0 || bytes.HasPrefix(buf, []byte(sqlite3Signature)):
		return fileIsSQLite3
	default:
		return fileIsInvalid
	}
}

// ---------------------------------------------------------------- 请求处理

// statusName 把文件状态翻译成日志里可读的文字。
func statusName(status int) string {
	switch status {
	case fileDoesNotExist:
		return "does not exist"
	case fileCannotBeOpened:
		return "cannot be opened"
	case fileIsSQLite2:
		return "SQLite 2 file"
	case fileIsSQLite3:
		return "SQLite 3 file"
	case fileIsInvalid:
		return "invalid or encrypted"
	default:
		return "unknown"
	}
}

// sqlForLog 截断过长的 SQL（批量插入等），避免日志刷屏；-debug 时输出全文。
func sqlForLog(query string) string {
	if debugLog || len(query) <= 400 {
		return query
	}
	return query[:400] + fmt.Sprintf(" ...(%d bytes total)", len(query))
}

// formQueries 按 PHP 的 $_POST["q"] 语义取出查询列表。
//
// 关键点：Navicat 用 multipart/form-data 提交，字段名可能是 "q"、也可能写成
// PHP 数组风格 "q[]"、"q[0]"、"q[1]"。PHP 会自动把后者收拢成数组，而 Go 的
// url.Values 会保留原始字段名，所以这里手动兼容并按索引顺序拼接。
func formQueries(form url.Values) []string {
	// 最常见的形式：重复的 q 字段。
	if values, ok := form["q"]; ok {
		return append([]string(nil), values...)
	}

	// 兼容 q[] / q[N] 写法。
	type queryField struct {
		index int    // q[N] 里的 N；q[] 记为 len(form)，排在编号项之后
		key   string // 原始字段名
	}
	var fields []queryField
	for key := range form {
		if !strings.HasPrefix(key, "q[") || !strings.HasSuffix(key, "]") {
			continue
		}
		index := len(form)
		if inner := key[2 : len(key)-1]; inner != "" {
			if n, err := strconv.Atoi(inner); err == nil {
				index = n
			}
		}
		fields = append(fields, queryField{index: index, key: key})
	}
	if len(fields) == 0 {
		return nil
	}

	// 按数组下标排序，保证查询执行顺序与客户端发送顺序一致。
	sort.Slice(fields, func(i, j int) bool {
		if fields[i].index != fields[j].index {
			return fields[i].index < fields[j].index
		}
		return fields[i].key < fields[j].key
	})

	var queries []string
	for _, field := range fields {
		queries = append(queries, form[field.key]...)
	}
	return queries
}

// handle 处理一次 HTTP 请求，是隧道的主入口：
//
//  1. 建立带 [#N] 编号的日志
//  2. 响应先写进内存缓冲区（便于计算 Content-Length、打印 hex）
//  3. 非 POST 或缺少 actn/dbfile 时返回测试页（与 PHP 的测试菜单一致）
//  4. 校验参数，必要时对 q 做 base64 解码
//  5. 判断 dbfile 类型，按 actn 分发到 C / Q / 3
func handle(w http.ResponseWriter, r *http.Request) {
	reqID := atomic.AddUint64(&requestSeq, 1)
	logger := log.New(os.Stderr, fmt.Sprintf("[#%d] ", reqID), log.LstdFlags)
	start := time.Now()

	// 响应统一先写进 out，最后一次性发出：这样能拿到准确长度，
	// 也能在日志里记录响应内容（与 PHP 的输出缓冲效果类似）。
	var out bytes.Buffer
	contentType := "text/plain; charset=x-user-defined"

	// 收尾工作：恢复 panic、设置响应头、记录日志、写出响应。
	defer func() {
		if rec := recover(); rec != nil {
			logger.Printf("panic while handling request: %v\n%s", rec, debug.Stack())
			if out.Len() == 0 {
				// 还没写任何内容时，尽量返回一个合法的错误响应。
				echoError(&out, 202, fmt.Sprintf("internal error: %v", rec))
			}
		}

		w.Header().Set("Content-Type", contentType)
		w.Header().Set("Content-Length", strconv.Itoa(out.Len()))
		logger.Printf("response: %d bytes in %s", out.Len(), time.Since(start).Round(time.Microsecond))
		if debugLog && out.Len() > 0 {
			head := out.Bytes()
			if len(head) > 256 {
				head = head[:256]
			}
			logger.Printf("response head (hex): %s", hex.EncodeToString(head))
		}
		if _, err := w.Write(out.Bytes()); err != nil {
			logger.Printf("write response: %v", err)
		}
	}()

	logger.Printf("%s %s from %s content-type=%q body=%d bytes",
		r.Method, r.URL.RequestURI(), r.RemoteAddr, r.Header.Get("Content-Type"), r.ContentLength)

	// 只有 POST 才是隧道请求；其余请求（例如用浏览器打开）返回测试页。
	if r.Method != http.MethodPost {
		logger.Printf("no POST body, serving test page")
		contentType = "text/html"
		writeTestPage(&out)
		return
	}

	// 同时支持 application/x-www-form-urlencoded 和 multipart/form-data。
	if err := r.ParseMultipartForm(64 << 20); err != nil && !errors.Is(err, http.ErrNotMultipart) {
		logger.Printf("parse form: %v", err)
	}

	// PHP 脚本在缺少参数时显示测试页，这里照做。
	if !r.PostForm.Has("actn") || !r.PostForm.Has("dbfile") {
		logger.Printf("missing actn/dbfile, serving test page")
		contentType = "text/html"
		writeTestPage(&out)
		return
	}

	// 记录客户端实际发送的字段名（便于排查 q 与 q[] 之类的差异）。
	formFields := make([]string, 0, len(r.PostForm))
	for key := range r.PostForm {
		formFields = append(formFields, fmt.Sprintf("%s=%d", key, len(r.PostForm[key])))
	}
	sort.Strings(formFields)
	logger.Printf("form fields: %v", formFields)

	action := r.PostForm.Get("actn")
	queries := formQueries(r.PostForm)
	logger.Printf("action=%q dbfile=%q version=%q encodeBase64=%q queries=%d",
		action, r.PostForm.Get("dbfile"), r.PostForm.Get("version"),
		r.PostForm.Get("encodeBase64"), len(queries))

	// 统一的"返回错误"出口：记录日志并写 errno + 消息块。
	fail := func(errno int, message string) {
		logger.Printf("returning error %d: %s", errno, message)
		echoError(&out, errno, message)
	}

	// 参数校验，与 PHP 脚本保持一致：
	// 新建库（2/3）必须带 version；执行查询（Q）至少要有一条 q。
	if (action == "2" || action == "3") && !r.PostForm.Has("version") {
		fail(202, "invalid parameters")
		return
	}
	if action == "Q" && len(queries) == 0 {
		fail(202, "invalid parameters")
		return
	}

	// 客户端勾选"用 base64 编码查询"时，先把每条 SQL 解码。
	if r.PostForm.Get("encodeBase64") == "1" {
		for i, q := range queries {
			decoded, err := base64.StdEncoding.DecodeString(q)
			if err != nil {
				decoded, err = base64.RawStdEncoding.DecodeString(q)
			}
			if err != nil {
				logger.Printf("query #%d: base64 decode failed: %v", i+1, err)
				continue
			}
			logger.Printf("query #%d: decoded %d bytes from %d base64 chars", i+1, len(decoded), len(q))
			queries[i] = string(decoded)
		}
	}

	file := r.PostForm.Get("dbfile")
	status := detectFileStatus(file)
	logger.Printf("database file %q detected as %s", file, statusName(status))

	// 新建数据库：只允许 dbfile 不存在；SQLite2 已不支持。
	if action == "2" || action == "3" {
		if status != fileDoesNotExist {
			fail(202, "Datbase file exists already") // 原脚本的拼写，保持原样
			return
		}
		if action == "2" {
			fail(203, "SQLite2 is not supported on the server")
			return
		}
		serveSQLite3(&out, logger, action, file, queries)
		return
	}

	// 普通操作（连接测试 C / 查询 Q）：按文件类型决定能否继续。
	switch status {
	case fileDoesNotExist:
		fail(202, "Database file does not exist")
	case fileCannotBeOpened:
		fail(202, "Database file cannot be opened")
	case fileIsSQLite2:
		fail(203, "SQLite2 is not supported on the server")
	case fileIsSQLite3:
		serveSQLite3(&out, logger, action, file, queries)
	case fileIsInvalid:
		fail(202, "Database file is encrypted or invalid")
	}
}

// serveSQLite3 打开 dbfile 并处理 action：
//
//	C  连接测试：返回 SQLite 版本号
//	3  新建数据库：执行 VACUUM 让空文件变成合法的库，再返回版本号
//	Q  依次执行 queries，每条查询输出一个结果集
//
// 这里只使用一个 *sql.Conn（单连接），这样 changes()、last_insert_rowid()
// 以及事务类语句（BEGIN/COMMIT）在整批查询中保持与 PHP 脚本相同的语义。
func serveSQLite3(w *bytes.Buffer, logger *log.Logger, action, path string, queries []string) {
	ctx := context.Background()

	logger.Printf("opening database %q (action=%q)", path, action)
	// 驱动名固定为 "sqlite"：modernc.org/sqlite 是纯 Go 实现，不需要 cgo。
	db, err := sql.Open("sqlite", path)
	if err != nil {
		logger.Printf("sql.Open failed: %v", err)
		echoError(w, 202, err.Error())
		return
	}
	defer db.Close()

	conn, err := db.Conn(ctx)
	if err != nil {
		logger.Printf("open connection to %q failed: %v", path, err)
		echoError(w, 202, err.Error())
		return
	}
	defer conn.Close()

	switch action {
	case "C":
		// 连接测试：读取库版本，然后按协议写 3 份版本号。
		version, err := sqliteVersion(ctx, conn)
		if err != nil {
			logger.Printf("reading sqlite_version() failed: %v", err)
			echoError(w, 202, err.Error())
			return
		}
		logger.Printf("connection ok, sqlite %s", version)
		echoHeader(w, 0)
		echoConnInfo(w, version)
	case "3":
		// 新建库：空文件还不算数据库，VACUUM 会写入合法的文件头。
		if _, err := conn.ExecContext(ctx, "VACUUM"); err != nil {
			logger.Printf("VACUUM on new database failed: %v", err)
		} else {
			logger.Printf("new database created")
		}
		version, err := sqliteVersion(ctx, conn)
		if err != nil {
			logger.Printf("reading sqlite_version() failed: %v", err)
			echoError(w, 202, err.Error())
			return
		}
		echoHeader(w, 0)
		echoConnInfo(w, version)
	case "Q":
		// 批量查询：先写公共头，之后每条查询各写一个结果集。
		echoHeader(w, 0)
		runQueries(ctx, w, logger, conn, queries)
	default:
		// 未知动作：与 PHP 一样只回一个空头。
		logger.Printf("unknown action %q, returning empty header", action)
		echoHeader(w, 0)
	}
}

// sqliteVersion 查询 SQLite 库版本号（例如 "3.54.1"）。
func sqliteVersion(ctx context.Context, conn *sql.Conn) (string, error) {
	var version string
	if err := conn.QueryRowContext(ctx, "SELECT sqlite_version()").Scan(&version); err != nil {
		return "", err
	}
	return version, nil
}

// runQueries 依次执行每条查询，并为每条查询输出一个结果集：
//
//	结果集头 + (错误消息 | 字段头 + 数据) + 分隔符
//
// 分隔符：后面还有查询时写 0x01，最后一条写 0x00（与 PHP 脚本一致）。
// 注意：空查询会被跳过（PHP 的 continue），因此建议客户端不要发空语句。
func runQueries(ctx context.Context, w *bytes.Buffer, logger *log.Logger, conn *sql.Conn, queries []string) {
	for i, query := range queries {
		if query == "" {
			logger.Printf("query #%d: skipped (empty)", i+1)
			continue
		}

		logger.Printf("query #%d: %s", i+1, sqlForLog(query))
		queryStart := time.Now()
		startLen := w.Len() // 用于统计本条结果集写了多少字节

		errno := 0
		errMsg := ""
		var columns []string
		var data [][]cell

		// 执行查询。结果集头里要同时给出字段数和行数，所以这里先把所有行
		// 读进内存（PHP 脚本也是先 fetch 一遍计数再 reset）。
		rows, err := conn.QueryContext(ctx, query)
		if err != nil {
			errno = sqliteErrCode(err)
			errMsg = err.Error()
		} else {
			columns, err = rows.Columns()
			if err != nil {
				errno = sqliteErrCode(err)
				errMsg = err.Error()
			} else if len(columns) > 0 {
				data, err = readRows(rows, len(columns))
				if err != nil {
					errno = sqliteErrCode(err)
					errMsg = err.Error()
				}
			}
			rows.Close()
		}

		// 取影响行数和最后插入 ID（对应 PHP 的 changes()/lastInsertRowID()）。
		// 即使上面的语句失败也照样读取，保持与 PHP 相同的"沿用上一次值"行为。
		affectedRows, insertID := queryChanges(ctx, conn)

		logger.Printf("query #%d done in %s: errno=%d affected=%d insertid=%d fields=%d rows=%d",
			i+1, time.Since(queryStart).Round(time.Microsecond), errno, affectedRows, insertID,
			len(columns), len(data))
		if errno != 0 {
			logger.Printf("query #%d error: %s", i+1, errMsg)
		} else if len(columns) > 0 {
			logger.Printf("query #%d columns: %q", i+1, columns)
		}

		// 1) 结果集头：errno/影响行数/插入ID/字段数/行数。
		echoResultSetHeader(w, int64(errno), affectedRows, insertID, int64(len(columns)), int64(len(data)))

		// 2) 内容：出错时只写错误消息；有字段时写字段头 + 数据；否则写空块。
		if errno != 0 {
			w.Write(getBlockString(errMsg))
		} else if len(columns) > 0 {
			echoFieldsHeader(w, columns)
			echoData(w, data)
		} else {
			w.Write(getBlockString(""))
		}

		// 3) 分隔符。
		if i < len(queries)-1 {
			w.Write([]byte{0x01})
		} else {
			w.Write([]byte{0x00})
		}

		logger.Printf("query #%d payload: %d bytes (total response %d bytes)", i+1, w.Len()-startLen, w.Len())
		// -debug 时打印前 3 行样本，便于核对返回值与类型。
		if debugLog {
			for r := 0; r < len(data) && r < 3; r++ {
				values := make([]string, len(data[r]))
				for c, cell := range data[r] {
					if cell.isNull {
						values[c] = "NULL"
						continue
					}
					text := string(cell.value)
					if len(text) > 120 {
						text = text[:120] + "..."
					}
					values[c] = fmt.Sprintf("type=%d %q", cell.typ, text)
				}
				logger.Printf("query #%d row #%d: %v", i+1, r+1, values)
			}
		}
	}
}

// readRows 把 sql.Rows 全部读出来，转成协议需要的 cell 结构。
func readRows(rows *sql.Rows, numFields int) ([][]cell, error) {
	data := make([][]cell, 0, 16)
	values := make([]any, numFields)
	ptrs := make([]any, numFields)
	for i := range values {
		ptrs[i] = &values[i]
	}

	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			return data, err
		}
		// 把每列的值转换成 cell（确定 NULL 与协议类型）。
		row := make([]cell, numFields)
		for i, v := range values {
			row[i] = makeCell(v)
		}
		data = append(data, row)
	}
	return data, rows.Err()
}

// makeCell 把驱动返回的 Go 值转换成协议里的"值 + 类型"：
// nil->NULL(5)、int64->INTEGER(1)、float64->FLOAT(2)、string/时间->TEXT(3)、
// []byte->BLOB(4)。
func makeCell(v any) cell {
	switch x := v.(type) {
	case nil:
		return cell{typ: typeNull, isNull: true}
	case int64:
		return cell{typ: typeInteger, value: []byte(strconv.FormatInt(x, 10))}
	case float64:
		return cell{typ: typeFloat, value: []byte(strconv.FormatFloat(x, 'g', -1, 64))}
	case bool:
		if x {
			return cell{typ: typeInteger, value: []byte("1")}
		}
		return cell{typ: typeInteger, value: []byte("0")}
	case string:
		return cell{typ: typeText, value: []byte(x)}
	case []byte:
		return cell{typ: typeBlob, value: x}
	case time.Time:
		// 纯 Go 驱动遇到 DATE/DATETIME 列可能返回 time.Time，这里转成文本。
		return cell{typ: typeText, value: []byte(x.Format("2006-01-02 15:04:05.999999999-07:00"))}
	default:
		return cell{typ: typeText, value: []byte(fmt.Sprint(x))}
	}
}

// queryChanges 读取当前连接的影响行数与最后插入 rowid。
// 用单独的 SELECT 获取，等价于 PHP 的 SQLite3::changes()/lastInsertRowID()，
// 并且不会重置这两个值（SQLite 只在执行 DML 时更新它们）。
func queryChanges(ctx context.Context, conn *sql.Conn) (int64, int64) {
	var affectedRows, insertID int64
	if err := conn.QueryRowContext(ctx, "SELECT changes(), last_insert_rowid()").Scan(&affectedRows, &insertID); err != nil {
		return 0, 0
	}
	return affectedRows, insertID
}

// sqliteErrCode 提取 SQLite 错误码（modernc 驱动的 Error 实现了 Code() int）。
// 取不到时退回 1（SQLITE_ERROR），保证客户端能识别为失败。
func sqliteErrCode(err error) int {
	var coder interface{ Code() int }
	if errors.As(err, &coder) {
		return coder.Code()
	}
	return 1
}

// ---------------------------------------------------------------- 测试页

// writeTestPage 输出原 PHP 脚本里的 "Navicat HTTP Tunnel Tester" 页面：
// 上半部分是服务端环境自检，下半部分是浏览器端的连接测试（JS 会 POST actn=C）。
func writeTestPage(w io.Writer) {
	// 用内存库取一下 SQLite 版本，用于显示环境信息。
	sqliteVersion := ""
	if db, err := sql.Open("sqlite", ":memory:"); err == nil {
		defer db.Close()
		_ = db.QueryRow("SELECT sqlite_version()").Scan(&sqliteVersion)
	}

	type testRow struct {
		desc string
		succ bool
		text string
	}
	rows := []testRow{
		{"[SQLite2] SQLite 2 database support", false, "No"},
		{"[SQLite3] SQLite3 library available", sqliteVersion != "", "Yes (" + sqliteVersion + ")"},
		{"Go runtime version", true, runtime.Version()},
		{"Current tunnel file version", true, fmt.Sprintf("%d.%d", tunnelVersion/100, tunnelVersion%100)},
	}

	// 页头 + 表格行 + 页尾，拼接成完整 HTML。
	var b strings.Builder
	b.WriteString(testPageHead)
	for _, row := range rows {
		class, text := "TestFail", row.text
		if row.succ {
			class = "TestSucc"
		}
		if text == "" {
			text = "No"
		}
		b.WriteString(`		<tr><td class="TestDesc">` + html.EscapeString(row.desc) +
			`</td><td class="` + class + `">` + html.EscapeString(text) + "</td></tr>\n")
	}
	b.WriteString(testPageTail)

	w.Write([]byte(b.String()))
}

// testPageHead 是测试页的静态前半部分（样式 + JS + 系统环境表格开头）。
const testPageHead = `<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
	<title>SQLite HTTP Tunnel Tester</title>
	<meta http-equiv="Content-Type" content="text/html; charset=ISO-8859-1">
	<style type="text/css">
		body{
			margin: 30px;
			font-family: Tahoma;
			font-weight: normal;
			font-size: 14px;
			color: #222222;
		}
		table{
			width: 100%;
			border: 0px;
		}
		input{
			font-family:Tahoma,sans-serif;
			border-style:solid;
			border-color:#666666;
			border-width:1px;
		}
		fieldset{
			border-style:solid;
			border-color:#666666;
			border-width:1px;
		}
		.Title1{
			font-size: 30px;
			color: #003366;
		}
		.Title2{
			font-size: 10px;
			color: #999966;
		}
		.TestDesc{
			width:70%
		}
		.TestSucc{
			color: #00BB00;
		}
		.TestFail{
			color: #DD0000;
		}
		.mysql{
			display:none;
		}
		.pgsql{
			display:none;
		}
		.sqlite{
		}
		#page{
			max-width: 42em;
			min-width: 36em;
			border-width: 0px;
			margin: auto auto;
		}
		#host, #dbfile{
			width: 300px;
		}
		#port{
			width: 75px;
		}
		#login, #password, #db{
			width: 150px;
		}
		#Copyright{
			text-align: right;
			font-size: 10px;
			color: #888888;
		}
	</style>
	<script type="text/javascript">
	function getInternetExplorerVersion(){
		var ver = -1;
		if (navigator.appName == "Microsoft Internet Explorer"){
			var regex = new RegExp("MSIE ([0-9]{1,}[\.0-9]{0,})");
			if (regex.exec(navigator.userAgent))
				ver = parseFloat(RegExp.$1);
		}
		return ver;
	}
	function setText(element, text, succ){
		element.className = (succ)?"TestSucc":"TestFail";
		element.innerHTML = text;
	}
	function getByteAt(str, offset){
		return str.charCodeAt(offset) & 0xff;
	}
	function getIntAt(binStr, offset){
		return (getByteAt(binStr, offset) << 24)+
			(getByteAt(binStr, offset+1) << 16)+
			(getByteAt(binStr, offset+2) << 8)+
			(getByteAt(binStr, offset+3) >>> 0);
	}
	function getBlockStr(binStr, offset){
		if (getByteAt(binStr, offset) < 254)
			return binStr.substring(offset+1, offset+1+binStr.charCodeAt(offset));
		else
			return binStr.substring(offset+5, offset+5+getIntAt(binStr, offset+1));
	}
	function doServerTest(){
		var version = getInternetExplorerVersion();
		if (version==-1 || version>=9.0){
			var xmlhttp = (window.XMLHttpRequest)? new XMLHttpRequest() : xmlhttp=new ActiveXObject("Microsoft.XMLHTTP");

			xmlhttp.onreadystatechange=function(){
				var outputDiv = document.getElementById("ServerTest");
				if (xmlhttp.readyState == 4){
					if (xmlhttp.status == 200){
						var errno = getIntAt(xmlhttp.responseText, 6);
						if (errno == 0)
							setText(outputDiv, "Connection Success!", true);
						else
							setText(outputDiv, parseInt(errno)+" - "+getBlockStr(xmlhttp.responseText, 16), false);
					}else
						setText(outputDiv, "HTTP Error - "+xmlhttp.status, false);
				}
			}

			var params = "";
			var form = document.getElementById("TestServerForm");
			for (var i=0; i<form.elements.length; i++){
				if (i>0) params += "&";
				params += encodeURIComponent(form.elements[i].id)+"="+encodeURIComponent(form.elements[i].value);
			}

			document.getElementById("ServerTest").className = "";
			document.getElementById("ServerTest").innerHTML = "Connecting...";
			xmlhttp.open("POST", "", true);
			xmlhttp.setRequestHeader("Content-type", "application/x-www-form-urlencoded");
			xmlhttp.setRequestHeader("Content-length", params.length);
			xmlhttp.setRequestHeader("Connection", "close");
			xmlhttp.send(params);
		}else{
			document.getElementById("ServerTest").className = "";
			document.getElementById("ServerTest").innerHTML = "Internet Explorer "+version+" is not supported, please use Internet explorer 9.0 or above, firefox, chrome or safari";
		}
	}
	</script>
</head>

<body>
<div id="page">
<p>
	<font class="Title1">SQLite HTTP Tunnel</font><br>
	<font class="Title2">A Go port of the Navicat SQLite tunnel</font>
</p>
<fieldset>
	<legend>System Environment Test</legend>
	<table>
		<tr style="display:none"><td width=70%>Server installed properly</td><td class="TestFail">No</td></tr>
`

// testPageTail 是测试页的静态后半部分（连接测试表单 + 版权信息）。
// 表单里的字段 id 会被 JS 当作 POST 参数名，所以不要随意改名。
const testPageTail = `	</table>
</fieldset>
<br>
<fieldset>
	<legend>Server Test</legend>
	<form id="TestServerForm" action="#" onSubmit="return false;">
	<input type=hidden id="actn" value="C">
	<table>
		<tr class="mysql"><td width="35%">Hostname/IP Address:</td><td><input type=text id="host" placeholder="localhost"></td></tr>
		<tr class="mysql"><td>Port:</td><td><input type=text id="port" placeholder=""></td></tr>
		<tr class="pgsql"><td>Initial Database:</td><td><input type=text id="db" placeholder="template1"></td></tr>
		<tr class="mysql"><td>Username:</td><td><input type=text id="login" placeholder=""></td></tr>
		<tr class="mysql"><td>Password:</td><td><input type=password id="password" placeholder=""></td></tr>
		<tr class="sqlite"><td>Database File:</td><td><input type=text id="dbfile" placeholder="sqlite.db"></td></tr>
		<tr><td></td><td><br><input id="TestButton" type="submit" value="Test Connection" onClick="doServerTest()"></td></tr>
	</table>
	</form>
	<div id="ServerTest"><br></div>
</fieldset>
<p id="Copyright">MIT License</p>
</div>
</body>
</html>
`
