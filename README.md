# Navicat SQLite HTTP Tunnel (Go)

Go port of Navicat's `ntunnel_sqlite.php`. It speaks the same binary protocol, so Navicat can use it as an HTTP tunnel endpoint for SQLite files.

## Build and run

```sh
go build -o ntunnel-sqlite .
./ntunnel-sqlite -addr :8080          # normal logs
./ntunnel-sqlite -addr :8080 -debug   # + full SQL, response hex, sample rows
```

Requires Go 1.21+. SQLite comes from `modernc.org/sqlite`, a pure-Go driver, so no cgo or external SQLite library is needed.

Logs are written to stderr, one `[#N]` block per request.

## Navicat setup

1. Open the connection settings and go to the **HTTP** tab.
2. Enable **Use HTTP Tunnel** and set the URL to `http://your-host:8080/`.
3. Set **Database File** to the path on the server, for example `C:/data/app.db`.

Opening the URL in a browser shows the built-in self-test page.

## Supported actions

| `actn` | Meaning |
| ------ | ------- |
| `C` | Connection test, returns the SQLite library version |
| `Q` | Execute one or more queries (`q` / `q[]` / `q[N]` form fields) |
| `3` | Create a new database file (must not exist yet) |
| `2` | Create a SQLite 2 database, always answered with error 203 |

## Notes

- `dbfile` is resolved on the server, not on the client.
- Multipart and urlencoded form bodies are both accepted.
- Binary responses are buffered, sent with an exact `Content-Length` and `charset=x-user-defined`.
- Error codes: `202` invalid parameters or unusable file, `203` SQLite 2 not supported.
- `-debug` also prints the first 256 bytes of each response in hex, which helps when a client fails to parse a result.

## License

MIT. See [LICENSE](LICENSE).
