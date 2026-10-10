# Vendored dependencies

- cpp-httplib v0.60.1: https://github.com/yhirose/cpp-httplib/tree/v0.60.1
  (`httplib.h`, MIT license in `cpp-httplib/LICENSE`).
- nlohmann/json v3.11.3: https://github.com/nlohmann/json/tree/v3.11.3
  (`single_include/nlohmann/json.hpp`, MIT license in `nlohmann/LICENSE.MIT`).

Headers are unmodified upstream release files. TLS and compression integrations
are not enabled. Review upstream changes when updating these pinned versions.

cpp-httplib header SHA-256: `e932d7771bdb93150868987e30112d9a31e24bef592d6d7a07e973d5845cf8d6`.

The v0.60.1 update includes upstream fixes for percent-decoded header injection,
chunked trailer header injection, and negative chunk-size handling.
`http_security` exercises these cases using raw HTTP requests.
