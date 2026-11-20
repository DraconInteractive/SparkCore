# Password-encrypted files

In `/files/explorer`, click **Encrypt / decrypt** beside a file (or open its name).
The file window offers Encrypt for plaintext files, or these three decrypt modes:

- **Decrypt (View)**: enter the password to preview UTF-8 text or common images in
  memory. The stored file and revision stay encrypted. Close or Reload clears the
  preview. Temporary preview limit: 10 MiB. Active HTML/SVG/PDF content is not
  executed; HTML is displayed as text, and images use restricted raster formats.
- **Decrypt (Edit)**: unlock UTF-8 text up to the existing 2 MiB editor limit.
  **Save encrypted changes** saves new ciphertext, then locks the viewer again.
  Closing without saving discards edits. The server never publishes or stages
  plaintext for this mode. Mixed/legacy line endings remain view-only.
- **Decrypt (Full)**: permanently replace the encrypted contents with plaintext.
  Works for binary files too. Normal file permissions—including assistant file
  tools—can then read that plaintext without the encryption password.

Encrypt asks for the password twice. Passwords must be at least 8 UTF-8 bytes and
at most 1024 bytes; a long unique passphrase is preferable. SPARK does not retain a
password or recovery key. Losing the password means losing access to the contents.
Names, directories, sizes and other file metadata are not encrypted.

The file ID, name and directory do not change. Downloading, sharing a lease,
exporting a project or backing up an encrypted file transfers encrypted bytes.
Re-uploading those bytes preserves the envelope and permits password decryption on
another updated SPARK instance. Plaintext files encrypted elsewhere are not
implicitly supported; this feature uses the versioned format described below.

## API

All responses have `Cache-Control: private, no-store`. Passwords belong in JSON
request bodies, never URL parameters. SPARK's logs do not record request bodies.

| Endpoint | Input | Result |
| --- | --- | --- |
| `GET /files/{id}/encryption` | No body | `encrypted`, `format`, `plaintext_bytes`; current `ETag` header |
| `POST /files/{id}/encrypt` | `{"password":"..."}` | Saved file metadata; new ETag |
| `POST /files/{id}/decrypt` | `{"password":"...","mode":"view"}` | Raw plaintext with ETag; unchanged stored file |
| Same, mode `edit` | Same password | Raw validated UTF-8 text; unchanged stored file |
| Same, mode `full` | Same password | Permanently decrypted file metadata; new ETag |
| `PUT /files/{id}/encrypted-content` | `{"password":"...","text":"..."}` | Updated encrypted file metadata; new ETag |

Every POST/PUT requires an exact `If-Match` from the current encryption metadata
or content response. Stale versions return 412; reload before deciding what to
save. View needs `files.read`; encryption, full decryption, and editing need both
`files.read` and `files.write`. Existing browser session/CSRF and token rules apply.
No new permissions or database migration are needed for this feature.

Wrong passwords and authentication failures return 422
`incorrect_password_or_damaged_file`, with no plaintext response. Invalid envelopes
return 409. Editing ciphertext using ordinary `PUT /files/{id}/content` returns
409 `encrypted_file_requires_encrypted_edit`. A concurrent cryptographic operation
may return 503 `encryption_busy`; retry later. Only one cryptographic request per
process is allowed at a time to bound scrypt resource use. Storage operations are
serialized during transformation; large files may take time.

A full-file transformation writes a new revision and publishes it only after
completion/authentication, fsync, and a final authorization check. After success the
old revision is unlinked. Failed transforms remove the unpublished candidate;
uncertain database commit results retain versions for existing startup recovery.
Every committed transformation emits `file.updated`, and leases for old revisions
stop working. Temporary view/edit unlock emits no mutation event.

## Security boundary

Encryption is performed **by the SPARK server**. It is encryption at rest, not
end-to-end encryption against the server operator. Send passwords/content over
HTTPS or a trusted encrypted tunnel (for example Tailscale), not untrusted plain
HTTP. Browser temporary view/edit holds plaintext in DOM/memory; edit also holds the
password in memory until save, reload, close or page exit. Nothing is intentionally
saved to browser storage. The server buffers preview plaintext only in memory and
clears its working buffers on scope exit; OS swap, crash dumps, browser internals,
and copies the user makes are outside that guarantee.

Encrypting does not retroactively encrypt prior backups, exported copies or files
already downloaded. Unlinking old content is not secure erasure of SSD blocks or
filesystem snapshots. Full decryption intentionally writes plaintext to disk.

## SPARKENC-1 format

This is a versioned envelope built from OpenSSL scrypt and AES-256-GCM primitives,
not a new cipher. See the [OpenSSL authenticated encryption documentation](https://docs.openssl.org/3.0/man3/EVP_EncryptInit/).

The fixed 48-byte header contains:

| Offset | Size | Value |
| --- | --- | --- |
| 0 | 8 | ASCII `SPARKENC` |
| 8 | 1 | Version 1 |
| 9 | 7 | Reserved, all zero |
| 16 | 16 | Random scrypt salt |
| 32 | 8 | Random nonce prefix |
| 40 | 8 | Unsigned plaintext length, big endian |

Derive 32 bytes using scrypt: UTF-8 password bytes, N=32768, r=8, p=3, 64 MiB
allocation ceiling. This uses the 32 MiB scrypt configuration in the
[OWASP work-factor guidance](https://cheatsheetseries.owasp.org/cheatsheets/Password_Storage_Cheat_Sheet.html#scrypt). Split plaintext into 1,048,576-byte chunks (last may be shorter).
Empty files still have one zero-length authenticated chunk. Every chunk contains
ciphertext followed by a 16-byte GCM tag. The 12-byte nonce is the header nonce
prefix followed by the chunk's zero-based 32-bit big-endian index. Additional
authenticated data is the complete 48-byte header followed by that 4-byte index.
No padding is used. The file length must exactly match header + plaintext length +
16 bytes per chunk. The format permits at most 2^32−1 chunks (slightly under 4 PiB).
Fresh salt and nonce prefix are generated for every encryption and edited save.
Header/index authentication and exact length checks reject changed headers,
reordered/duplicated chunks, truncation and appended data. No view response or full
replacement is published until all chunks authenticate.

## Testing

Build `file_crypto_tests`, `file_encryption_tests`, `authorization_tests`, and
`spark_tests`, then run `scripts/test-file-encryption.sh` for a disposable database
integration run. `scripts/test-file-encryption-page.py` exercises all UI modes in
headless Firefox. Neither test modifies the live main/test instances.

An independent implementation check is available via `python3 scripts/test-file-crypto-interop.py`
(requires the test-only Python `cryptography` package); it verifies both directions
between C++/OpenSSL and Python AESGCM.
