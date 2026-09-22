# GFS CLI — Complete Command Reference

## Start the GFS Client

```bash
./build/gfs-client --master 127.0.0.1:5000
```

Prompt:

```text
gfs>
```

---

## 1. Help

```text
gfs> help
gfs> help ls
gfs> help mkdir
gfs> help create
gfs> help delete
gfs> help rename
gfs> help stat
gfs> help read
gfs> help cat
gfs> help write
gfs> help append
gfs> help snapshot
gfs> help chunks
gfs> help locations
```

---

## 2. Exit / Quit

```text
gfs> exit
gfs> quit
```

---

## 3. List Directory

```text
gfs> ls /
gfs> ls /data
gfs> ls /data/files
```

---

## 4. Create Directory

```text
gfs> mkdir /data
gfs> mkdir /data/files
gfs> mkdir /data/logs
```

---

## 5. Create File

Default replication:

```text
gfs> create /data/file
```

Specific replication factor:

```text
gfs> create /data/file 3
gfs> create /data/test.txt 2
```

---

## 6. Write

Write at offset 0:

```text
gfs> write /data/file 0 "Hello GFS"
```

Write at another offset:

```text
gfs> write /data/file 10 "Hello"
```

Write text containing spaces:

```text
gfs> write /data/file 0 "Hello from Google File System"
gfs> write /data/file 0 "This is a GFS test file"
```

---

## 7. Append

```text
gfs> append /data/file " more data"
gfs> append /data/file " appended content"
gfs> append /data/file " this is appended to the file"
```

---

## 8. Read

Read complete file:

```text
gfs> read /data/file
```

Read a specific range:

```text
gfs> read /data/file 0 10
gfs> read /data/file 5 20
gfs> read /data/file 0 100
```

---

## 9. Cat

```text
gfs> cat /data/file
```

Example:

```text
gfs> cat /data/file
Hello GFS more data
```

---

## 10. Stat

```text
gfs> stat /data/file
```

---

## 11. Delete

```text
gfs> delete /data/file
gfs> delete /data/test.txt
```

---

## 12. Rename

```text
gfs> rename /data/file /data/newfile
gfs> rename /data/old /data/new
gfs> rename /data/test.txt /data/final.txt
```

---

## 13. Snapshot

```text
gfs> snapshot /data /snapshot
gfs> snapshot /data /snapshots/data_snapshot
```

---

## 14. Chunks

```text
gfs> chunks /data/file
```

---

## 15. Locations

```text
gfs> locations /data/file
```

---

# Complete CLI Workflow

```text
gfs> help

gfs> ls /

gfs> mkdir /data

gfs> ls /

gfs> create /data/file

gfs> stat /data/file

gfs> write /data/file 0 "Hello GFS"

gfs> read /data/file

gfs> append /data/file " more data"

gfs> cat /data/file

gfs> chunks /data/file

gfs> locations /data/file

gfs> rename /data/file /data/newfile

gfs> stat /data/newfile

gfs> cat /data/newfile

gfs> delete /data/newfile

gfs> ls /data

gfs> exit
```

---

# File Lifecycle

```text
gfs> mkdir /data

gfs> create /data/hello.txt

gfs> write /data/hello.txt 0 "Hello GFS"

gfs> read /data/hello.txt

gfs> append /data/hello.txt "!"

gfs> cat /data/hello.txt

gfs> stat /data/hello.txt

gfs> chunks /data/hello.txt

gfs> locations /data/hello.txt

gfs> rename /data/hello.txt /data/greeting.txt

gfs> delete /data/greeting.txt
```

---

# Directory Workflow

```text
gfs> mkdir /data
gfs> mkdir /data/documents
gfs> mkdir /data/logs

gfs> ls /
gfs> ls /data

gfs> create /data/documents/file1.txt
gfs> create /data/documents/file2.txt

gfs> write /data/documents/file1.txt 0 "First file"
gfs> write /data/documents/file2.txt 0 "Second file"

gfs> cat /data/documents/file1.txt
gfs> cat /data/documents/file2.txt
```

---

# Replication Workflow

```text
gfs> create /data/important.txt 3

gfs> write /data/important.txt 0 "Important distributed data"

gfs> locations /data/important.txt

gfs> chunks /data/important.txt
```

---

# Snapshot Workflow

```text
gfs> mkdir /data

gfs> create /data/file.txt

gfs> write /data/file.txt 0 "Snapshot test"

gfs> snapshot /data /snapshot/data

gfs> ls /snapshot/data
```

---

# Quoted Strings

Use quotes when the data contains spaces:

```text
gfs> write /data/file 0 "Hello GFS World"

gfs> append /data/file " this is additional data"
```

---

# Command Summary

| Command | Usage | Description |
|---|---|---|
| `help` | `help` | Show all commands |
| `help <command>` | `help <command>` | Show command help |
| `exit` | `exit` | Exit CLI |
| `quit` | `quit` | Exit CLI |
| `ls <path>` | `ls <path>` | List directory |
| `mkdir <path>` | `mkdir <path>` | Create directory |
| `create <path>` | `create <path>` | Create file |
| `create <path> <replication>` | `create <path> <replication>` | Create replicated file |
| `delete <path>` | `delete <path>` | Delete file/path |
| `rename <src> <dst>` | `rename <src> <dst>` | Rename path |
| `stat <path>` | `stat <path>` | Show file metadata |
| `read <path>` | `read <path>` | Read file |
| `read <path> <offset> <length>` | `read <path> <offset> <length>` | Read byte range |
| `cat <path>` | `cat <path>` | Display complete file |
| `write <path> <offset> <data>` | `write <path> <offset> <data>` | Write data |
| `append <path> <data>` | `append <path> <data>` | Append data |
| `snapshot <src> <dst>` | `snapshot <src> <dst>` | Create snapshot |
| `chunks <path>` | `chunks <path>` | Show chunks |
| `locations <path>` | `locations <path>` | Show chunk locations |

---

# Start GFS Services

## Master

```bash
./build/gfs-master
```

## Chunkserver

```bash
./build/gfs-chunkserver --master 127.0.0.1:5000
```

## Client

```bash
./build/gfs-client --master 127.0.0.1:5000
```

---

# Build Project

```bash
cmake --build build -j2
```

---

# Run All Tests

```bash
ctest --test-dir build --output-on-failure
```

---

# Run Phase 17 Test

```bash
./build/gfs_phase17_test
```

# Run Phase 18 Test

```bash
./build/gfs_phase18_test
```

---


# Quick Copy-Paste CLI Test

```text
mkdir /data
create /data/file
write /data/file 0 "Hello GFS"
read /data/file
append /data/file " more data"
cat /data/file
stat /data/file
chunks /data/file
locations /data/file
rename /data/file /data/newfile
cat /data/newfile
delete /data/newfile
ls /data
exit
