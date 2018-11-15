dups
====

Find duplicate files in given directory trees. Where "duplicate" is defined as
having the same MD5 hash digest.

It is roughly equivalent to the following one-liner:
```sh
find . -type f -exec md5sum '{}' \; | awk '{digest = $1; path = $2; paths[digest, ++count[digest]] = path} END {for (digest in count) {n = count[digest]; if (n > 1) {print(digest, n); for (i=1; i<=n; i++) {print "    ", paths[digest, i]} } } }'
```

which, when indented, looks like:
```sh
find . -type f -exec md5sum '{}' \; \
| awk '
    {
        digest = $1
        path = $2
        paths[digest, ++count[digest]] = path
    }

    END {
        for (digest in count) {
            n = count[digest]
            if (n > 1) {
                print(digest, n)
                for (i=1; i<=n; i++) {
                    print "    ", paths[digest, i]
                }
            }
        }
    }'
```

and works well-enough, until you start getting weird file paths that are more
of a pain to handle quoting for than re-writing this thing in OCaml :)

Example
-------
After building, run `dups` on the current directory tree:

```sh
$ make
Finished, 0 targets (0 cached) in 00:00:00.
Finished, 5 targets (0 cached) in 00:00:00.

$ ./dups .
df4235f3da793b798095047810153c6b 2
    "./_build/dups.ml"
    "./dups.ml"
d41d8cd98f00b204e9800998ecf8427e 2
    "./_build/dups.mli"
    "./dups.mli"
087809b180957ce812a39a5163554502 2
    "./_build/dups.native"
    "./dups"
Processed 102 files in 0.025761 seconds.
```
Note that the report line (`Processed 102 files in 0.025761 seconds.`) is
written to `stderr`, so that `stdout` is safely processable by other tools.
