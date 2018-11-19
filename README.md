dups
====

Find duplicate files in given directory trees. Where "duplicate" is defined as
having the same (and non-0) file size and MD5 hash digest.

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
e40e3c4330857e2762d043427b499301 2
    "./_build/dups.native"
    "./dups"
3d1c679e5621b8150f54d21f3ef6dcad 2
    "./_build/dups.ml"
    "./dups.ml"
Time                       : 0.031084 seconds
Considered                 : 121
Hashed                     : 45
Skipped due to 0      size : 2
Skipped due to unique size : 74
Ignored due to regex match : 0

```
Note that the report lines are written to `stderr`, so that `stdout` is safely
processable by other tools:

```
$ ./dups . 2> /dev/null
e40e3c4330857e2762d043427b499301 2
    "./_build/dups.native"
    "./dups"
3d1c679e5621b8150f54d21f3ef6dcad 2
    "./_build/dups.ml"
    "./dups.ml"

$ ./dups . 1> /dev/null
Time                       : 0.070765 seconds
Considered                 : 121
Hashed                     : 45
Skipped due to 0      size : 2
Skipped due to unique size : 74
Ignored due to regex match : 0

```
