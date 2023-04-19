dups
====

Find duplicate files in N given directory trees. Where "duplicate" is defined
as having the same (and non-0) file size and MD5 hash digest.

It is roughly equivalent to the following one-liner (included as `dups.sh`):
```sh
find . -type f -print0 | xargs -0 -P $(nproc) -I % md5sum % | awk '{digest = $1;  sub("^" $1 " +", ""); path = $0; paths[digest, ++cnt[digest]] = path} END {for (digest in cnt) {n = cnt[digest]; if (n > 1) {print(digest, n); for (i=1; i<=n; i++) {printf "    %s\n", paths[digest, i]} } } }'
```

which, when indented, looks like:
```sh
find . -type f -print0 \
| xargs -0 -P $(nproc) md5sum \
| awk '
    {
        digest = $1
        sub("^" $1 " +", "")
        path = $0
        paths[digest, ++count[digest]] = path
    }

    END {
        for (digest in count) {
            n = count[digest]
            if (n > 1) {
                print(digest, n)
                for (i=1; i<=n; i++) {
                    printf "    %s\n", paths[digest, i]
                }
            }
        }
    }'
```

and works well-enough, but is painfully slow (for instance, it takes around 8
minutes to process my home directory, whereas `dups` takes around 8 seconds).

Originally, my main motivation for rewriting the above script in OCaml was
simply to avoid dealing with file paths containing newlines and spaces (the
original rewrite was substantially simpler than it currently is).

I since realized that, on the _input_, the problem is avoided by delimiting the
found paths with the null byte, rather than a newline and in AWK doing an
ostensible `shift` of the `$0` field (`sub("^" $1 " +", "")`).

However, on the _output_, I still don't know of a _simple_ way to escape the
newline in AWK (in OCaml, there's the `%S` in `printf` and in GNU `printf`
there's `%q`).

In any case, I now have 2 other reasons to continue with this project:
1. The speed-up is a boon to my UX (thanks in large part to optimizations
   suggested by @Aeronotix);
2. I plan to extend the feature set, which is just too-unpleasant to manage in
   a string-oriented PL:
    1. byte-by-byte comparison of files that hash to the same digest, to make
       super-duper sure they are indeed the same and do not just happen to
       collide;
    2. extend the metrics reporting;
    3. output sorting options.

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

TODO
-------------------------------------------------------------------------------

- [ ] Look into integrating with external tools for perceptual hashing and the like
  - https://github.com/knjcode/imgdupes
  - https://github.com/visual-layer/fastdup
- [ ] Consider implementing perceptual hashing
- [ ] Consider rewriting in Rust
- [ ] Can Cayley hashing speed anything up?
  <https://github.com/benwr/bromberg_sl2>
