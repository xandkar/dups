#! /bin/sh

find $@ -type f -print0 \
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
