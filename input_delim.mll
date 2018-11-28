rule by_null = parse
| eof                  {None}
| [^ '\000']+ as line  {Some line}
| '\000'+              {by_null lexbuf}
