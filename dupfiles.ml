open Printf

module List = ListLabels

module Stream : sig
  val lines : in_channel -> f:(string -> unit) -> unit
end = struct
  module S = Stream

  let lines_of_channel ic =
    S.from (fun _ ->
      match input_line ic with
      | exception End_of_file ->
          None
      | line ->
          Some line
    )

  let iter t ~f =
    S.iter f t

  let lines ic ~f =
    iter (lines_of_channel ic) ~f
end

let main ic =
  let paths_by_digest = Hashtbl.create 1_000_000 in
  Stream.lines ic ~f:(fun path ->
    try
      let digest = Digest.file path in
      let paths =
        match Hashtbl.find_opt paths_by_digest digest with
        | None ->
            []
        | Some paths ->
            paths
      in
      Hashtbl.replace paths_by_digest digest (path :: paths)
    with Sys_error e ->
      eprintf "WARNING: Failed to process %S: %S\n%!" path e
  );
  Hashtbl.iter
    (fun digest paths ->
      let n_paths = List.length paths in
      if n_paths > 1 then begin
        printf "%s %d\n%!" (Digest.to_hex digest) n_paths;
        List.iter paths ~f:(fun path -> printf "    %s\n%!" path)
      end
    )
    paths_by_digest

let () =
  let ic = ref stdin in
  Arg.parse [] (fun filename -> ic := open_in filename) "";
  main !ic;
  close_in !ic
