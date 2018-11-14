open Printf

module Array = ArrayLabels
module List  = ListLabels

module Stream : sig
  type 'a t
  val lines : in_channel -> string t
  val rec_file_paths : root:string -> string t
  val iter : 'a t -> f:('a -> unit) -> unit
end = struct
  module S = Stream

  type 'a t =
    'a S.t

  let rec_file_paths ~root =
    let dirs  = Queue.create () in
    let files = Queue.create () in
    Queue.add root dirs;
    let explore parent =
      Array.iter (Sys.readdir parent) ~f:(fun child ->
        let path = Filename.concat parent child in
        let {Unix.st_kind = file_kind; _} = Unix.lstat path in
        match file_kind with
        | Unix.S_REG ->
            Queue.add path files
        | Unix.S_DIR ->
            Queue.add path dirs
        | Unix.S_CHR
        | Unix.S_BLK
        | Unix.S_LNK
        | Unix.S_FIFO
        | Unix.S_SOCK ->
            ()
      )
    in
    let next_dir () =
      match Queue.take dirs with
      | exception Queue.Empty ->
          ()
      | dir ->
          explore dir
    in
    let next_file () =
      match Queue.take files with
      | exception Queue.Empty ->
          None
      | file_path ->
          Some file_path
    in
    S.from (fun _ ->
      next_dir ();
      next_file ()
    )

  let lines ic =
    S.from (fun _ ->
      match input_line ic with
      | exception End_of_file ->
          None
      | line ->
          Some line
    )

  let iter t ~f =
    S.iter f t
end

type input =
  | Root_path of string
  | Paths_on_stdin

let main input =
  let paths =
    match input with
    | Paths_on_stdin -> Stream.lines stdin
    | Root_path root -> Stream.rec_file_paths ~root
  in
  let paths_by_digest = Hashtbl.create 1_000_000 in
  Stream.iter paths ~f:(fun path ->
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
  let input = ref Paths_on_stdin in
  Arg.parse [] (fun path -> input := Root_path path) "";
  main !input
