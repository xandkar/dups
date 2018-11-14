open Printf

module Array = ArrayLabels
module List  = ListLabels
module StrSet= Set.Make(String)

module Stream : sig
  type 'a t

  val create : (unit -> 'a option) -> 'a t

  val iter : 'a t -> f:('a -> unit) -> unit

  val concat : ('a t) list -> 'a t
end = struct
  module S = Stream

  type 'a t =
    ('a S.t) list

  let create f =
    [S.from (fun _ -> f ())]

  let iter t ~f =
    List.iter t ~f:(S.iter f)

  let concat ts =
    List.concat ts
end

module In_channel : sig
  val lines : in_channel -> string Stream.t
end = struct
  let lines ic =
    Stream.create (fun () ->
      match input_line ic with
      | exception End_of_file ->
          None
      | line ->
          Some line
    )
end

module Directory_tree : sig
  val find_files : string -> string Stream.t
end = struct
  let find_files root =
    let dirs  = Queue.create () in
    let files = Queue.create () in
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
    explore root;
    let rec next () =
      match Queue.is_empty files, Queue.is_empty dirs with
      | false, _     -> Some (Queue.take files)
      | true , true  -> None
      | true , false ->
          explore (Queue.take dirs);
          next ()
    in
    Stream.create next
end

type input =
  | Root_paths of string list
  | Paths_on_stdin

let main input =
  let paths =
    match input with
    | Paths_on_stdin ->
        In_channel.lines stdin
    | Root_paths paths ->
        let paths = StrSet.elements (StrSet.of_list paths) in
        Stream.concat (List.map paths ~f:Directory_tree.find_files)
  in
  let paths_by_digest = Hashtbl.create 1_000_000 in
  let path_count = ref 0 in
  let t0 = Sys.time () in
  Stream.iter paths ~f:(fun path ->
    incr path_count;
    try
      let digest = Digest.file path in
      let paths =
        match Hashtbl.find_opt paths_by_digest digest with
        | None ->
            StrSet.empty
        | Some paths ->
            paths
      in
      Hashtbl.replace paths_by_digest digest (StrSet.add path paths)
    with Sys_error e ->
      eprintf "WARNING: Failed to process %S: %S\n%!" path e
  );
  Hashtbl.iter
    (fun digest paths ->
      let n_paths = StrSet.cardinal paths in
      if n_paths > 1 then begin
        printf "%s %d\n%!" (Digest.to_hex digest) n_paths;
        List.iter (StrSet.elements paths) ~f:(printf "    %s\n%!")
      end
    )
    paths_by_digest;
  let t1 = Sys.time () in
  eprintf "Processed %d files in %f seconds.\n%!" !path_count (t1 -. t0)

let () =
  let input = ref Paths_on_stdin in
  Arg.parse
    []
    (function
    | path when Sys.file_exists path ->
        (match !input with
        | Paths_on_stdin ->
            input := Root_paths [path]
        | Root_paths paths ->
            input := Root_paths (path :: paths)
        )
    | path ->
        eprintf "File does not exist: %S\n%!" path;
        exit 1
    )
    "";
  main !input
