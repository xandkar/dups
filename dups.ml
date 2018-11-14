open Printf

module Array = ArrayLabels
module List  = ListLabels
module StrSet= Set.Make(String)
module Unix  = UnixLabels

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
  | Stdin
  | Directories of string list

type output =
  | Stdout
  | Directory of string

let make_input_stream = function
  | Stdin ->
      In_channel.lines stdin
  | Directories paths ->
      let paths = StrSet.elements (StrSet.of_list paths) in
      Stream.concat (List.map paths ~f:Directory_tree.find_files)

let make_output_fun = function
  | Stdout ->
      fun digest n_paths paths ->
        printf "%s %d\n%!" (Digest.to_hex digest) n_paths;
        List.iter (StrSet.elements paths) ~f:(printf "    %S\n%!")
  | Directory dir ->
      fun digest _ paths ->
        let digest = Digest.to_hex digest in
        let dir = Filename.concat dir (String.sub digest 0 2) in
        Unix.mkdir dir ~perm:0o700;
        let oc = open_out (Filename.concat dir digest) in
        List.iter (StrSet.elements paths) ~f:(fun path ->
          output_string oc (sprintf "%S\n%!" path)
        );
        close_out oc

let main input output =
  let output = make_output_fun  output in
  let input  = make_input_stream input in
  let paths_by_digest = Hashtbl.create 1_000_000 in
  let path_count = ref 0 in
  let t0 = Sys.time () in
  Stream.iter input ~f:(fun path ->
    incr path_count;
    try
      let digest = Digest.file path in
      let count, paths =
        match Hashtbl.find_opt paths_by_digest digest with
        | None ->
            (0, StrSet.empty)
        | Some (n, paths) ->
            (n, paths)
      in
      Hashtbl.replace paths_by_digest digest (count + 1, StrSet.add path paths)
    with Sys_error e ->
      eprintf "WARNING: Failed to process %S: %S\n%!" path e
  );
  Hashtbl.iter (fun d (n, ps) -> if n > 1 then output d n ps) paths_by_digest;
  let t1 = Sys.time () in
  eprintf "Processed %d files in %f seconds.\n%!" !path_count (t1 -. t0)

let () =
  let input  = ref Stdin in
  let output = ref Stdout in
  let assert_file_exists path =
    if Sys.file_exists path then
      ()
    else begin
      eprintf "File does not exist: %S\n%!" path;
      exit 1
    end
  in
  let assert_file_is_dir path =
    if Sys.is_directory path then
      ()
    else begin
      eprintf "File is not a directory: %S\n%!" path;
      exit 1
    end
  in
  let spec =
    [ ( "-out"
      , Arg.String (fun path ->
          assert_file_exists path;
          assert_file_is_dir path;
          output := Directory path
        )
      , " Output to this directory instead of stdout."
      )
    ]
  in
  Arg.parse
    (Arg.align spec)
    (fun path ->
      assert_file_exists path;
      assert_file_is_dir path;
      match !input with
      | Stdin ->
          input := Directories [path]
      | Directories paths ->
          input := Directories (path :: paths)
    )
    "";
  main !input !output
