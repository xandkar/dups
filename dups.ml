open Printf

module Array = ArrayLabels
module List  = ListLabels
module StrSet= Set.Make(String)
module Unix  = UnixLabels

module Stream : sig
  type 'a t

  val create : (unit -> 'a option) -> 'a t

  val iter : 'a t -> f:('a -> unit) -> unit

  val map : 'a t -> f:('a -> 'b) -> 'b t

  val filter : 'a t -> f:('a -> bool) -> 'a t

  val concat : ('a t) list -> 'a t
end = struct
  module S = Stream

  type 'a t =
    {mutable streams : ('a S.t) list}

  let create f =
    {streams = [S.from (fun _ -> f ())]}

  let rec next t =
    match t.streams with
    | [] ->
        None
    | s :: streams ->
        (match S.next s with
        | exception Stream.Failure ->
            t.streams <- streams;
            next t
        | x ->
            Some x
        )

  let map t ~f =
    create (fun () ->
      match next t with
      | None   -> None
      | Some x -> Some (f x)
    )

  let filter t ~f =
    let rec filter () =
      match next t with
      | None ->
          None
      | Some x when f x ->
          Some x
      | Some _ ->
          filter ()
    in
    create filter

  let iter t ~f =
    List.iter t.streams ~f:(S.iter f)

  let concat ts =
    {streams = List.concat (List.map ts ~f:(fun {streams} -> streams))}
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

module File : sig
  type t =
    { path : string
    ; size : int
    }

  val find : string -> t Stream.t
  (** Find all files in the directory tree, starting from the given root path *)

  val lookup : string Stream.t -> t Stream.t
  (** Lookup file info for given paths *)
end = struct
  type t =
    { path : string
    ; size : int
    }

  let lookup paths =
    Stream.map paths ~f:(fun path ->
      let {Unix.st_size = size; _} = Unix.lstat path in
      {path; size}
    )

  let find root =
    let dirs  = Queue.create () in
    let files = Queue.create () in
    let explore parent =
      Array.iter (Sys.readdir parent) ~f:(fun child ->
        let path = Filename.concat parent child in
        let {Unix.st_kind = file_kind; st_size; _} = Unix.lstat path in
        match file_kind with
        | Unix.S_REG ->
            let file = {path; size = st_size} in
            Queue.add file files
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

type opt =
  { input  : input
  ; output : output
  ; ignore : Str.regexp option
  }

type count =
  { considered  : int ref
  ; empty       : int ref
  ; ignored     : int ref
  ; unique_size : int ref
  ; hashed      : int ref
  }

let make_input_stream input ignore count =
  let input =
    match input with
    | Stdin ->
        File.lookup (In_channel.lines stdin)
    | Directories paths ->
        let paths = StrSet.elements (StrSet.of_list paths) in
        Stream.concat (List.map paths ~f:File.find)
  in
  Stream.filter input ~f:(fun {File.path; size} ->
    incr count.considered;
    let empty = size = 0 in
    let ignored =
      match ignore with
      | Some regexp when (Str.string_match regexp path 0) ->
          true
      | Some _ | None ->
          false
    in
    if empty   then incr count.empty;
    if ignored then incr count.ignored;
    (not empty) && (not ignored)
  )

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

let main {input; output; ignore} =
  let t0 = Sys.time () in
  let count =
    { considered  = ref 0
    ; empty       = ref 0
    ; ignored     = ref 0
    ; unique_size = ref 0
    ; hashed      = ref 0
    }
  in
  let output = make_output_fun  output in
  let input  = make_input_stream input ignore count in
  let paths_by_size = Hashtbl.create 1_000_000 in
  let paths_by_digest = Hashtbl.create 1_000_000 in
  let process tbl path ~f =
    let key = f path in
    let count, paths =
      match Hashtbl.find_opt tbl key with
      | None ->
          (0, StrSet.empty)
      | Some (n, paths) ->
          (n, paths)
    in
    Hashtbl.replace tbl key (count + 1, StrSet.add path paths)
  in
  Stream.iter input ~f:(fun {File.path; size} ->
    process paths_by_size path ~f:(fun _ -> size)
  );
  Hashtbl.iter
    (fun _ (n, paths) ->
      (* Skip files with unique sizes *)
      if n > 1 then
        StrSet.iter
          (fun path ->
            incr count.hashed;
            process paths_by_digest path ~f:Digest.file
          )
          paths
      else
        incr count.unique_size;
    )
    paths_by_size;
  Hashtbl.iter (fun d (n, ps) -> if n > 1 then output d n ps) paths_by_digest;
  let t1 = Sys.time () in
  eprintf "Time                       : %f seconds\n%!" (t1 -. t0);
  eprintf "Considered                 : %d\n%!" !(count.considered);
  eprintf "Hashed                     : %d\n%!" !(count.hashed);
  eprintf "Skipped due to 0      size : %d\n%!" !(count.empty);
  eprintf "Skipped due to unique size : %d\n%!" !(count.unique_size);
  eprintf "Ignored due to regex match : %d\n%!" !(count.ignored)

let get_opt () : opt =
  let assert_ test x msg =
    if not (test x) then begin
      eprintf "%s\n%!" msg;
      exit 1
    end
  in
  let assert_file_exists path =
    assert_ Sys.file_exists path (sprintf "File does not exist: %S" path)
  in
  let assert_file_is_dir path =
    assert_ Sys.is_directory path (sprintf "File is not a directory: %S" path)
  in
  let input  = ref Stdin in
  let output = ref Stdout in
  let ignore = ref None in
  let spec =
    [ ( "-out"
      , Arg.String (fun path ->
          assert_file_exists path;
          assert_file_is_dir path;
          output := Directory path
        )
      , " Output to this directory instead of stdout."
      )
    ; ( "-ignore"
      , Arg.String (fun regexp -> ignore := Some (Str.regexp regexp))
      , " Ignore file paths which match this regexp pattern (see Str module)."
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
  { input  = !input
  ; output = !output
  ; ignore = !ignore
  }

let () =
  main (get_opt ())
