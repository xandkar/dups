open Printf

module Array = ArrayLabels
module List  = ListLabels
module StrSet = Set.Make(String)
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

  module Set : sig include Set.S with type elt := t end
end = struct
  type t =
    { path : string
    ; size : int
    }

  let compare {path=p1; _} {path=p2; _} =
    Stdlib.compare p1 p2

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

  module Set = Set.Make(struct
    type elt = t
    type t = elt
    let compare = compare
  end)
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
  ; sample : int
  }

type count =
  { considered_files    : int ref
  ; considered_bytes    : int ref
  ; empty               : int ref
  ; ignored_files       : int ref
  ; ignored_bytes       : int ref
  ; unique_size_files   : int ref
  ; unique_size_bytes   : int ref
  ; unique_sample_files : int ref
  ; unique_sample_bytes : int ref
  ; sampled_files       : int ref
  ; sampled_bytes       : int ref
  ; hashed_files        : int ref
  ; hashed_bytes        : int ref
  ; digests             : int ref
  }

let add sum addend =
  sum := !sum + addend

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
    incr count.considered_files;
    add count.considered_bytes size;
    let empty = size = 0 in
    let ignored =
      match ignore with
      | Some regexp when (Str.string_match regexp path 0) ->
          incr count.ignored_files;
          add count.ignored_bytes size;
          true
      | Some _ | None ->
          false
    in
    if empty   then incr count.empty;
    (not empty) && (not ignored)
  )

let make_output_fun = function
  | Stdout ->
      fun digest n_files files ->
        printf "%s %d\n%!" (Digest.to_hex digest) n_files;
        List.iter (File.Set.elements files) ~f:(fun {File.path; _} ->
          printf "    %S\n%!" path
        )
  | Directory dir ->
      fun digest _ files ->
        let digest = Digest.to_hex digest in
        let dir = Filename.concat dir (String.sub digest 0 2) in
        Unix.mkdir dir ~perm:0o700;
        let oc = open_out (Filename.concat dir digest) in
        List.iter (File.Set.elements files) ~f:(fun {File.path; _} ->
          output_string oc (sprintf "%S\n%!" path)
        );
        close_out oc

let sample path ~len ~count =
  let buf = Bytes.make len ' ' in
  let ic = open_in_bin path in
  let rec read pos len =
    assert (len >= 0);
    if len = 0 then
      ()
    else begin
      let chunk_size = input ic buf pos len in
      add count.sampled_bytes chunk_size;
      if chunk_size = 0 then (* EOF *)
        ()
      else
        read (pos + chunk_size) (len - chunk_size)
    end
  in
  read 0 len;
  close_in ic;
  Bytes.to_string buf

let main {input; output; ignore; sample = sample_len} =
  let t0 = Sys.time () in
  let count =
    { considered_files    = ref 0
    ; considered_bytes    = ref 0
    ; empty               = ref 0
    ; ignored_files       = ref 0
    ; ignored_bytes       = ref 0
    ; unique_size_files   = ref 0
    ; unique_size_bytes   = ref 0
    ; sampled_files       = ref 0
    ; sampled_bytes       = ref 0
    ; hashed_files        = ref 0
    ; hashed_bytes        = ref 0
    ; unique_sample_files = ref 0
    ; unique_sample_bytes = ref 0
    ; digests             = ref 0
    }
  in
  let output = make_output_fun  output in
  let input  = make_input_stream input ignore count in
  let files_by_size = Hashtbl.create 1_000_000 in
  let files_by_sample = Hashtbl.create 1_000_000 in
  let files_by_digest = Hashtbl.create 1_000_000 in
  let process tbl ~group ~file =
    let count, files =
      match Hashtbl.find_opt tbl group with
      | None ->
          (0, File.Set.empty)
      | Some (n, files) ->
          (n, files)
    in
    Hashtbl.replace tbl group (count + 1, File.Set.add file files)
  in
  (* TODO: Make a nice(r) abstraction to re-assemble pieces in the pipeline:
   *
   * from input           to files_by_size
   * from files_by_size   to files_by_sample
   * from files_by_sample to files_by_digest
   * from files_by_digest to output
   *
   * input |> files_by_size |> files_by_sample |> files_by_digest |> output
   *)
  let t0_group_by_size = Sys.time () in
  Stream.iter input ~f:(fun ({File.size; _} as file) ->
    process files_by_size ~group:size ~file
  );
  let t1_group_by_size = Sys.time () in
  let t0_group_by_sample = Sys.time () in
  Hashtbl.iter
    (fun _ (n, files) ->
      (* Skip files with unique sizes *)
      if n > 1 then
        File.Set.iter
          (fun ({File.path; _} as file) ->
            incr count.sampled_files;
            process
              files_by_sample
              ~group:(sample path ~len:sample_len ~count)
              ~file
          )
          files
      else
        File.Set.iter
          (fun {File.size; _} ->
            incr count.unique_size_files;
            add count.unique_size_bytes size
          )
          files
    )
    files_by_size;
  let t1_group_by_sample = Sys.time () in
  let t0_group_by_digest = Sys.time () in
  Hashtbl.iter
    (fun _ (n, files) ->
      (* Skip files with unique samples *)
      if n > 1 then
        File.Set.iter
          (fun ({File.path; size} as file) ->
            incr count.hashed_files;
            add count.hashed_bytes size;
            process files_by_digest ~group:(Digest.file path) ~file
          )
          files
      else
        File.Set.iter
          (fun {File.size; _} ->
            incr count.unique_sample_files;
            add count.unique_sample_bytes size;
          )
          files
    )
    files_by_sample;
  let t1_group_by_digest = Sys.time () in
  Hashtbl.iter
    (fun d (n, files) ->
      incr count.digests;
      if n > 1 then
        output d n files
    )
    files_by_digest;
  let t1 = Sys.time () in
  let b_to_mb b = (float_of_int b) /. 1024. /. 1024. in
  let b_to_gb b = (b_to_mb b) /. 1024. in
  eprintf "Time                         : %8.2f seconds\n%!" (t1 -. t0);
  eprintf "Considered                   : %8d files  %6.2f Gb\n%!"
    !(count.considered_files)
    (b_to_gb !(count.considered_bytes));
  eprintf "Sampled                      : %8d files  %6.2f Gb\n%!"
    !(count.sampled_files)
    (b_to_gb !(count.sampled_bytes));
  eprintf "Hashed                       : %8d files  %6.2f Gb  %6.2f seconds\n%!"
    !(count.hashed_files)
    (b_to_gb !(count.hashed_bytes))
    (t1_group_by_digest -. t0_group_by_digest);
  eprintf "Digests                      : %8d\n%!"
    !(count.digests);
  eprintf "Duplicates (Hashed - Digests): %8d\n%!"
    (!(count.hashed_files) - !(count.digests));
  eprintf "Skipped due to 0      size   : %8d files\n%!" !(count.empty);
  eprintf "Skipped due to unique size   : %8d files  %6.2f Gb  %6.2f seconds\n%!"
    !(count.unique_size_files)
    (b_to_gb !(count.unique_size_bytes))
    (t1_group_by_size -. t0_group_by_size);
  eprintf "Skipped due to unique sample : %8d files  %6.2f Gb  %6.2f seconds\n%!"
    !(count.unique_sample_files)
    (b_to_gb !(count.unique_sample_bytes))
    (t1_group_by_sample -. t0_group_by_sample);
  eprintf "Ignored due to regex match   : %8d files  %6.2f Gb\n%!"
    !(count.ignored_files)
    (b_to_gb !(count.ignored_bytes))

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
  let sample = ref 256 in
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
    ; ( "-sample"
      , Arg.Set_int sample
      , (sprintf " Byte size of file samples to use. Default: %d" !sample)
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
  assert_
    (fun x -> x > 0)
    !sample
    (sprintf "Sample size cannot be negative: %d" !sample);
  { input  = !input
  ; output = !output
  ; ignore = !ignore
  ; sample = !sample
  }

let () =
  main (get_opt ())
