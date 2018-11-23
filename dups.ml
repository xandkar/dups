open Printf

module Array = ArrayLabels
module List  = ListLabels
module StrSet = Set.Make(String)
module Unix  = UnixLabels

module Metrics : sig
  type t

  val init
    : unit -> t
  val report
    : t
    -> time_all:float
    -> time_group_by_size:float
    -> time_group_by_head:float
    -> time_group_by_digest:float
    -> unit

  val file_considered
    : t -> size:int -> unit
  val file_ignored
    : t -> size:int -> unit
  val file_empty
    : t -> unit
  val file_sampled
    : t -> unit
  val chunk_read
    : t -> size:int -> unit
  val file_unique_size
    : t -> size:int -> unit
  val file_unique_sample
    : t -> size:int -> unit
  val file_hashed
    : t -> size:int -> unit
  val digest
    : t -> unit
end = struct
  type t =
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

  let init () =
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

  let add sum addend =
    sum := !sum + addend

  let file_considered t ~size =
    incr t.considered_files;
    add t.considered_bytes size

  let file_ignored {ignored_files; ignored_bytes; _} ~size =
    incr ignored_files;
    add ignored_bytes size

  let file_empty t =
    incr t.empty

  let chunk_read t ~size =
    add t.sampled_bytes size

  let file_sampled t =
    incr t.sampled_files

  let file_unique_size t ~size =
    incr t.unique_size_files;
    add t.unique_size_bytes size

  let file_unique_sample t ~size =
    incr t.unique_sample_files;
    add t.unique_sample_bytes size

  let file_hashed t ~size =
    incr t.hashed_files;
    add t.hashed_bytes size

  let digest t =
    incr t.digests

  let report
    t
    ~time_all
    ~time_group_by_size
    ~time_group_by_head
    ~time_group_by_digest
  =
    let b_to_mb b = (float_of_int b) /. 1024. /. 1024. in
    let b_to_gb b = (b_to_mb b) /. 1024. in
    eprintf "Time                         : %8.2f seconds\n%!"
      time_all;
    eprintf "Considered                   : %8d files  %6.2f Gb\n%!"
      !(t.considered_files)
      (b_to_gb !(t.considered_bytes));
    eprintf "Sampled                      : %8d files  %6.2f Gb\n%!"
      !(t.sampled_files)
      (b_to_gb !(t.sampled_bytes));
    eprintf "Hashed                       : %8d files  %6.2f Gb  %6.2f seconds\n%!"
      !(t.hashed_files)
      (b_to_gb !(t.hashed_bytes))
      time_group_by_digest;
    eprintf "Digests                      : %8d\n%!"
      !(t.digests);
    eprintf "Duplicates (Hashed - Digests): %8d\n%!"
      (!(t.hashed_files) - !(t.digests));
    eprintf "Skipped due to 0      size   : %8d files\n%!" !(t.empty);
    eprintf "Skipped due to unique size   : %8d files  %6.2f Gb  %6.2f seconds\n%!"
      !(t.unique_size_files)
      (b_to_gb !(t.unique_size_bytes))
      time_group_by_size;
    eprintf "Skipped due to unique sample : %8d files  %6.2f Gb  %6.2f seconds\n%!"
      !(t.unique_sample_files)
      (b_to_gb !(t.unique_sample_bytes))
      time_group_by_head;
    eprintf "Ignored due to regex match   : %8d files  %6.2f Gb\n%!"
      !(t.ignored_files)
      (b_to_gb !(t.ignored_bytes))
end

module M = Metrics

module Stream : sig
  type 'a t

  val create : (unit -> 'a option) -> 'a t

  val of_queue : 'a Queue.t -> 'a t

  val iter : 'a t -> f:('a -> unit) -> unit

  val map : 'a t -> f:('a -> 'b) -> 'b t

  val filter : 'a t -> f:('a -> bool) -> 'a t

  val concat : ('a t) list -> 'a t

  val group_by : 'a t -> f:('a -> 'b) -> ('b * int * 'a list) t
end = struct
  module S = Stream

  type 'a t =
    {mutable streams : ('a S.t) list}

  let create f =
    {streams = [S.from (fun _ -> f ())]}

  let of_queue q =
    create (fun () ->
      match Queue.take q with
      | exception Queue.Empty ->
          None
      | x ->
          Some x
    )

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

  let group_by t ~f =
    let groups_tbl = Hashtbl.create 1_000_000 in
    let group_update x =
      let group = f x in
      let members =
        match Hashtbl.find_opt groups_tbl group with
        | None ->
            (1, [x])
        | Some (n, xs) ->
            (succ n, x :: xs)
      in
      Hashtbl.replace groups_tbl group members
    in
    iter t ~f:group_update;
    let groups = Queue.create () in
    Hashtbl.iter
      (fun name (length, members) -> Queue.add (name, length, members) groups)
      groups_tbl;
    of_queue groups
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

  val filter_out_unique_sizes : t Stream.t -> metrics:M.t -> t Stream.t
  val filter_out_unique_heads : t Stream.t -> len:int -> metrics:M.t -> t Stream.t
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

  let filter_out_singletons files ~group ~handle_singleton =
    let q = Queue.create () in
    Stream.iter (Stream.group_by files ~f:group) ~f:(fun group ->
      let (_, n, members) = group in
      if n > 1 then
        List.iter members ~f:(fun m -> Queue.add m q)
      else
        handle_singleton group
    );
    Stream.of_queue q

  let filter_out_unique_sizes files ~metrics =
    filter_out_singletons
      files
      ~group:(fun {size; _} -> size)
      ~handle_singleton:(fun (size, _, _) -> M.file_unique_size metrics ~size)

  let head path ~len ~metrics =
    let buf = Bytes.make len ' ' in
    let ic = open_in_bin path in
    let rec read pos len =
      assert (len >= 0);
      if len = 0 then
        ()
      else begin
        let chunk_size = input ic buf pos len in
        M.chunk_read metrics ~size:chunk_size;
        if chunk_size = 0 then (* EOF *)
          ()
        else
          read (pos + chunk_size) (len - chunk_size)
      end
    in
    read 0 len;
    close_in ic;
    Bytes.to_string buf

  let filter_out_unique_heads files ~len ~metrics =
    filter_out_singletons
      files
      ~group:(fun {path; _} ->
        M.file_sampled metrics;
        head path ~len ~metrics
      )
      ~handle_singleton:(fun (_, _, files) ->
        let {size; _} = List.hd files in  (* Guaranteed non-empty *)
        M.file_unique_sample metrics ~size
      )
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

let make_input_stream input ignore ~metrics =
  let input =
    match input with
    | Stdin ->
        File.lookup (In_channel.lines stdin)
    | Directories paths ->
        let paths = StrSet.elements (StrSet.of_list paths) in
        Stream.concat (List.map paths ~f:File.find)
  in
  Stream.filter input ~f:(fun {File.path; size} ->
    M.file_considered metrics ~size;
    let empty = size = 0 in
    if empty then M.file_empty metrics;
    let ignored =
      match ignore with
      | Some regexp when (Str.string_match regexp path 0) ->
          M.file_ignored metrics ~size;
          true
      | Some _ | None ->
          false
    in
    (not empty) && (not ignored)
  )

let make_output_fun = function
  | Stdout ->
      fun digest n_files files ->
        printf "%s %d\n%!" (Digest.to_hex digest) n_files;
        List.iter files ~f:(fun {File.path; _} ->
          printf "    %S\n%!" path
        )
  | Directory dir ->
      fun digest _ files ->
        let digest = Digest.to_hex digest in
        let dir = Filename.concat dir (String.sub digest 0 2) in
        Unix.mkdir dir ~perm:0o700;
        let oc = open_out (Filename.concat dir digest) in
        List.iter files ~f:(fun {File.path; _} ->
          output_string oc (sprintf "%S\n%!" path)
        );
        close_out oc

let main {input; output; ignore; sample = sample_len} =
  let t0_all = Sys.time () in
  let metrics = M.init () in
  let output = make_output_fun  output in
  let input  = make_input_stream input ignore ~metrics in
  (* TODO: Make a nice(r) abstraction to re-assemble pieces in the pipeline:
   *
   * from input           to files_by_size
   * from files_by_size   to files_by_sample
   * from files_by_sample to files_by_digest
   * from files_by_digest to output
   *
   * input |> files_by_size |> files_by_sample |> files_by_digest |> output
   *)

  let files = input in

  let t0_group_by_size = Sys.time () in
  let files = File.filter_out_unique_sizes files ~metrics in
  let t1_group_by_size = Sys.time () in

  let t0_group_by_sample = t1_group_by_size in
  let files = File.filter_out_unique_heads files ~len:sample_len ~metrics in
  let t1_group_by_sample = Sys.time () in

  let t0_group_by_digest = t1_group_by_sample in
  let groups =
    Stream.group_by files ~f:(fun {File.path; size} ->
      M.file_hashed metrics ~size;
      Digest.file path
    )
  in
  let t1_group_by_digest = Sys.time () in

  Stream.iter groups ~f:(fun (d, n, files) ->
    M.digest metrics;
    if n > 1 then output d n files
  );

  let t1_all = Sys.time () in

  M.report metrics
    ~time_all:            (t1_all             -. t0_all)
    ~time_group_by_size:  (t1_group_by_size   -. t0_group_by_size)
    ~time_group_by_head:  (t1_group_by_sample -. t0_group_by_sample)
    ~time_group_by_digest:(t1_group_by_digest -. t0_group_by_digest)

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
