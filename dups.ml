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
    -> wall_time_all:float
    -> wall_time_group_by_size:float
    -> wall_time_group_by_head:float
    -> wall_time_group_by_digest:float
    -> proc_time_all:float
    -> proc_time_group_by_size:float
    -> proc_time_group_by_head:float
    -> proc_time_group_by_digest:float
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
  val redundant_data
    : t -> size:int -> unit
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
    ; redundant_data      : int ref
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
    ; redundant_data      = ref 0
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

  let redundant_data t ~size =
    add t.redundant_data size

  let report
    t
    ~wall_time_all
    ~wall_time_group_by_size
    ~wall_time_group_by_head
    ~wall_time_group_by_digest
    ~proc_time_all
    ~proc_time_group_by_size
    ~proc_time_group_by_head
    ~proc_time_group_by_digest
  =
    let b_to_mb b = (float_of_int b) /. 1024. /. 1024. in
    let b_to_gb b = (b_to_mb b) /. 1024. in
    eprintf "Total time                   : %.2f wall sec  %.2f proc sec\n%!"
      wall_time_all
      proc_time_all;
    eprintf "Considered                   : %8d files  %6.2f Gb\n%!"
      !(t.considered_files)
      (b_to_gb !(t.considered_bytes));
    eprintf "Sampled                      : %8d files  %6.2f Gb\n%!"
      !(t.sampled_files)
      (b_to_gb !(t.sampled_bytes));
    eprintf "Hashed                       : %8d files  %6.2f Gb  %6.2f wall sec  %6.2f proc sec\n%!"
      !(t.hashed_files)
      (b_to_gb !(t.hashed_bytes))
      wall_time_group_by_digest
      proc_time_group_by_digest;
    eprintf "Digests                      : %8d\n%!"
      !(t.digests);
    eprintf "Duplicates (Hashed - Digests): %8d files  %6.2f Gb\n%!"
      (!(t.hashed_files) - !(t.digests))
      (b_to_gb !(t.redundant_data));
    eprintf "Skipped due to 0      size   : %8d files\n%!" !(t.empty);
    eprintf "Skipped due to unique size   : %8d files  %6.2f Gb  %6.2f wall sec  %6.2f proc sec\n%!"
      !(t.unique_size_files)
      (b_to_gb !(t.unique_size_bytes))
      wall_time_group_by_size
      proc_time_group_by_size;
    eprintf "Skipped due to unique sample : %8d files  %6.2f Gb  %6.2f wall sec  %6.2f proc sec\n%!"
      !(t.unique_sample_files)
      (b_to_gb !(t.unique_sample_bytes))
      wall_time_group_by_head
      proc_time_group_by_head;
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

  val bag_map : 'a t -> njobs:int -> f:('a -> 'b) -> ('a * 'b) t
  (** Parallel map with arbitrarily-reordered elements. *)

  val map : 'a t -> f:('a -> 'b) -> 'b t

  val filter : 'a t -> f:('a -> bool) -> 'a t

  val concat : ('a t) list -> 'a t

  val group_by : 'a t -> f:('a -> 'b) -> ('b * int * 'a list) t
end = struct
  module S = Stream

  type 'a t =
    {mutable streams : ('a S.t) list}

  type ('input, 'output) msg_from_vassal =
    | Ready of int
    | Result of (int * ('input * 'output))
    | Exiting of int

  type 'input msg_from_lord =
    | Job of 'input option

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

  module Ipc : sig
    val send : out_channel -> 'a -> unit
    val recv : in_channel -> 'a
  end = struct
    let send oc msg =
      Marshal.to_channel oc msg [];
      flush oc

    let recv ic =
      Marshal.from_channel ic
  end

  let lord t ~njobs ~vassals ~ic ~ocs =
    eprintf "[debug] [lord] started\n%!";
    let active_vassals = ref njobs in
    let results = Queue.create () in
    let rec dispatch () =
      match Ipc.recv ic with
      | ((Exiting i) : ('input, 'output) msg_from_vassal) ->
          close_out ocs.(i);
          decr active_vassals;
          if !active_vassals = 0 then
            ()
          else
            dispatch ()
      | ((Ready i) : ('input, 'output) msg_from_vassal) ->
          Ipc.send ocs.(i) (Job (next t));
          dispatch ()
      | ((Result (i, result)) : ('input, 'output) msg_from_vassal) ->
          Queue.add result results;
          Ipc.send ocs.(i) (Job (next t));
          dispatch ()
    in
    let rec wait = function
      | [] -> ()
      | vassals ->
          let pid, _process_status = Unix.wait () in
          (* TODO: handle process_status *)
          wait (List.filter vassals ~f:(fun p -> p <> pid))
    in
    dispatch ();
    close_in ic;
    wait vassals;
    of_queue results

  let vassal i ~f ~vassal_pipe_r ~lord_pipe_w =
    eprintf "[debug] [vassal %d] started\n%!" i;
    let ic = Unix.in_channel_of_descr vassal_pipe_r in
    let oc = Unix.out_channel_of_descr lord_pipe_w in
    let rec work msg =
      Ipc.send oc msg;
      match Ipc.recv ic with
      | (Job (Some x) : 'input msg_from_lord) ->
          work (Result (i, (x, f x)))
      | (Job None : 'input msg_from_lord) ->
          Ipc.send oc (Exiting i)
    in
    work (Ready i);
    close_in ic;
    close_out oc;
    exit 0

  let bag_map t ~njobs ~f =
    let lord_pipe_r, lord_pipe_w = Unix.pipe () in
    let vassal_pipes   = Array.init njobs ~f:(fun _ -> Unix.pipe ()) in
    let vassal_pipes_r = Array.map vassal_pipes ~f:(fun (r, _) -> r) in
    let vassal_pipes_w = Array.map vassal_pipes ~f:(fun (_, w) -> w) in
    let vassals = ref [] in
    for i=0 to (njobs - 1) do
      begin match Unix.fork () with
      | 0 ->
          Unix.close lord_pipe_r;
          vassal i ~f ~lord_pipe_w ~vassal_pipe_r:vassal_pipes_r.(i)
      | pid ->
          vassals := pid :: !vassals
      end
    done;
    Unix.close lord_pipe_w;
    lord
      t
      ~njobs
      ~vassals:!vassals
      ~ic:(Unix.in_channel_of_descr lord_pipe_r)
      ~ocs:(Array.map vassal_pipes_w ~f:Unix.out_channel_of_descr)
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

  val head : t -> len:int -> metrics:M.t -> string

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

  let head {path; _} ~len ~metrics =
    M.file_sampled metrics;
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
      ~group:(head ~len ~metrics)
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
  ; ignore : string -> bool
  ; sample : int
  ; njobs  : int
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
    let ignored = ignore path in
    if empty then M.file_empty metrics;
    if ignored then M.file_ignored metrics ~size;
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

let time_wall () =
  Unix.gettimeofday ()

let time_proc () =
  Sys.time ()

let main {input; output; ignore; sample = sample_len; njobs} =
  let wt0_all = time_wall () in
  let pt0_all = time_proc () in
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

  let wt0_group_by_size = time_wall () in
  let pt0_group_by_size = time_proc () in
  eprintf "[debug] filtering-out files with unique size\n%!";
  let files = File.filter_out_unique_sizes files ~metrics in
  let pt1_group_by_size = time_proc () in
  let wt1_group_by_size = time_wall () in

  let wt0_group_by_sample = wt1_group_by_size in
  let pt0_group_by_sample = pt1_group_by_size in
  eprintf "[debug] filtering-out files with unique heads\n%!";
  let files =
    if njobs > 1 then begin
      let q = Queue.create () in
      files
        |> Stream.bag_map ~njobs ~f:(File.head ~len:sample_len ~metrics)
        |> Stream.group_by ~f:snd
        |> Stream.map ~f:(fun (d, n, pairs) -> (d, n, List.map pairs ~f:fst))
        |> Stream.filter ~f:(fun (_, n, _) -> n > 1)
        |> Stream.iter ~f:(fun (_, _, fs) -> List.iter fs ~f:(fun f -> Queue.add f q))
        ;
      Stream.of_queue q
    end else
      File.filter_out_unique_heads files ~len:sample_len ~metrics
  in
  let pt1_group_by_sample = time_proc () in
  let wt1_group_by_sample = time_wall () in

  let wt0_group_by_digest = wt1_group_by_sample in
  let pt0_group_by_digest = pt1_group_by_sample in
  eprintf "[debug] hashing\n%!";
  let groups =
    if njobs > 1 then
      let with_digests =
        Stream.bag_map files ~njobs ~f:(fun {File.path; _} -> Digest.file path)
      in
      Stream.map (Stream.group_by with_digests ~f:snd) ~f:(
        fun (digest, n, file_digest_pairs) ->
          let files =
            List.map file_digest_pairs ~f:(fun (file, _) ->
              M.file_hashed metrics ~size:file.File.size;
              file
            )
          in
          (digest, n, files)
      )
    else
      Stream.group_by files ~f:(fun {File.path; size} ->
        M.file_hashed metrics ~size;
        Digest.file path
      )
  in
  let pt1_group_by_digest = time_proc () in
  let wt1_group_by_digest = time_wall () in

  eprintf "[debug] reporting\n%!";
  Stream.iter groups ~f:(fun (d, n, files) ->
    M.digest metrics;
    if n > 1 then
      M.redundant_data metrics ~size:(n * (List.hd files).File.size);
      output d n files
  );

  let pt1_all = time_proc () in
  let wt1_all = time_wall () in

  M.report metrics
    ~wall_time_all:            (wt1_all             -. wt0_all)
    ~wall_time_group_by_size:  (wt1_group_by_size   -. wt0_group_by_size)
    ~wall_time_group_by_head:  (wt1_group_by_sample -. wt0_group_by_sample)
    ~wall_time_group_by_digest:(wt1_group_by_digest -. wt0_group_by_digest)
    ~proc_time_all:            (pt1_all             -. pt0_all)
    ~proc_time_group_by_size:  (pt1_group_by_size   -. pt0_group_by_size)
    ~proc_time_group_by_head:  (pt1_group_by_sample -. pt0_group_by_sample)
    ~proc_time_group_by_digest:(pt1_group_by_digest -. pt0_group_by_digest)

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
  let ignore = ref (fun _ -> false) in
  let sample = ref 512 in
  let njobs  = ref 6 in
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
      , Arg.String (fun regexp ->
          let regexp = Str.regexp regexp in
          ignore := fun string -> Str.string_match regexp string 0)
      , " Ignore file paths which match this regexp pattern (see Str module)."
      )
    ; ( "-sample"
      , Arg.Set_int sample
      , (sprintf " Byte size of file samples to use. Default: %d" !sample)
      )
    ; ( "-j"
      , Arg.Set_int njobs
      , (sprintf " Number of parallel jobs. Default: %d" !njobs)
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
  ; njobs  = !njobs
  }

let () =
  main (get_opt ())
