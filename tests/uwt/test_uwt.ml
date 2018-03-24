let log_src = Logs.Src.create "distributed" ~doc:"logs events related to the distributed library"

module Log = (val Logs_lwt.src_log log_src : Logs_lwt.LOG)

module Test_io = struct    

  type 'a t = 'a Lwt.t

  type 'a stream = 'a Lwt_stream.t

  type input_channel = Uwt_io.input_channel

  type output_channel = Uwt_io.output_channel

  type server = Uwt_io.server

  type level = Debug 
              | Info
              | Warning
              | Error             

  exception Timeout = Uwt_compat.Lwt_unix.Timeout

  let established_connections : int ref = ref 0

  let exit_fns : (unit -> unit Lwt.t) list ref = ref []

  let lib_name = "Test_uwt_io"

  let lib_version = "%%VERSION_NUM%%"

  let lib_description = "A Uwt based test implementation that uses for testing purposes"              

  let return = Lwt.return

  let (>>=) = Lwt.(>>=)

  let fail = Lwt.fail    

  let catch = Lwt.catch

  let async = Lwt.async

  let create_stream = Lwt_stream.create

  let get = Lwt_stream.get        

  let stream_append = Lwt_stream.append            

  let close_input = Uwt_io.close

  let close_output = Uwt_io.close    

  let read_value = Uwt_io.read_value

  let write_value = Uwt_io.write_value

  let of_logs_lwt_level = function
    | Debug -> Logs.Debug 
    | Info -> Logs.Info
    | Warning -> Logs.Warning
    | Error -> Logs.Error    
  
  let log (level:level) (msg_fmtter:unit -> string) =
    Log.msg (of_logs_lwt_level level) (fun m -> m "%s" @@ msg_fmtter ()) >>= fun _ -> return ()     

  let open_connection sock_addr = Uwt_io.open_connection sock_addr

  let establish_server ?backlog sock_addr server_fn = 
    Uwt_io.establish_server_with_client_address ?backlog sock_addr server_fn >>= fun server ->
    established_connections := !established_connections + 1 ;
    Lwt.return server

  let shutdown_server = Uwt_io.shutdown_server

  let sleep = Uwt_compat.Lwt_unix.sleep

  let timeout = Uwt_compat.Lwt_unix.timeout

  let pick = Lwt.pick

  let at_exit f = exit_fns := f::!exit_fns

  let run_fn  = Uwt.Main.run  

  let get_established_connection_count () = !established_connections

  let reset_established_connection_count () = established_connections := 0

  let get_atexit_fns () = !exit_fns

  let clear_atexit_fnns () = exit_fns := []
  
end

(* slightly modified version of reporter defined in Logs_lwt manual : http://erratique.ch/software/logs/doc/Logs_lwt.html#report_ex*)
let lwt_reporter log_it =
  let buf_fmt () =
    let b = Buffer.create 512 in
    Format.formatter_of_buffer b, fun () -> let m = Buffer.contents b in Buffer.reset b; m
  in
  let app, app_flush = buf_fmt () in
  let reporter = Logs.format_reporter ~app ~dst:app () in
  let report src level ~over k msgf =
    let k' () =
      let write () = log_it @@ app_flush () in
      let unblock () = over (); Lwt.return_unit in
      Lwt.finalize write unblock |> Lwt.ignore_result;
      k ()
    in
    reporter.Logs.report src level ~over:(fun () -> ()) k' msgf;
  in
  { Logs.report = report }  

let log_it_quiet _ = Lwt.return ()

let log_to_stdout = Uwt_io.write Uwt_io.stdout

let () =
  let module Tests = Test_distributed.Make(Test_io) in
  let logger = log_it_quiet in
  Logs.Src.set_level log_src (Some Logs.Debug) ;
  Logs.set_reporter @@ lwt_reporter logger ;
  Tests.run_suite ()