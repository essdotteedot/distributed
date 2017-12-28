let log_src = Logs.Src.create "distributed" ~doc:"logs events related to the distributed library"

module Log = (val Logs_lwt.src_log log_src : Logs_lwt.LOG)

let msg = Log.msg

(* slightly modified version of reporter defined in Logs_lwt manual : http://erratique.ch/software/logs/doc/Logs_lwt.html#report_ex*)
let lwt_reporter () =
  let buf_fmt () =
    let b = Buffer.create 512 in
    Format.formatter_of_buffer b, fun () -> let m = Buffer.contents b in Buffer.reset b; m
  in
  let app, app_flush = buf_fmt () in
  let reporter = Logs.format_reporter ~app ~dst:app () in
  let report src level ~over k msgf =
    let k' () =
      let write () = Lwt_io.write Lwt_io.stdout (app_flush ()) in
      let unblock () = over (); Lwt.return_unit in
      Lwt.finalize write unblock |> Lwt.ignore_result;
      k ()
    in
    reporter.Logs.report src level ~over:(fun () -> ()) k' msgf;
  in
  { Logs.report = report }  