module IO_LWT = struct

  type 'a t = 'a Lwt.t

  type 'a stream = 'a Lwt_stream.t

  type input_channel = Lwt_io.input_channel

  type output_channel = Lwt_io.output_channel

  type server = Lwt_io.server

  type logger = Lwt_log.logger

  type level = Debug 
             | Info
             | Notice
             | Warning
             | Error
             | Fatal

  exception Timeout = Lwt_unix.Timeout  

  let lib_name = "Distributed_lwt"

  let lib_version = "0.2.0"

  let lib_description = "A Lwt based implementation."           

  let return = Lwt.return

  let (>>=) = Lwt.(>>=)

  let ignore_result = Lwt.ignore_result

  let fail = Lwt.fail    

  let catch = Lwt.catch

  let async = Lwt.async

  let create_stream = Lwt_stream.create

  let get = Lwt_stream.get        

  let stream_append = Lwt_stream.append            

  let close_input = Lwt_io.close

  let close_output = Lwt_io.close    

  let read_value = Lwt_io.read_value

  let write_value = Lwt_io.write_value   

  let open_connection sock_addr = Lwt_io.open_connection sock_addr

  let establish_server ?backlog sock_addr server_fn = Lwt_io.establish_server ?backlog sock_addr server_fn

  let of_lwt_level = function
    | Debug -> Lwt_log.Debug 
    | Info -> Lwt_log.Info
    | Notice -> Lwt_log.Notice
    | Warning -> Lwt_log.Warning
    | Error -> Lwt_log.Error
    | Fatal -> Lwt_log.Fatal

  let log ?exn ?location ~(logger:logger) ~(level:level) (msg:string) =
    Lwt_log.log ?exn ?location ~level:(of_lwt_level level) ~logger msg    

  let shutdown_server = Lwt_io.shutdown_server

  let sleep = Lwt_unix.sleep

  let timeout = Lwt_unix.timeout

  let choose = Lwt.choose

  let pick = Lwt.pick

  let nchoose = Lwt.nchoose

  let at_exit = Lwt_main.at_exit

end

module Make(M : Distributed.Message_type) : (Distributed.Process.S with type 'a io = 'a Lwt.t and type message_type = M.t and type logger = Lwt_log.logger) =
  Distributed.Process.Make(IO_LWT)(M)