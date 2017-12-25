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

  let lib_version = "0.5.0"

  let lib_description = "A Lwt based implementation."           

  let return = Lwt.return

  let (>>=) = Lwt.(>>=)

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

  let establish_server ?backlog sock_addr server_fn = Lwt_io.Versioned.establish_server_2 ?backlog sock_addr server_fn

  let of_lwt_level = function
    | Debug -> Lwt_log.Debug 
    | Info -> Lwt_log.Info
    | Notice -> Lwt_log.Notice
    | Warning -> Lwt_log.Warning
    | Error -> Lwt_log.Error
    | Fatal -> Lwt_log.Fatal

  let log ?exn ?location ~(logger:logger) ~(level:level) (msg:string) =
    Lwt_log.log ?exn ?location ~level:(of_lwt_level level) ~logger msg    

  let shutdown_server = Lwt_io.Versioned.shutdown_server_2

  let sleep = Lwt_unix.sleep

  let timeout = Lwt_unix.timeout

  let pick = Lwt.pick

  let at_exit = Lwt_main.at_exit

end

module Make(M : Distributed.Message_type) : (Distributed.Process with type 'a io = 'a Lwt.t and type message_type = M.t and type logger = Lwt_log.logger) =
  Distributed.Make(IO_LWT)(M)