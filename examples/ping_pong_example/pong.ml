module D = Distributed_lwt.Make (Ping_message) (Custom_logger)

let config = D.Remote { D.Remote_config.node_name = "pong_node" ; 
                        D.Remote_config.local_port = 47000 ;
                        D.Remote_config.heart_beat_frequency = 5.0 ;
                        D.Remote_config.heart_beat_timeout = 10.0 ;
                        D.Remote_config.connection_backlog = 10 ;
                        D.Remote_config.node_ip = "127.0.0.1" ;
                        D.Remote_config.remote_nodes = [] ;
                      }  

let counter = ref 0

let pong () = D.(
    receive_loop [
      case (function
          | Ping_message.Ping s -> Some (fun () -> 
              lift_io (Lwt_io.printl @@ Format.sprintf "Got message Ping %s" s) >>= fun () ->
              get_remote_node "ping_node" >>= 
              begin 
                function
                | None -> 
                  lift_io (Lwt_io.printl "Remote node ping is not up, exiting") >>= fun () -> 
                  return false
                | Some rnode ->
                  broadcast rnode (Ping_message.Pong (string_of_int !counter)) >>= fun () ->
                  counter := !counter + 1 ;
                  return true
              end)
          | v -> Some (fun () -> 
              lift_io (Lwt_io.printl @@ Format.sprintf "Got unexpected message %s" (Ping_message.string_of_message v)) >>= fun () ->
              assert false)
        ) 
    ] 
  )

let () =
  Logs.Src.set_level Custom_logger.log_src (Some Logs.App) ;
  Logs.set_reporter @@ Custom_logger.lwt_reporter () ;
  Lwt.(Lwt_main.run ((D.run_node ~process:pong config) >>= fun () -> fst @@ wait ()))    