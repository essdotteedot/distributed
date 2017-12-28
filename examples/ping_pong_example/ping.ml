module D = Distributed_lwt.Make (Ping_message) (Custom_logger)

let config = D.Remote { D.Remote_config.node_name = "ping_node" ; 
                        D.Remote_config.local_port = 46000 ;
                        D.Remote_config.heart_beat_frequency = 5.0 ;
                        D.Remote_config.heart_beat_timeout = 10.0 ;
                        D.Remote_config.connection_backlog = 10 ;
                        D.Remote_config.node_ip = "127.0.0.1" ;
                        D.Remote_config.remote_nodes = [("127.0.0.1",47000,"pong_node")] ;
                      }                        

let rec ping_loop (counter : int) () = D.(
    get_remote_node "pong_node" >>= function
    | None -> lift_io (Lwt_io.printl "Remote node pong is not up, exiting")
    | Some node' ->         
      broadcast node' (Ping_message.Ping (string_of_int counter)) >>= fun () ->
      receive [
        case (function 
            | Ping_message.Pong s -> Some (fun () -> lift_io (Lwt_io.printl @@ Format.sprintf "Got message Pong %s" s))
            | v -> Some (fun () ->
                lift_io (Lwt_io.printl @@ Format.sprintf "Got unexpected message %s" (Ping_message.string_of_message v)) >>= fun () ->
                assert false)
          )  
      ] >>= fun _ ->    
      lift_io (Lwt_unix.sleep 1.0) >>= fun () ->  
      ping_loop (counter + 1) ()
  )

let () =
  Logs.Src.set_level Custom_logger.log_src (Some Logs.App) ;
  Logs.set_reporter @@ Custom_logger.lwt_reporter () ;
  Lwt.(Lwt_main.run (D.run_node ~process:(ping_loop(0)) config >>= fun () -> fst @@ wait ()))    