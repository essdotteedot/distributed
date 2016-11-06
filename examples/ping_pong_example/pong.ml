module D = Distributed_lwt.Make (Ping_message)

let logger =
  Lwt_log.add_rule "*" Lwt_log.Fatal ; 
  Lwt_log.channel ~template:"$(date).$(milliseconds) {$(level)} : $(message)" ~close_mode:`Close ~channel:Lwt_io.stdout () 

let config = D.Remote { D.Remote_config.node_name = "pong_node" ; 
                        D.Remote_config.logger = logger ;
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
      case (function Ping_message.Ping _ -> true | _ -> false) 
        (function
          | Ping_message.Ping s -> 
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
            end
          | _ -> assert false
        ) ;
      case (fun _ -> true) 
        (fun v -> 
           lift_io (Lwt_io.printl @@ Format.sprintf "Got unexpected message %s" (Ping_message.string_of_message v)) >>= fun () ->
           assert false
        ) 
    ] 
  )

let () =
  Lwt.(Lwt_main.run ((D.run_node ~process:pong config) >>= fun () -> fst @@ wait ()))    