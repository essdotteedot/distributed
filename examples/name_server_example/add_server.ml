(* The add server holds the add process which will add two ints and
   send back the result to the requester. The goal is to make then
   server reslient to failures in the name_server.Arg

   main_proc
    - attempt to add the name server node, repeat until successful
    - spawn and monitor process_add_request, if process_add_request fails then
      the name server is added again an process_add_request is respawned

   process_add_request
    - attempt to register with the name server, if don't get okay in 0.5 raise
      Failed_to_register at whic point main_proc which is monitoring will restart
    - after successful registration enter a receive loop to process add messages
*)
module D = Distributed_lwt.Make(Message)

exception Failed_to_register 

let logger =
  Lwt_log.add_rule "*" Lwt_log.Fatal ; 
  Lwt_log.channel ~template:"$(date).$(milliseconds) {$(level)} : $(message)" ~close_mode:`Close ~channel:Lwt_io.stdout () 

let config = D.Remote { D.Remote_config.node_name = "add_server" ; 
                        D.Remote_config.logger = logger ;   
                        D.Remote_config.local_port = 46000 ;
                        D.Remote_config.heart_beat_frequency = 5.0 ;
                        D.Remote_config.heart_beat_timeout = 10.0 ;
                        D.Remote_config.connection_backlog = 10 ;
                        D.Remote_config.node_ip = "127.0.0.1" ;
                        D.Remote_config.remote_nodes = [] ; (* we will add nodes dynamically*)
                      } 

let process_add_request name_server_node = D.(
    get_self_pid >>= fun self_pid ->    
    lift_io (Lwt_io.printl "Add process is registering itself with the name server.") >>= fun () ->
    broadcast name_server_node (Message.Register ("add_process", self_pid)) >>= fun () ->
    receive ~timeout_duration:0.5 [
      case (function Message.Register_ok -> true | _ -> false)
        (fun _ -> return ())        
    ] >>= function
    | None ->
      lift_io (Lwt_io.printl "Add process failed to get ok response for registration request.") >>= fun () ->
      fail Failed_to_register
    | _ ->
      receive_loop [
        case (function Message.Add _ -> true | _ -> false)
          (function
            | Message.Add (x, y, requester_pid) ->
              requester_pid >! (Message.Add_result (x+y)) >>= fun () ->
              lift_io (Lwt_io.printlf "Successfully added %d and %d and sent back result." x y) >>= fun () ->
              return true
            | _ -> assert false
          ) ;    
        case (fun _ -> true)
          (fun m ->
             lift_io (Lwt_io.printlf "Add process ignoring message %s." (Message.string_of_message m)) >>= fun () -> 
             return true
          )
      ]
  )   

let rec main_proc () = D.(
    get_self_node >>= fun self_node_id ->
    catch 
      (fun () -> add_remote_node "127.0.0.1" 45000 "name_server")
      (fun _ ->
         lift_io (Lwt_io.printlf"Failed to add name server node, trying again in 1 second.") >>= fun () ->
         lift_io (Lwt_unix.sleep 1.0) >>= fun () ->
         main_proc ()      
      ) 
    >>= fun name_server_node_id ->
    spawn ~monitor:true self_node_id (process_add_request name_server_node_id) >>= fun _ ->        
    receive_loop [
      termination_case 
        (function
          | _ -> 
            lift_io (Lwt_io.printlf"Add process died, respawning it.") >>= fun () ->
            return false
        ) ;
      case (fun _ -> true)
        (fun _ -> return true)
    ] >>= fun _ ->
    main_proc ()
  )  

let () =
  Lwt_main.run (D.run_node ~process:(D.(main_proc () >>= fun _ -> return ())) config)

