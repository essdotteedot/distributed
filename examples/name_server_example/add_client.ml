(* The add client will add the name server node dynamically at startup,
   then spawn and monitor a local process to add two random number between
   0 and 100 infinitely. 

   The idea is to make the add process resilient to failures of the add server
   remote server and the name server. 

   main main_proc
    - spawns and monitors find_remote_process, if it goes down then the main process
       will try to add the name server again and restart the find_remote_process again
   find_remote_process
    - looks up the add remote process, if it failes raises Failed_to_lookup which
      causes a restart from main process which is monitoring it 
    - attempts to add the add_server node, again a failure will result in a restart from
      the main process
    - spawns and monitors the add_forever process, if the add_forever fails then then
      remote add process is looked up via the node server and add_forever is restarted with
      a potentially new pid for the new remote add process. If the lookup fails then the main 
      process will restart the entire process since it's monitoring find_remote_process
   add_forever
     - picks two random ints <= 100 and asks the remote add process to add them
     - if the result isn't received in 0.5 seconds then it exits and which point find_remote_process
       which is monitoring it will restart with
     - otherwise it loops back and adds more ints   
*)

module D = Distributed_lwt.Make(Message)

exception Failed_to_lookup

let logger =
  Lwt_log.add_rule "*" Lwt_log.Fatal ; 
  Lwt_log.channel ~template:"$(date).$(milliseconds) {$(level)} : $(message)" ~close_mode:`Close ~channel:Lwt_io.stdout () 

let config = D.Remote { D.Remote_config.node_name = "add_client" ; 
                        D.Remote_config.logger = logger ;   
                        D.Remote_config.local_port = 47000 ;
                        D.Remote_config.heart_beat_frequency = 5.0 ;
                        D.Remote_config.heart_beat_timeout = 10.0 ;
                        D.Remote_config.connection_backlog = 10 ;
                        D.Remote_config.node_ip = "127.0.0.1" ;
                        D.Remote_config.remote_nodes = [] ; (* we will add nodes dynamically*)
                      } 

let rec add_forever add_pid = D.(
    get_self_pid >>= fun self_pid ->
    let x = Random.int 100 in
    let y = Random.int 100 in
    add_pid >! (Message.Add (x, y, self_pid)) >>= fun () ->
    receive ~timeout_duration:0.5 [
      case (function Message.Add_result _ -> true | _ -> false)
        (function
          | Message.Add_result r ->
            lift_io (Lwt_io.printlf "Sucessfully added %d and %d, result of %d." x y r)             
          | _ -> assert false
        )            
    ] >>= function
    | None -> 
      lift_io (Lwt_io.printlf "Failed to result in time for add request of %d + %d." x y)
    | _ -> 
      lift_io (Lwt_unix.sleep 0.5) >>= fun () ->
      add_forever add_pid
  )

let rec find_remote_process add_node = D.(
    get_self_pid >>= fun self_pid ->
    broadcast add_node (Message.Whois ("add_process", self_pid)) >>= fun () ->    
    receive ~timeout_duration:0.5 [
      case (function Message.Whois_result _ -> true | _ -> false)
        (function 
          | Message.Whois_result add_pid -> return add_pid          
          | _ -> assert false
        )         
    ] >>= function
    | None ->
      lift_io (Lwt_io.printl "Failed to lookup remote add process in time.") >>= fun () ->
      fail Failed_to_lookup
    | Some add_pid ->
      lift_io (Lwt_io.printl "Successfully looked up remote add process.") >>= fun () ->
      lift_io (Lwt_io.printl "Attempting to add to remote add node.") >>= fun () ->
      add_remote_node "127.0.0.1" 46000 "add_server" >>= fun _ ->
      lift_io (Lwt_io.printl "Successfully added remote add node.") >>= fun () ->
      get_self_node >>= fun self_node ->
      spawn ~monitor:true self_node (add_forever add_pid) >>= fun _ ->
      receive_loop [
        termination_case 
          (function
            | _ -> lift_io (Lwt_io.printl "Add process died, querying for remote add process id then respawning.") >>= fun () ->
              lift_io (Lwt_unix.sleep 1.0) >>= fun () ->
              return false
          ) ;
        case (fun _ -> true)
          (fun _ -> return true)
      ] >>= fun _ ->      
      find_remote_process add_node
  )   

let rec main_proc () = D.(
    get_self_node >>= fun self_node_id ->
    (* adding a node that already exists is a no-op, but in case the name server went down it would have been
       removed automatically from the list of nodes that we are connected to remotely.
    *)
    catch 
      (fun () -> add_remote_node "127.0.0.1" 45000 "name_server")
      (fun _ ->
         lift_io (Lwt_io.printl "Failed to add name server node, trying again in 1 second.") >>= fun () ->
         lift_io (Lwt_unix.sleep 1.0) >>= fun () ->
         main_proc ()      
      ) 
    >>= fun name_server_node_id ->
    spawn ~monitor:true self_node_id (find_remote_process name_server_node_id) >>= fun _ ->        
    receive_loop [
      termination_case 
        (function
          | _ -> 
            lift_io (Lwt_io.printl "Add process died, respawning it") >>= fun () ->
            return false          
        ) ;
      case (fun _ -> true)
        (fun _ -> return true)
    ] >>= fun _ ->
    main_proc ()
  )  

let () =
  Lwt_main.run (D.run_node ~process:(D.(main_proc () >>= fun _ -> return ())) config)

