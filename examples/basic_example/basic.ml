(* An example program intended to show the basics of the API (spawn, monitor, send, receive).
   The program can be run on the same computer or distributed across different computers. 
   If you want to run across different computers then tweak the node configurations below
   to have the correct ip addresses and ports. (in a real program the configurations 
   would be externalized in a configuration file). To run the example start up the 
   consumer first then start the producer.

   The program simply has the producer spawning a couple of process on the consumer node
   then sending messages to the spawned processes and finally receiving the responses from
   the spawned processes for the messages that were sent. It also monitors for the spawned 
   processes for termination (normal or otherwise).     
*)

(* Define the message type that will be used to send messages between processes. *)
module M  = struct

  type t = Ping 
         | Pong
         | Var of [`End | `Noop]
         | Incr of (int -> int)
         | Incr_res of int
         | Raise

  exception Ping_ex         

  let string_of_message = function
    | Ping -> "Ping"
    | Pong -> "Pong"
    | Var `End -> "Var End"
    | Var `Noop -> "Var Noop"
    | Incr _ -> "Incr fn"
    | Incr_res i -> Format.sprintf "Incr result of %d" i 
    | Raise -> "Raise"  
end

(* create a Distributed_lwt implementation based on the message type defined above *)
module D = Distributed_lwt.Make (M)

(* The setup of the logger is left to the client, in this example the logger will
   simply log to stdout. The resultant logger is passed in as part of the 
   node configuration.
*)
let logger =
  Lwt_log.add_rule "*" Lwt_log.Fatal ; 
  Lwt_log.channel ~template:"$(date).$(milliseconds) {$(level)} : $(message)" ~close_mode:`Close ~channel:Lwt_io.stdout () 

let consumer_config = D.Remote { D.Remote_config.node_name = "consumer" ; 
                                 D.Remote_config.logger = logger ;   
                                 D.Remote_config.local_port = 47000 ;
                                 D.Remote_config.heart_beat_frequency = 5.0 ;
                                 D.Remote_config.heart_beat_timeout = 10.0 ;
                                 D.Remote_config.connection_backlog = 10 ;
                                 D.Remote_config.node_ip = "127.0.0.1" ;
                                 D.Remote_config.remote_nodes = [] ;
                               } 

let producer_config = D.Remote { D.Remote_config.node_name = "producer" ; 
                                 D.Remote_config.logger = logger ;
                                 D.Remote_config.local_port = 46000 ;
                                 D.Remote_config.heart_beat_frequency = 5.0 ;
                                 D.Remote_config.heart_beat_timeout = 10.0 ;
                                 D.Remote_config.connection_backlog = 10 ;
                                 D.Remote_config.node_ip = "127.0.0.1" ;
                                 D.Remote_config.remote_nodes = [("127.0.0.1",47000,"consumer")] ;
                               }  

let consumer_proc master_pid () = D.(
    (* The 'receive' function takes a list of matchers to use to process the incoming messages.
       A matcher can be created by using the 'case' function. The 'case' function takes 2 functions :
       a matching criteria function and a function to process the message if the criteria is satisfied.

       The other matcher creation function is 'termination_case' which is used in the producer below. 
    *)
    receive_loop [
      case (function 
          | M.Ping as v -> Some (fun () -> 
              send master_pid M.Pong >>= fun () -> 
              lift_io @@ Lwt_io.printlf "got message %s from remote node" @@ M.string_of_message v >>= fun () ->
              return true)
          | M.Incr incr_fn -> Some (fun () ->
              send master_pid (M.Incr_res (incr_fn 1)) >>= fun () -> 
              lift_io @@ Lwt_io.printlf "got message %s from remote node" @@ M.string_of_message (M.Incr incr_fn) >>= fun () ->
              return true)
          | M.Raise as v -> Some (fun () ->
              lift_io @@ Lwt_io.printlf "got message %s from remote node" @@ M.string_of_message v >>= fun () ->
              fail M.Ping_ex)
          | M.Var `End -> Some (fun () -> 
              lift_io @@ Lwt_io.printlf "got message %s from remote node" @@ M.string_of_message (M.Var `End) >>= fun () ->
              return false)
          | M.Var `Noop -> Some (fun () -> 
              lift_io @@ Lwt_io.printlf "got message %s from remote node" @@ M.string_of_message (M.Var `Noop) >>= fun () ->
              return true)          
          (*a catch all case, this is good to have otherwise the non-matching messages are just left in the order they came in the processes' mailbox *)
          | v -> Some (fun () -> 
              lift_io @@ Lwt_io.printlf "got unexpected message %s from remote node" @@ M.string_of_message v >>= fun () ->
              return true 
            )                
        )
    ] 
  ) 

let producer_proc () = D.(
    get_remote_nodes >>= fun nodes ->         (* get a list of currently connected remote nodes *)
    get_self_pid >>= fun pid_to_send_to ->    (* get the process id of the current process *)
    get_self_node >>= fun my_node ->          (* get our own node *) 

    (* Can lift operations from the underlying threading library, lwt is this case, into the process monad. *)
    (* spawn a process on the local node which will just print "hi.." then exit *)
    spawn my_node (fun () -> lift_io @@ Lwt_io.printl "hi from local process") >>= fun (_, _) -> 
    (* spawn and monitor a process on the remote node atomically *)
    spawn ~monitor:true (List.hd nodes) (consumer_proc pid_to_send_to) >>= fun (remote_pid1, _) -> 

    (* spawn and monitor a process on the remote node separately *)
    spawn (List.hd nodes) (consumer_proc pid_to_send_to) >>= fun (remote_pid2, _) -> 
    monitor remote_pid2 >>= fun _ ->

    (* send messages to the spawned remote processes, using the infix funtion '>!' alias of send *)
    remote_pid1 >! M.Ping >>= fun () ->
    remote_pid1 >! (M.Incr (fun i -> i + 5)) >>= fun () ->
    remote_pid1 >! (M.Var `Noop) >>= fun () ->
    remote_pid1 >! (M.Var `End) >>= fun () ->

    remote_pid2 >! M.Ping >>= fun () ->
    remote_pid2 >! (M.Incr (fun i -> i + 7)) >>= fun () ->
    remote_pid2 >! (M.Var `Noop) >>= fun () ->
    remote_pid2 >! (M.Raise) >>= fun () ->

    let processes_terminated = ref 0 in         

    (* process messages that are sent to us *)
    receive_loop [
      case (function 
          | M.Pong as v -> Some (fun () -> 
              lift_io @@ Lwt_io.printlf "got message %s from remote node" (M.string_of_message v) >>= fun () ->
              return true)
          | M.Incr_res r -> Some (fun () -> 
              lift_io @@ Lwt_io.printlf "got message %s from remote node" @@ M.string_of_message (M.Incr_res r) >>= fun () ->
              return true)
          | v -> Some (fun () -> 
              lift_io @@ Lwt_io.printlf "got unexpected message %s from remote node" (M.string_of_message v) >>= fun () ->
              return true) 
        ) ;  
      (* use the termination_case matcher to match against messages about the termination, either normal or exception, or previously monitored processes *)
      termination_case (function
          | Normal _ -> 
            processes_terminated := !processes_terminated + 1 ;
            lift_io (Lwt_io.printlf "remote process terminated successfully, number of remote processes terminated %d" !processes_terminated) >>= fun () ->                    
            return (!processes_terminated < 2)
          | Exception (_,ex) ->            
            if (Printexc.exn_slot_name ex) = (Printexc.exn_slot_name M.Ping_ex) (* work around inability to pattern match on unmarshalled exceptions, see http://caml.inria.fr/pub/docs/manual-ocaml/libref/Marshal.html*)
            then
              begin 
                processes_terminated := !processes_terminated + 1 ;
                lift_io (Lwt_io.printlf "remote process terminated with exception, number of remote processes terminated %d" !processes_terminated) >>= fun () ->
                return (!processes_terminated < 2)
              end
            else assert false
          | _ -> assert false 
        )       
    ] 
  )                                                                  

let () =
  let args = Sys.argv in
  if Array.length args <> 2
  then Format.printf "Usage : %s <producer/consumer>\n" args.(0)
  else if args.(1) = "producer" 
  then Lwt.(Lwt_main.run (D.run_node ~process:producer_proc producer_config >>= fun () -> fst @@ wait ()))    
  else if args.(1) = "consumer"
  (* no initial process is spawned on the consumer node, it will be spawned from the producer *)
  then Lwt.(Lwt_main.run (D.run_node consumer_config >>= fun () -> fst @@ wait ()))    
  else Format.printf "Usage : %s <master/worker>\n" args.(0)

