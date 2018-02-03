(*BISECT-IGNORE-BEGIN*)
open OUnit

exception Test_ex

type server_handler = Unix.sockaddr -> (Lwt_io.input_channel * Lwt_io.output_channel) -> unit Lwt.t

let established_connections : (Unix.sockaddr,(Lwt_io.input_channel * Lwt_io.output_channel) list * server_handler) Hashtbl.t = Hashtbl.create 10

let exit_fn : (unit -> unit Lwt.t) option ref = ref None 

let log_src = Logs.Src.create "distributed" ~doc:"logs events related to the distributed library"

module Log = (val Logs_lwt.src_log log_src : Logs_lwt.LOG)

let get_option (v : 'a option) : 'a = 
  match v with
  | None -> assert false
  | Some v' -> v'

let pp_list ?first ?last ?sep (items : 'a list) (string_of_item : 'a -> string) : string =
  let buff = Buffer.create 100 in
  if first <> None then Buffer.add_string buff (get_option first) else () ;        
  List.iter 
    (fun i -> 
       Buffer.add_string buff @@ string_of_item i ;
       if sep <> None then Buffer.add_string buff (get_option sep) else ()
    ) 
    items ;    
  if last <> None then Buffer.add_string buff (get_option last) else () ;
  Buffer.contents buff

let hashtbl_keys (table : ('a,'b) Hashtbl.t) : 'a list =
  Hashtbl.fold (fun k _ acc -> k::acc) table []    

module Test_io = struct    

  type 'a t = 'a Lwt.t

  type 'a stream = 'a Lwt_stream.t

  type input_channel = Lwt_io.input_channel

  type output_channel = Lwt_io.output_channel

  type server = unit

  type level = Debug 
             | Info
             | Warning
             | Error             

  exception Timeout = Lwt_unix.Timeout

  let lib_name = "Test_io"

  let lib_version = "%%VERSION_NUM%%"

  let lib_description = "A Lwt based implementation that uses pipes instead of sockets for testing purposes"              

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

  let of_logs_lwt_level = function
    | Debug -> Logs.Debug 
    | Info -> Logs.Info
    | Warning -> Logs.Warning
    | Error -> Logs.Error    
  
  let sock_addr_of_client () =
      (* bit of test hackery, the client nodes that call open_connection are always 1.2.3.4:100 in the tests,
         we need to pass this address on to the server loop function
      *)
      Unix.ADDR_INET (Unix.inet_addr_of_string "1.2.3.4",100)

  let log (level:level) (msg_fmtter:unit -> string) =
    Log.msg (of_logs_lwt_level level) (fun m -> m "%s" @@ msg_fmtter ()) >>= fun _ -> return ()     

  let string_of_sock_addr = function
    | Unix.ADDR_UNIX s -> s
    | Unix.ADDR_INET (inet_addr,port) -> Format.sprintf "%s:%d" (Unix.string_of_inet_addr inet_addr) port       

  let open_connection sock_addr =
    log Debug (fun () -> Format.sprintf "opening connection to %s"  (string_of_sock_addr sock_addr)) >>= fun _ ->
    log Debug 
      (fun () -> Format.sprintf "current establised connections : %s" 
         (
           let keys : Unix.sockaddr list = hashtbl_keys established_connections in
           pp_list ~first:"[" ~last:"]" ~sep:"," keys (fun v -> string_of_sock_addr v)             
         )
      ) >>= fun _ -> 
    let (conns,server_fn) = Hashtbl.find established_connections sock_addr in    
    let new_in_ch0,new_out_ch0 = Lwt_io.pipe () in
    let new_in_ch1,new_out_ch1 = Lwt_io.pipe () in    
    Hashtbl.replace established_connections sock_addr ((new_in_ch1,new_out_ch1)::(new_in_ch0,new_out_ch0)::conns,server_fn) ; 
    log Debug (fun () -> Format.sprintf "opened connection to %s" (string_of_sock_addr sock_addr)) >>= fun _ ->
    async @@ (fun () -> server_fn (sock_addr_of_client ()) (new_in_ch0,new_out_ch1)) ;
    return (new_in_ch1,new_out_ch0)

  let establish_server ?backlog sock_addr server_fn : server t =    
    log Debug (fun () -> Format.sprintf "establised connection for %s" (string_of_sock_addr sock_addr)) >>= fun _ ->
    Hashtbl.replace established_connections sock_addr ([],server_fn) ;
    log Debug 
      (fun () -> Format.sprintf "current establised connections : %s" 
        (let keys : Unix.sockaddr list = hashtbl_keys established_connections in
         pp_list ~first:"[" ~last:"]" ~sep:"," keys (fun v -> string_of_sock_addr v) 
        )
      )    

  let shutdown_server (s : server) : unit t = return ()

  let sleep = Lwt_unix.sleep

  let timeout = Lwt_unix.timeout

  let pick = Lwt.pick

  let at_exit f = exit_fn := Some f 

end

module M  = struct
  type t = string

  let string_of_message m = m  
end

let pipe_close p =
  Lwt.catch (fun () -> Lwt_io.close p) (fun _ -> Lwt.return ())

let rec close_pipes = function
  | [] -> Lwt.return ()
  | (in_ch,out_ch)::chs -> Lwt.(pipe_close in_ch >>= fun () -> pipe_close out_ch >>= fun () -> close_pipes chs)

let test_run_wrapper lwt_exp =
  Lwt.(Lwt_main.run (
        catch (fun () -> 
          Hashtbl.fold (fun _ (pipes,_) _ -> close_pipes pipes >>= fun () -> return ()) established_connections (return ()) >>= fun () ->
          Hashtbl.clear established_connections ;
          lwt_exp ()
        ) 
        (fun e -> assert_failure @@ Format.sprintf "Test encountered exception %s, backtrace : %s" (Printexc.to_string e) (Printexc.get_backtrace ()))))

(* there is a lot of code duplication because ocaml currently does not support higher kinded polymorphism *)  

(* return, bind test *)

let test_return_bind _ =
  let module P = Distributed.Make (Test_io) (M) in      
  let node_config = P.Local {P.Local_config.node_name = "test" ;} in
  let result = ref None in
  let test_proc () = P.(
      return 5 >>= fun i ->
      result := Some i ;
      return ()       
    ) in             
  test_run_wrapper (fun () -> P.run_node node_config ~process:test_proc) ;
  assert_equal ~msg:"return, bind failed" (Some 5) !result ;     
  assert_equal ~msg:"local config should hav establised 0 connections" 0 (Hashtbl.length established_connections)

(* spawn, spawn monitor tests for local and remote configurations *)

let test_spawn_local_local_config _ =
  let module P = Distributed.Make (Test_io) (M) in
  let node_config = P.Local {P.Local_config.node_name = "test" ;} in
  let result = ref false in
  let mres = ref None in
  let test_proc () = P.(                    
      get_self_node >>= fun local_node ->
      assert_bool "process should not have spawned yet" (not !result) ;
      spawn local_node (fun () -> return () >>= fun _ -> return (result := true)) >>= fun (_, mon_res) ->
      mres := mon_res ;
      return ()        
    ) in             
  test_run_wrapper (fun () -> P.run_node node_config ~process:test_proc) ;
  assert_equal ~msg:"monitor result should have been None" None !mres ;
  assert_bool "process was not spawned" !result ;     
  assert_equal ~msg:"local config should hav establised 0 connections" 0 (Hashtbl.length established_connections)

let test_spawn_local_remote_config _ =
  let module P = Distributed.Make (Test_io) (M) in  
  let node_config = P.Remote { P.Remote_config.node_name = "producer" ; 
                               P.Remote_config.local_port = 100 ;
                               P.Remote_config.heart_beat_frequency = 2.0 ;
                               P.Remote_config.heart_beat_timeout = 5.0 ;
                               P.Remote_config.connection_backlog = 10 ;
                               P.Remote_config.node_ip = "1.2.3.4" ;
                               P.Remote_config.remote_nodes = [] ;
                             } in
  let result = ref false in
  let mres = ref None in
  let test_proc () = P.(        
      get_self_node >>= fun local_node ->
      assert_bool "process should not have spawned yet" (not !result) ;
      spawn local_node (fun () -> return () >>= fun _ -> result := true ; return ()) >>= fun (_, mon_res) ->
      mres := mon_res ;      
      return ()        
    ) in             
  Lwt.(
    test_run_wrapper (fun () -> 
      P.run_node node_config ~process:test_proc >>= fun () -> 
      (get_option !exit_fn) () >>= fun () ->
      assert_bool "process was not spawned" !result ;
      assert_equal ~msg:"monitor result should have been none" None !mres ;    
      assert_equal ~msg:"remote config with only a single node should have establised 1 connection" 1 (Hashtbl.length established_connections) ;  
      
      return ()
    )
  )

let test_spawn_remote_remote_config _ =
  let module Producer = Distributed.Make (Test_io) (M) in
  let module Consumer = Distributed.Make (Test_io) (M) in  
  let node_config = Producer.Remote { Producer.Remote_config.node_name = "producer" ; 
                                      Producer.Remote_config.local_port = 100 ;
                                      Producer.Remote_config.heart_beat_frequency = 200.0 ;
                                      Producer.Remote_config.heart_beat_timeout = 500.0 ;
                                      Producer.Remote_config.connection_backlog = 10 ;
                                      Producer.Remote_config.node_ip = "1.2.3.4" ;
                                      Producer.Remote_config.remote_nodes = [("5.6.7.8",101,"consumer")] ;
                                    } in
  let remote_config = Consumer.Remote { Consumer.Remote_config.node_name = "consumer" ; 
                                        Consumer.Remote_config.local_port = 101 ;
                                        Consumer.Remote_config.heart_beat_frequency = 200.0 ;
                                        Consumer.Remote_config.heart_beat_timeout = 500.0 ;
                                        Consumer.Remote_config.connection_backlog = 10 ;
                                        Consumer.Remote_config.node_ip = "5.6.7.8" ;
                                        Consumer.Remote_config.remote_nodes = [] ;
                                      } in
  let spawn_res = ref None in
  let mres = ref None in                                  
  let consumer_proc () = Consumer.(
      (* a bit of test hackery to get around the fact that the node will establish a connection on the loop back address
         but we need an entry in the established_connections table with the external node address so that the producer can
         connect to it
      *)
      return () >>= fun _ ->
      return (
        let (conns,server_fn) = Hashtbl.find established_connections (Unix.ADDR_INET (Unix.inet6_addr_any , 101)) in
        Hashtbl.remove established_connections (Unix.ADDR_INET (Unix.inet6_addr_any , 101)) ;
        Hashtbl.replace established_connections (Unix.ADDR_INET (Unix.inet_addr_of_string "5.6.7.8" , 101)) (conns,server_fn) ;         
      ) 
    ) in    
  let producer_proc () = Producer.(
      return () >>= fun _ ->
      return (
        let (conns,server_fn) = Hashtbl.find established_connections (Unix.ADDR_INET (Unix.inet6_addr_any , 100)) in
        Hashtbl.remove established_connections (Unix.ADDR_INET (Unix.inet6_addr_any , 100)) ;
        Hashtbl.replace established_connections (Unix.ADDR_INET (Unix.inet_addr_of_string "1.2.3.4" , 100)) (conns,server_fn) ; 
      ) >>= fun _ -> 
      get_remote_nodes >>= fun nodes ->
      get_self_pid >>= fun pid_to_send_to ->
      assert_bool "process should not have spawned yet" (!spawn_res = None) ;
      spawn (List.hd nodes) (fun () -> Consumer.send pid_to_send_to "spawned") >>= fun (_, mon_res) ->      
      receive ~timeout_duration:0.05 [
        case (fun v -> Some (fun () -> return v))
      ] >>= fun msg ->
      spawn_res := msg ;
      mres := mon_res ;
      return ()                   
    ) in
  Lwt.(
    test_run_wrapper (fun () -> 
      Lwt.async (fun () -> Consumer.run_node remote_config ~process:consumer_proc) ;                
      Producer.run_node node_config ~process:producer_proc >>= fun () ->
      (get_option !exit_fn) () >>= fun () ->
      assert_equal ~msg:"remote config with 2 remote nodes should have establised 2 connections" 2 (Hashtbl.length established_connections) ;
      assert_equal ~msg:"process did not spawn" (Some "spawned") !spawn_res ;
      
      return () 
    ) 
  )       

let test_spawn_monitor_local_local_config _ =
  let module P = Distributed.Make (Test_io) (M) in
  let node_config = P.Local {P.Local_config.node_name = "test" ;} in
  let result = ref false in
  let result_monitor = ref None in
  let mres = ref None in
  let test_proc () = P.(                          
      get_self_node >>= fun local_node ->
      assert_bool "process should not have spawned yet" (not @@ !result || !result_monitor <> None) ;      
      spawn ~monitor:true local_node (fun () -> return () >>= fun _ -> return (result := true)) >>= fun (_, mon_res) ->
      receive [
        termination_case 
          (function
            | Normal _ -> return (result_monitor := Some "got normal termination")
            | _ -> assert false
          )
      ] >>= fun _ ->    
      mres := mon_res ;  
      return ()        
    ) in             
  Lwt.(test_run_wrapper (fun () -> P.run_node node_config ~process:test_proc >>= fun () -> (get_option !exit_fn) ())) ;
  assert_bool "process was not spawned" (!result && !result_monitor <> None) ;
  assert_equal ~msg:"termination monitor result not received" (Some "got normal termination") !result_monitor ;     
  assert_equal ~msg:"local config should hav establised 0 connections" 0 (Hashtbl.length established_connections) ;
  assert_bool "spawn monitor failed" (None <> !mres) ;
  assert_equal 0 (Hashtbl.length established_connections)    

let test_spawn_monitor_local_remote_config _ =
  let module P = Distributed.Make (Test_io) (M) in
  let node_config = P.Remote { P.Remote_config.node_name = "producer" ; 
                               P.Remote_config.local_port = 100 ;
                               P.Remote_config.heart_beat_frequency = 200.0 ;
                               P.Remote_config.heart_beat_timeout = 500.0 ;
                               P.Remote_config.connection_backlog = 10 ;
                               P.Remote_config.node_ip = "1.2.3.4" ;
                               P.Remote_config.remote_nodes = [] ;
                             } in
  let result = ref false in
  let result_monitor = ref None in
  let mres = ref None in
  let test_proc () = P.(                          
      get_self_node >>= fun local_node ->
      assert_bool "Process should not have spawned yet" (not @@ !result || !result_monitor <> None) ;      
      spawn ~monitor:true local_node (fun () -> return () >>= fun _ -> return (result := true)) >>= fun (_, mon_res) ->
      receive [
        termination_case 
          (function
            | Normal _ -> return (result_monitor := Some "got normal termination")
            | _ -> assert false
          )
      ] >>= fun _ ->
      mres := mon_res ;  
      return ()             
    ) in             
  Lwt.(
    test_run_wrapper (fun () -> 
      P.run_node node_config ~process:test_proc >>= fun () -> 
      (get_option !exit_fn) () >>= fun () ->
      assert_bool "process was not spawned" (!result && !result_monitor <> None) ;
      assert_equal ~msg:"termination monitor result not received" (Some "got normal termination") !result_monitor ;     
      assert_equal ~msg:"remote config with only a single node should have establised 1 connection" 1 (Hashtbl.length established_connections) ;
      assert_bool "spawn monitor failed" (None <> !mres) ;
      
      return ()    
    )
  )

let test_spawn_monitor_remote_remote_config _ =
  let module Producer = Distributed.Make (Test_io) (M) in
  let module Consumer = Distributed.Make (Test_io) (M) in  
  let node_config = Producer.Remote { Producer.Remote_config.node_name = "producer" ; 
                                      Producer.Remote_config.local_port = 100 ;
                                      Producer.Remote_config.heart_beat_frequency = 200.0 ;
                                      Producer.Remote_config.heart_beat_timeout = 500.0 ;
                                      Producer.Remote_config.connection_backlog = 10 ;
                                      Producer.Remote_config.node_ip = "1.2.3.4" ;
                                      Producer.Remote_config.remote_nodes = [("5.6.7.8",101,"consumer")] ;
                                    } in
  let remote_config = Consumer.Remote { Consumer.Remote_config.node_name = "consumer" ; 
                                        Consumer.Remote_config.local_port = 101 ;
                                        Consumer.Remote_config.heart_beat_frequency = 200.0 ;
                                        Consumer.Remote_config.heart_beat_timeout = 500.0 ;
                                        Consumer.Remote_config.connection_backlog = 10 ;
                                        Consumer.Remote_config.node_ip = "5.6.7.8" ;
                                        Consumer.Remote_config.remote_nodes = [] ;
                                      } in
  let result_monitor = ref None in
  let mres = ref None in                                  
  let consumer_proc () = Consumer.(
      return () >>= fun _ ->
      return (
        let (conns,server_fn) = Hashtbl.find established_connections (Unix.ADDR_INET (Unix.inet6_addr_any , 101)) in
        Hashtbl.remove established_connections (Unix.ADDR_INET (Unix.inet6_addr_any , 101)) ;
        Hashtbl.replace established_connections (Unix.ADDR_INET (Unix.inet_addr_of_string "5.6.7.8" , 101)) (conns,server_fn) ; 
      )
    ) in    
  let producer_proc () = Producer.(      
      return () >>= fun _ ->
      return (
        let (conns,server_fn) = Hashtbl.find established_connections (Unix.ADDR_INET (Unix.inet6_addr_any , 100)) in
        Hashtbl.remove established_connections (Unix.ADDR_INET (Unix.inet6_addr_any , 100)) ;
        Hashtbl.replace established_connections (Unix.ADDR_INET (Unix.inet_addr_of_string "1.2.3.4" , 100)) (conns,server_fn) ; 
      ) >>= fun _ -> 
      get_remote_nodes >>= fun nodes ->
      get_self_pid >>= fun pid_to_send_to ->
      spawn ~monitor:true (List.hd nodes) (fun () -> Consumer.send pid_to_send_to "spawned") >>= fun (_, mon_res) ->      
      receive [
        termination_case 
          (function
            | Normal _ -> return (result_monitor := Some "got normal termination")
            | _ -> assert false
          )
      ] >>= fun _ ->
      mres := mon_res ;  
      return ()                       
    ) in
  Lwt.(
    test_run_wrapper (fun () -> 
      Lwt.async (fun () -> Consumer.run_node remote_config ~process:consumer_proc) ;            
      Producer.run_node node_config ~process:producer_proc >>= fun () ->
      (get_option !exit_fn) () >>= fun () ->      
      assert_equal ~msg:"termination monitor result not received" (Some "got normal termination") !result_monitor ;     
      assert_equal ~msg:"remote config with 2 remote nodes should have establised 2 connections" 2 (Hashtbl.length established_connections) ;
      assert_bool "spawn monitor failed" (None <> !mres) ;
      
      return () 
    ) 
  )          

(* monitor tests for local and remote configurations *)

let test_monitor_local_local_config _ =
  let module P = Distributed.Make (Test_io) (M) in
  let node_config = P.Local {P.Local_config.node_name = "test" ;} in
  let result = ref false in
  let result_monitor = ref None in  
  let result_monitor2 = ref None in
  let another_monitor_proc pid_to_monitor () = P.(
    monitor pid_to_monitor >>= fun _ ->
    receive [
      termination_case
        (function
          | Normal _ -> return (result_monitor2 := Some "got normal termination")
          | _ -> assert false
        )
    ] >>= fun _ -> return ()
  ) in 
  let test_proc () = P.(                                        
      get_self_node >>= fun local_node ->
      assert_bool "Process should not have spawned yet" (not !result) ;
      spawn local_node (fun () -> return () >>= fun _ -> lift_io (Test_io.sleep 0.05) >>= fun () -> return (result := true)) >>= fun (new_pid, _) ->
      spawn local_node (another_monitor_proc new_pid) >>= fun _ ->
      monitor new_pid >>= fun _ ->
      receive [
        termination_case 
          (function
            | Normal _ -> return (result_monitor := Some "got normal termination")
            | _ -> assert false
          )
      ] >>= fun _ ->
      return ()        
    ) in             
  Lwt.(test_run_wrapper (fun () -> P.run_node node_config ~process:test_proc >>= fun () -> (get_option !exit_fn) ()));
  assert_bool "process was not spawned" (!result && !result_monitor <> None) ; 
  assert_equal ~msg:"monitor failed" (Some "got normal termination") !result_monitor ;    
  assert_equal ~msg:"monitor 2 failed" (Some "got normal termination") !result_monitor2 ;    
  assert_equal ~msg:"local config should hav establised 0 connections" 0 (Hashtbl.length established_connections)    

let test_monitor_local_remote_config _ =
  let module P = Distributed.Make (Test_io) (M) in  
  let node_config = P.Remote { P.Remote_config.node_name = "producer" ; 
                               P.Remote_config.local_port = 100 ;
                               P.Remote_config.heart_beat_frequency = 2.0 ;
                               P.Remote_config.heart_beat_timeout = 5.0 ;
                               P.Remote_config.connection_backlog = 10 ;
                               P.Remote_config.node_ip = "1.2.3.4" ;
                               P.Remote_config.remote_nodes = [] ;
                             } in
  let result = ref false in
  let result_monitor = ref None in
  let result_monitor2 = ref None in
  let another_monitor_proc pid_to_monitor () = P.(
    monitor pid_to_monitor >>= fun _ ->
    receive [
      termination_case
        (function
          | Normal _ -> return (result_monitor2 := Some "got normal termination")
          | _ -> assert false
        )
    ] >>= fun _ -> return ()
  ) in   
  let test_proc () = P.(                                        
      get_self_node >>= fun local_node ->
      assert_bool "Process should not have spawned yet" (not !result) ;
      spawn local_node (fun () -> return () >>= fun _ -> lift_io (Test_io.sleep 0.05) >>= fun () -> return (result := true)) >>= fun (new_pid, _) ->
      spawn local_node (another_monitor_proc new_pid) >>= fun _ ->
      monitor new_pid >>= fun _ ->
      receive [
        termination_case 
          (function
            | Normal _ -> return (result_monitor := Some "got normal termination")
            | _ -> assert false
          )
      ] >>= fun _ ->
      return ()       
    ) in             
  Lwt.(
    test_run_wrapper (fun () -> 
      P.run_node node_config ~process:test_proc >>= fun () -> 
      (get_option !exit_fn) () >>= fun () ->
      assert_bool "process was not spawned" (!result && !result_monitor <> None) ;
      assert_equal ~msg:"monitor failed" (Some "got normal termination") !result_monitor ;      
      assert_equal ~msg:"monitor 2 failed" (Some "got normal termination") !result_monitor2 ;      
      assert_equal ~msg:"remote config with only a single node should have establised 1 connection" 1 (Hashtbl.length established_connections) ;
      
      return ()    
    )
  )

let test_monitor_remote_remote_config _ =
  let module Producer = Distributed.Make (Test_io) (M) in
  let module Consumer = Distributed.Make (Test_io) (M) in  
  let node_config = Producer.Remote { Producer.Remote_config.node_name = "producer" ; 
                                      Producer.Remote_config.local_port = 100 ;
                                      Producer.Remote_config.heart_beat_frequency = 200.0 ;
                                      Producer.Remote_config.heart_beat_timeout = 500.0 ;
                                      Producer.Remote_config.connection_backlog = 10 ;
                                      Producer.Remote_config.node_ip = "1.2.3.4" ;
                                      Producer.Remote_config.remote_nodes = [("5.6.7.8",101,"consumer")] ;
                                    } in
  let remote_config = Consumer.Remote { Consumer.Remote_config.node_name = "consumer" ; 
                                        Consumer.Remote_config.local_port = 101 ;
                                        Consumer.Remote_config.heart_beat_frequency = 200.0 ;
                                        Consumer.Remote_config.heart_beat_timeout = 500.0 ;
                                        Consumer.Remote_config.connection_backlog = 10 ;
                                        Consumer.Remote_config.node_ip = "5.6.7.8" ;
                                        Consumer.Remote_config.remote_nodes = [] ;
                                      } in
  let consumer_proc () = Consumer.(
      return () >>= fun _ ->
      return (
        let (conns,server_fn) = Hashtbl.find established_connections (Unix.ADDR_INET (Unix.inet6_addr_any , 101)) in
        Hashtbl.remove established_connections (Unix.ADDR_INET (Unix.inet6_addr_any , 101)) ;
        Hashtbl.replace established_connections (Unix.ADDR_INET (Unix.inet_addr_of_string "5.6.7.8" , 101)) (conns,server_fn) ; 
      )
    ) in    
  let result_monitor = ref None in 
  let result_monitor2 = ref None in 
  let another_monitor_proc pid_to_monitor () = Producer.(
    monitor pid_to_monitor >>= fun _ ->
    receive [
      termination_case
        (function
          | Normal _ -> return (result_monitor2 := Some "got normal termination")
          | _ -> assert false
        )
    ] >>= fun _ -> return ()
  ) in      
  let producer_proc () = Producer.(        
      return () >>= fun _ ->
      return (
        let (conns,server_fn) = Hashtbl.find established_connections (Unix.ADDR_INET (Unix.inet6_addr_any , 100)) in
        Hashtbl.remove established_connections (Unix.ADDR_INET (Unix.inet6_addr_any , 100)) ;
        Hashtbl.replace established_connections (Unix.ADDR_INET (Unix.inet_addr_of_string "1.2.3.4" , 100)) (conns,server_fn) ; 
      ) >>= fun _ -> 
      get_remote_nodes >>= fun nodes ->
      get_self_node >>= fun local_node ->
      spawn (List.hd nodes) (fun () -> return () >>= fun _ -> lift_io (Test_io.sleep 0.05)) >>= fun (remote_pid, _) ->
      spawn local_node (another_monitor_proc remote_pid) >>= fun _ ->
      monitor remote_pid >>= fun _ ->      
      receive [
        termination_case 
          (function
            | Normal _ -> return (result_monitor := Some "got normal termination")
            | _ -> assert false
          )
      ] >>= fun _ ->
      return ()                 
    ) in
  Lwt.(
    test_run_wrapper (fun () -> 
      Lwt.async (fun () -> Consumer.run_node remote_config ~process:consumer_proc) ;            
      Producer.run_node node_config ~process:producer_proc >>= fun () ->
      (get_option !exit_fn) () >>= fun () ->      
      assert_equal ~msg:"monitor failed" (Some "got normal termination") !result_monitor ;      
      assert_equal ~msg:"monitor 2 failed" (Some "got normal termination") !result_monitor2 ;      
      assert_equal ~msg:"remote config with 2 remote nodes should have establised 2 connections" 2 (Hashtbl.length established_connections) ;
      
      return () 
    ) 
  )            

(* unmonitor processes that were monitored using 'monitor' tests for local and remote configurations *)  

let test_unmonitor_local_local_config _ =
  let module P = Distributed.Make (Test_io) (M) in
  let node_config = P.Local {P.Local_config.node_name = "test" ;} in
  let result = ref false in
  let unmon_res = ref None in
  let unmon_res2 = ref None in 
  let another_monitor_proc pid_to_monitor () = P.(
    monitor pid_to_monitor >>= fun mres ->
    unmonitor mres >>= fun _ ->
    receive ~timeout_duration:0.05 [
      termination_case
        (function
          | Normal _ -> return (unmon_res2 := Some "got normal termination")
          | _ -> assert false
        )
    ] >>= fun _ -> return ()
  ) in     
  let test_proc () = P.(                                        
      get_self_node >>= fun local_node ->
      assert_bool "process should not have spawned yet" (not !result) ;
      spawn local_node (fun () -> return () >>= fun _ -> lift_io (Test_io.sleep 0.05) >>= fun () -> return (result := true)) >>= fun (new_pid, _) ->
      spawn local_node (another_monitor_proc new_pid) >>= fun _ ->
      monitor new_pid >>= fun mon_res ->
      unmonitor mon_res >>= fun () ->
      receive ~timeout_duration:0.05 [
        termination_case 
          (function
            | Normal _ -> return "got normal termination"
            | _ -> assert false
          )
      ] >>= fun received ->
      unmon_res := received ;
      return ()        
    ) in             
  Lwt.(test_run_wrapper (fun () -> P.run_node node_config ~process:test_proc >>= fun () -> (get_option !exit_fn) ()));
  assert_bool "process was not spawned" !result ;
  assert_equal ~msg:"unmonitor failed" None !unmon_res ;     
  assert_equal ~msg:"unmonitor 2 failed" None !unmon_res2 ;     
  assert_equal ~msg:"local config should hav establised 0 connections" 0 (Hashtbl.length established_connections)    

let test_unmonitor_local_remote_config _ =
  let module P = Distributed.Make (Test_io) (M) in  
  let node_config = P.Remote { P.Remote_config.node_name = "producer" ; 
                               P.Remote_config.local_port = 100 ;
                               P.Remote_config.heart_beat_frequency = 2.0 ;
                               P.Remote_config.heart_beat_timeout = 5.0 ;
                               P.Remote_config.connection_backlog = 10 ;
                               P.Remote_config.node_ip = "1.2.3.4" ;
                               P.Remote_config.remote_nodes = [] ;
                             } in
  let result = ref false in
  let unmon_res = ref None in
  let unmon_res2 = ref None in 
  let another_monitor_proc pid_to_monitor () = P.(
    monitor pid_to_monitor >>= fun mres ->
    unmonitor mres >>= fun _ ->
    receive ~timeout_duration:0.05 [
      termination_case
        (function
          | Normal _ -> return (unmon_res2 := Some "got normal termination")
          | _ -> assert false
        )
    ] >>= fun _ -> return ()
  ) in     
  let test_proc () = P.(                                        
      get_self_node >>= fun local_node ->
      assert_bool "Process should not have spawned yet" (not !result) ;
      spawn local_node (fun () -> return () >>= fun _ -> lift_io (Test_io.sleep 0.05) >>= fun () -> return (result := true)) >>= fun (new_pid, _) ->
      spawn local_node (another_monitor_proc new_pid) >>= fun _ ->
      monitor new_pid >>= fun mon_res ->
      unmonitor mon_res >>= fun () ->
      receive ~timeout_duration:0.05 [
        termination_case
          (function
            | Normal _ -> return "got normal termination"
            | _ -> assert false
          )
      ] >>= fun received ->
      unmon_res := received ;
      return ()        
    ) in             
  Lwt.(
    test_run_wrapper (fun () -> 
      P.run_node node_config ~process:test_proc >>= fun () -> 
      (get_option !exit_fn) () >>= fun () ->
      assert_bool "process was not spawned" !result ;  
      assert_equal ~msg:"unmonitor failed" None !unmon_res ;    
      assert_equal ~msg:"unmonitor 2 failed" None !unmon_res2 ;    
      assert_equal ~msg:"remote config with only a single node should have establised 1 connection" 1 (Hashtbl.length established_connections) ;
      
      return ()    
    )
  )

let test_unmonitor_remote_remote_config _ =
  let module Producer = Distributed.Make (Test_io) (M) in
  let module Consumer = Distributed.Make (Test_io) (M) in  
  let node_config = Producer.Remote { Producer.Remote_config.node_name = "producer" ; 
                                      Producer.Remote_config.local_port = 100 ;
                                      Producer.Remote_config.heart_beat_frequency = 200.0 ;
                                      Producer.Remote_config.heart_beat_timeout = 500.0 ;
                                      Producer.Remote_config.connection_backlog = 10 ;
                                      Producer.Remote_config.node_ip = "1.2.3.4" ;
                                      Producer.Remote_config.remote_nodes = [("5.6.7.8",101,"consumer")] ;
                                    } in
  let remote_config = Consumer.Remote { Consumer.Remote_config.node_name = "consumer" ; 
                                        Consumer.Remote_config.local_port = 101 ;
                                        Consumer.Remote_config.heart_beat_frequency = 200.0 ;
                                        Consumer.Remote_config.heart_beat_timeout = 500.0 ;
                                        Consumer.Remote_config.connection_backlog = 10 ;
                                        Consumer.Remote_config.node_ip = "5.6.7.8" ;
                                        Consumer.Remote_config.remote_nodes = [] ;
                                      } in
  let consumer_proc () = Consumer.(
      return () >>= fun _ ->
      return (
        let (conns,server_fn) = Hashtbl.find established_connections (Unix.ADDR_INET (Unix.inet6_addr_any , 101)) in
        Hashtbl.remove established_connections (Unix.ADDR_INET (Unix.inet6_addr_any , 101)) ;
        Hashtbl.replace established_connections (Unix.ADDR_INET (Unix.inet_addr_of_string "5.6.7.8" , 101)) (conns,server_fn) ; 
      )
    ) in    
  let unmon_res = ref None in
  let unmon_res2 = ref None in 
  let another_monitor_proc pid_to_monitor () = Producer.(
    monitor pid_to_monitor >>= fun mres ->
    unmonitor mres >>= fun _ ->
    receive ~timeout_duration:0.05 [
      termination_case
        (function
          | Normal _ -> return (unmon_res2 := Some "got normal termination")
          | _ -> assert false
        )
    ] >>= fun _ -> return ()
  ) in     
  let producer_proc () = Producer.(      
      return () >>= fun _ ->
      return (
        let (conns,server_fn) = Hashtbl.find established_connections (Unix.ADDR_INET (Unix.inet6_addr_any , 100)) in
        Hashtbl.remove established_connections (Unix.ADDR_INET (Unix.inet6_addr_any , 100)) ;
        Hashtbl.replace established_connections (Unix.ADDR_INET (Unix.inet_addr_of_string "1.2.3.4" , 100)) (conns,server_fn) ; 
      ) >>= fun _ -> 
      get_remote_nodes >>= fun nodes ->    
      get_self_node >>= fun local_node ->  
      spawn (List.hd nodes) (fun () -> return () >>= fun _ -> lift_io (Test_io.sleep 0.05)) >>= fun (remote_pid, _) ->
      spawn local_node (another_monitor_proc remote_pid) >>= fun _ ->
      monitor remote_pid >>= fun mon_res ->      
      unmonitor mon_res >>= fun () ->
      receive ~timeout_duration:0.05 [
        termination_case
          (function
            | Normal _ -> return "got normal termination"
            | _ -> assert false
          )
      ] >>= fun received ->
      unmon_res := received ;     
      return ()                 
    ) in
  Lwt.(
    test_run_wrapper (fun () -> 
      Lwt.async (fun () -> Consumer.run_node remote_config ~process:consumer_proc) ;            
      Producer.run_node node_config ~process:producer_proc >>= fun () ->
      (get_option !exit_fn) () >>= fun () ->      
      assert_equal ~msg:"unmonitor failed" None !unmon_res ;    
      assert_equal ~msg:"unmonitor 2 failed" None !unmon_res2 ;    
      assert_equal ~msg:"remote config with 2 remote nodes should have establised 2 connections" 2 (Hashtbl.length established_connections) ;
      
      return () 
    ) 
  )            

(* unmonitor processes that were monitored using 'spawn monitor:true' tests for local and remote configurations *)  

let test_unmonitor_from_spawn_monitor_local_local_config _ =
  let module P = Distributed.Make (Test_io) (M) in
  let node_config = P.Local {P.Local_config.node_name = "test" ;} in
  let result = ref false in
  let mres = ref None in
  let unmon_res = ref None in
  let test_proc () = P.(                                        
      get_self_node >>= fun local_node ->
      assert_bool "Process should not have spawned yet" (not !result) ;
      spawn ~monitor:true local_node (fun () -> return () >>= fun _ -> lift_io (Test_io.sleep 0.05) >>= fun () -> result := true ; return ()) >>= fun (_, spawn_mon_res) ->
      mres := spawn_mon_res ;
      unmonitor (get_option spawn_mon_res) >>= fun () ->
      receive ~timeout_duration:0.05 [
        termination_case
          (function
            | Normal _ -> return "got normal termination"
            | _ -> assert false
          )
      ] >>= fun received ->
      unmon_res := received ;
      return ()             
    ) in             
  Lwt.(test_run_wrapper (fun () -> P.run_node node_config ~process:test_proc >>= fun () -> (get_option !exit_fn) ()));
  assert_bool "Process was not spawned and monitored" (!result && !mres <> None) ;
  assert_equal ~msg:"unmonitor failed" None !unmon_res ;       
  assert_equal ~msg:"local config should hav establised 0 connections" 0 (Hashtbl.length established_connections)    

let test_unmonitor_from_spawn_monitor_local_remote_config _ =
  let module P = Distributed.Make (Test_io) (M) in  
  let node_config = P.Remote { P.Remote_config.node_name = "producer" ; 
                               P.Remote_config.local_port = 100 ;
                               P.Remote_config.heart_beat_frequency = 2.0 ;
                               P.Remote_config.heart_beat_timeout = 5.0 ;
                               P.Remote_config.connection_backlog = 10 ;
                               P.Remote_config.node_ip = "1.2.3.4" ;
                               P.Remote_config.remote_nodes = [] ;
                             } in
  let result = ref false in
  let mres = ref None in
  let unmon_res = ref None in  
  let test_proc () = P.(     
      get_self_node >>= fun local_node ->
      assert_bool "Process should not have spawned yet" (not !result) ;
      spawn ~monitor:true local_node (fun () -> return () >>= fun _ -> lift_io (Test_io.sleep 0.05) >>= fun () -> result := true ; return ()) >>= fun (_, spawn_mon_res) ->      
      mres := spawn_mon_res ;
      unmonitor (get_option spawn_mon_res) >>= fun () ->
      receive ~timeout_duration:0.05 [
        termination_case
          (function
            | Normal _ -> return "got normal termination"
            | _ -> assert false
          )
      ] >>= fun received ->
      unmon_res := received ;
      return ()                 
    ) in             
  Lwt.(
    test_run_wrapper (fun () -> 
      P.run_node node_config ~process:test_proc >>= fun () -> 
      (get_option !exit_fn) () >>= fun () ->
      assert_bool "Process was not spawned and monitored" (!result && !mres <> None) ;
      assert_equal ~msg:"unmonitor failed" None !unmon_res ;       
      assert_equal ~msg:"remote config with only a single node should have establised 1 connection" 1 (Hashtbl.length established_connections) ;
      
      return ()    
    )
  )

let test_unmonitor_from_spawn_monitor_remote_remote_config _ =
  let module Producer = Distributed.Make (Test_io) (M) in
  let module Consumer = Distributed.Make (Test_io) (M) in  
  let node_config = Producer.Remote { Producer.Remote_config.node_name = "producer" ; 
                                      Producer.Remote_config.local_port = 100 ;
                                      Producer.Remote_config.heart_beat_frequency = 200.0 ;
                                      Producer.Remote_config.heart_beat_timeout = 500.0 ;
                                      Producer.Remote_config.connection_backlog = 10 ;
                                      Producer.Remote_config.node_ip = "1.2.3.4" ;
                                      Producer.Remote_config.remote_nodes = [("5.6.7.8",101,"consumer")] ;
                                    } in
  let remote_config = Consumer.Remote { Consumer.Remote_config.node_name = "consumer" ; 
                                        Consumer.Remote_config.local_port = 101 ;
                                        Consumer.Remote_config.heart_beat_frequency = 200.0 ;
                                        Consumer.Remote_config.heart_beat_timeout = 500.0 ;
                                        Consumer.Remote_config.connection_backlog = 10 ;
                                        Consumer.Remote_config.node_ip = "5.6.7.8" ;
                                        Consumer.Remote_config.remote_nodes = [] ;
                                      } in
  let consumer_proc () = Consumer.(
      return () >>= fun _ ->
      return (
        let (conns,server_fn) = Hashtbl.find established_connections (Unix.ADDR_INET (Unix.inet6_addr_any , 101)) in
        Hashtbl.remove established_connections (Unix.ADDR_INET (Unix.inet6_addr_any , 101)) ;
        Hashtbl.replace established_connections (Unix.ADDR_INET (Unix.inet_addr_of_string "5.6.7.8" , 101)) (conns,server_fn) ; 
      )
    ) in   
  let mres = ref None in
  let unmon_res = ref None in 
  let producer_proc () = Producer.(
      return () >>= fun _ ->
      return (
        let (conns,server_fn) = Hashtbl.find established_connections (Unix.ADDR_INET (Unix.inet6_addr_any , 100)) in
        Hashtbl.remove established_connections (Unix.ADDR_INET (Unix.inet6_addr_any , 100)) ;
        Hashtbl.replace established_connections (Unix.ADDR_INET (Unix.inet_addr_of_string "1.2.3.4" , 100)) (conns,server_fn) ; 
      ) >>= fun _ -> 
      get_remote_nodes >>= fun nodes ->      
      spawn ~monitor:true (List.hd nodes) (fun () -> return () >>= fun _ -> lift_io (Test_io.sleep 0.05)) >>= fun (_, spawn_mon_res) ->
      mres := spawn_mon_res ;
      unmonitor (get_option spawn_mon_res) >>= fun () ->
      receive ~timeout_duration:0.05 [
        termination_case
          (function
            | Normal _ -> return "got normal termination"
            | _ -> assert false
          )
      ] >>= fun received ->
      unmon_res := received ;
      return ()                  
    ) in
  Lwt.(
    test_run_wrapper (fun () -> 
      Lwt.async (fun () -> Consumer.run_node remote_config ~process:consumer_proc) ;            
      Producer.run_node node_config ~process:producer_proc >>= fun () ->
      (get_option !exit_fn) () >>= fun () ->      
      assert_equal ~msg:"unmonitor failed" None !unmon_res ;       
      assert_equal ~msg:"remote config with 2 remote nodes should have establised 2 connections" 2 (Hashtbl.length established_connections) ;
      
      return () 
    ) 
  )   

(* tests for get_remote_nodes for local and remote configurations *)

let test_get_remote_nodes_local_only _ =
  let module P = Distributed.Make (Test_io) (M) in      
  let node_config = P.Local {P.Local_config.node_name = "test" ;} in
  let num_remote_nodes = ref (-1) in
  let test_proc () = P.(
      get_remote_nodes >>= fun nodes ->
      return (num_remote_nodes := (List.length nodes))       
    ) in             
  test_run_wrapper (fun () -> P.run_node node_config ~process:test_proc) ;     
  assert_equal ~msg:"get remote nodes in local config should return 0" 0 !num_remote_nodes ;
  assert_equal ~msg:"local config should hav establised 0 connections" 0 (Hashtbl.length established_connections)

let test_get_remote_nodes_remote_local _ =
  let module P = Distributed.Make (Test_io) (M) in  
  let node_config = P.Remote { P.Remote_config.node_name = "producer" ; 
                               P.Remote_config.local_port = 100 ;
                               P.Remote_config.heart_beat_frequency = 2.0 ;
                               P.Remote_config.heart_beat_timeout = 5.0 ;
                               P.Remote_config.connection_backlog = 10 ;
                               P.Remote_config.node_ip = "1.2.3.4" ;
                               P.Remote_config.remote_nodes = [] ;
                             } in
  let num_remote_nodes = ref (-1) in
  let test_proc () = P.(        
      get_remote_nodes >>= fun nodes ->
      return (num_remote_nodes := (List.length nodes))         
    ) in             
  Lwt.(
    test_run_wrapper (fun () -> 
      P.run_node node_config ~process:test_proc >>= fun () -> 
      (get_option !exit_fn) () >>= fun () ->
      assert_equal ~msg:"get remote nodes in remote config with no remote nodes should return 0" 0 !num_remote_nodes ;
      assert_equal ~msg:"remote config with only a single node should have establised 1 connection" 1 (Hashtbl.length established_connections) ;
      
      return ()
    )
  )

let test_get_remote_nodes_remote_conifg _ =
  let module Producer = Distributed.Make (Test_io) (M) in
  let module Consumer = Distributed.Make (Test_io) (M) in  
  let node_config = Producer.Remote { Producer.Remote_config.node_name = "producer" ; 
                                      Producer.Remote_config.local_port = 100 ;
                                      Producer.Remote_config.heart_beat_frequency = 200.0 ;
                                      Producer.Remote_config.heart_beat_timeout = 500.0 ;
                                      Producer.Remote_config.connection_backlog = 10 ;
                                      Producer.Remote_config.node_ip = "1.2.3.4" ;
                                      Producer.Remote_config.remote_nodes = [("5.6.7.8",101,"consumer")] ;
                                    } in
  let remote_config = Consumer.Remote { Consumer.Remote_config.node_name = "consumer" ; 
                                        Consumer.Remote_config.local_port = 101 ;
                                        Consumer.Remote_config.heart_beat_frequency = 200.0 ;
                                        Consumer.Remote_config.heart_beat_timeout = 500.0 ;
                                        Consumer.Remote_config.connection_backlog = 10 ;
                                        Consumer.Remote_config.node_ip = "5.6.7.8" ;
                                        Consumer.Remote_config.remote_nodes = [] ;
                                      } in
  let consumer_proc () = Consumer.(
      return () >>= fun _ ->
      return (
        let (conns,server_fn) = Hashtbl.find established_connections (Unix.ADDR_INET (Unix.inet6_addr_any , 101)) in
        Hashtbl.remove established_connections (Unix.ADDR_INET (Unix.inet6_addr_any , 101)) ;
        Hashtbl.replace established_connections (Unix.ADDR_INET (Unix.inet_addr_of_string "5.6.7.8" , 101)) (conns,server_fn) ; 
      )
    ) in
  let num_remote_nodes = ref (-1) in    
  let producer_proc () = Producer.(
      return () >>= fun _ ->
      return (
        let (conns,server_fn) = Hashtbl.find established_connections (Unix.ADDR_INET (Unix.inet6_addr_any , 100)) in
        Hashtbl.remove established_connections (Unix.ADDR_INET (Unix.inet6_addr_any , 100)) ;
        Hashtbl.replace established_connections (Unix.ADDR_INET (Unix.inet_addr_of_string "1.2.3.4" , 100)) (conns,server_fn) ; 
      ) >>= fun _ -> 
      get_remote_nodes >>= fun nodes ->
      return (num_remote_nodes := (List.length nodes))                          
    ) in
  Lwt.(
    test_run_wrapper (fun () -> 
      Lwt.async (fun () -> Consumer.run_node remote_config ~process:consumer_proc) ;            
      Producer.run_node node_config ~process:producer_proc >>= fun () ->
      (get_option !exit_fn) () >>= fun () ->
      assert_equal ~msg:"get remote nodes in remote config with 1 remote nodes should return 1" 1 !num_remote_nodes ;
      assert_equal ~msg:"remote config with 2 remote nodes should have establised 2 connections" 2 (Hashtbl.length established_connections) ;
      
      return () 
    ) 
  )             

(* tests for broadcast for local and remote configurations  *)

let test_broadcast_local_only _ =
  let module P = Distributed.Make (Test_io) (M) in  
  let node_config = P.Local {P.Local_config.node_name = "test" ;} in
  let broadcast_received = ref 0 in      
  let loop_back_received = ref None in                         
  let recv_proc () = P.(        
      receive [
        case (
          function 
          | "broadcast message" -> Some (fun () -> return (broadcast_received := !broadcast_received +1))
          | _ -> None
        ) ;
        case (fun _ -> Some (fun () -> return ()))
      ] >>= fun _ ->
      return ()      
    ) in      
  let test_proc () = P.(
      get_self_node >>= fun local_node ->
      spawn local_node recv_proc >>= fun _ ->
      spawn local_node recv_proc >>= fun _ ->
      broadcast local_node "broadcast message" >>= fun () ->
      receive ~timeout_duration:0.05 [
        case (fun m -> Some (fun () -> return m))       
      ] >>= fun recv_res ->
      loop_back_received := recv_res ;            
      return ()
    ) in           
  Lwt.(test_run_wrapper (fun () -> P.run_node node_config ~process:test_proc >>= fun () -> (get_option !exit_fn) ())) ;
  assert_equal ~msg:"broacast failed" 2 !broadcast_received ;
  assert_equal ~msg:"broadcast message sent to originator" None !loop_back_received ;    
  assert_equal ~msg:"local config should hav establised 0 connections" 0 (Hashtbl.length established_connections)

let test_broadcast_remote_local _ =
  let module P = Distributed.Make (Test_io) (M) in  
  let node_config = P.Remote { P.Remote_config.node_name = "producer" ; 
                               P.Remote_config.local_port = 100 ;
                               P.Remote_config.heart_beat_frequency = 2.0 ;
                               P.Remote_config.heart_beat_timeout = 5.0 ;
                               P.Remote_config.connection_backlog = 10 ;
                               P.Remote_config.node_ip = "1.2.3.4" ;
                               P.Remote_config.remote_nodes = [] ;
                             } in
  let broadcast_received = ref 0 in 
  let loop_back_received = ref None in                            
  let recv_proc () = P.(        
      receive [
        case (
          function 
          | "broadcast message" -> Some (fun () -> return (broadcast_received := !broadcast_received +1))
          | _ -> None
        ) ;
        case (fun _ -> Some (fun () -> return ()))
      ] >>= fun _ ->
      return ()      
    ) in      
  let test_proc () = P.(
      get_self_node >>= fun local_node ->
      spawn local_node recv_proc >>= fun _ ->
      spawn local_node recv_proc >>= fun _ ->
      broadcast local_node "broadcast message" >>= fun () ->
      receive ~timeout_duration:0.05 [
        case (fun m -> Some (fun () -> return m))       
      ] >>= fun recv_res ->
      loop_back_received := recv_res ;      
      return ()
    ) in           
  Lwt.(
    test_run_wrapper (fun () -> 
      P.run_node node_config ~process:test_proc >>= fun () -> 
      (get_option !exit_fn) () >>= fun () ->
      assert_equal ~msg:"remote config with only a single node should have establised 1 connection" 1 (Hashtbl.length established_connections) ;
      assert_equal ~msg:"broacast fail" 2 !broadcast_received ; 
      assert_equal ~msg:"broadcast message sent to originator" None !loop_back_received ;     
      
      return ()  
    )
  )

let test_broadcast_remote_remote _ =
  let module Producer = Distributed.Make (Test_io) (M) in
  let module Consumer = Distributed.Make (Test_io) (M) in  
  let node_config = Producer.Remote { Producer.Remote_config.node_name = "producer" ; 
                                      Producer.Remote_config.local_port = 100 ;
                                      Producer.Remote_config.heart_beat_frequency = 200.0 ;
                                      Producer.Remote_config.heart_beat_timeout = 500.0 ;
                                      Producer.Remote_config.connection_backlog = 10 ;
                                      Producer.Remote_config.node_ip = "1.2.3.4" ;
                                      Producer.Remote_config.remote_nodes = [("5.6.7.8",101,"consumer")] ;
                                    } in
  let remote_config = Consumer.Remote { Consumer.Remote_config.node_name = "consumer" ; 
                                        Consumer.Remote_config.local_port = 101 ;
                                        Consumer.Remote_config.heart_beat_frequency = 200.0 ;
                                        Consumer.Remote_config.heart_beat_timeout = 500.0 ;
                                        Consumer.Remote_config.connection_backlog = 10 ;
                                        Consumer.Remote_config.node_ip = "5.6.7.8" ;
                                        Consumer.Remote_config.remote_nodes = [] ;
                                      } in
  let consumer_proc () = Consumer.(
      return () >>= fun _ ->
      return (
        let (conns,server_fn) = Hashtbl.find established_connections (Unix.ADDR_INET (Unix.inet6_addr_any , 101)) in
        Hashtbl.remove established_connections (Unix.ADDR_INET (Unix.inet6_addr_any , 101)) ;
        Hashtbl.replace established_connections (Unix.ADDR_INET (Unix.inet_addr_of_string "5.6.7.8" , 101)) (conns,server_fn) ; 
      )
    ) in
  let broadcast_received = ref 0 in
  let loop_back_received = ref None in                             
  let recv_proc to_send_pid () = Consumer.(        
      receive [
        case (
          function 
          | "broadcast message" -> Some (fun () -> send to_send_pid "incr")
          | _ -> None
        ) ;
        case (fun _ -> Some (fun _ -> return ()))
      ] >>= fun _ ->
      return ()      
    ) in      
  let producer_proc () = Producer.(
      return () >>= fun _ ->
      return (
        let (conns,server_fn) = Hashtbl.find established_connections (Unix.ADDR_INET (Unix.inet6_addr_any , 100)) in
        Hashtbl.remove established_connections (Unix.ADDR_INET (Unix.inet6_addr_any , 100)) ;
        Hashtbl.replace established_connections (Unix.ADDR_INET (Unix.inet_addr_of_string "1.2.3.4" , 100)) (conns,server_fn) ; 
      ) >>= fun _ ->
      get_self_node >>= fun local_node ->
      get_self_pid >>= fun my_pid ->
      spawn local_node (recv_proc my_pid) >>= fun _ ->
      spawn local_node (recv_proc my_pid) >>= fun _ ->
      broadcast local_node "broadcast message" >>= fun () ->
      get_remote_nodes >>= fun rnodes ->
      spawn (List.hd rnodes) (recv_proc my_pid) >>= fun _ ->
      spawn (List.hd rnodes) (recv_proc my_pid) >>= fun _ ->
      broadcast (List.hd rnodes) "broadcast message" >>= fun () ->
      let rec receive_loop () =
        receive ~timeout_duration:0.05 [
          case (
            function 
            | "incr" -> Some (fun () -> return (broadcast_received := !broadcast_received +1))
            | _ -> None
          ) ;
          case (
            function
            | "broadcast message" -> Some (fun _ -> return (loop_back_received := Some "broadcast message"))
            | _ -> None
          ) ;          
          case (fun _ -> Some (fun _ -> return ()))       
        ] >>= fun res ->
        if res = None
        then return ()
        else receive_loop ()
      in 
      receive_loop ()      
    ) in      
  Lwt.(
    test_run_wrapper (fun () -> 
      Lwt.async (fun () -> Consumer.run_node remote_config ~process:consumer_proc) ;            
      Producer.run_node node_config ~process:producer_proc >>= fun () ->
      (get_option !exit_fn) () >>= fun () ->
      assert_equal ~msg:"broacast fail" 4 !broadcast_received ;
      assert_equal ~msg:"broadcast message sent to originator" None !loop_back_received ;
      assert_equal ~msg:"remote config with 2 remote nodes should have establised 2 connections" 2 (Hashtbl.length established_connections) ;
      
      return () 
    ) 
  )                       

(* tests for send for local and remote configurations *)

let test_send_local_only _ =
  let module P = Distributed.Make (Test_io) (M) in  
  let node_config = P.Local {P.Local_config.node_name = "test" ;} in  
  let received_message = ref None in
  let mres = ref None in       
  let send_failed = ref false in                  
  let recv_proc () = P.(        
      receive [
        case (
          function 
          | "sent message" -> Some (fun () -> return (received_message := Some "sent message"))
          | _ -> None
        ) ;
        case (fun _ -> Some (fun _ -> return ()))
      ] >>= fun _ ->
      return ()      
    ) in      
  let test_proc () = P.(      
      get_self_node >>= fun local_node ->
      spawn ~monitor:true local_node recv_proc >>= fun (spawned_pid,mref) ->
      mres := mref ;
      send spawned_pid "sent message" >>= fun () ->
      receive ~timeout_duration:0.05 [
        termination_case 
          (function
            | Normal _ -> 
              catch 
                (fun () -> send spawned_pid "sent message")
                (function
                  | _ -> return (send_failed := true)                  
                )
            | _ -> return (send_failed := true)
          )        
      ] >>= fun _ ->           
      return ()
    ) in           
  Lwt.(test_run_wrapper (fun () -> P.run_node node_config ~process:test_proc >>= fun () -> (get_option !exit_fn) ())) ;
  assert_bool "spawn and monitor failed" (!mres <> None) ;
  assert_equal ~msg:"send failed" (Some "sent message") !received_message ;
  assert_bool "sending to invalid process should have succeeded" (not !send_failed) ;    
  assert_equal ~msg:"local config should hav establised 0 connections" 0 (Hashtbl.length established_connections)  

let test_send_remote_local _ =
  let module P = Distributed.Make (Test_io) (M) in  
  let node_config = P.Remote { P.Remote_config.node_name = "producer" ; 
                               P.Remote_config.local_port = 100 ;
                               P.Remote_config.heart_beat_frequency = 2.0 ;
                               P.Remote_config.heart_beat_timeout = 5.0 ;
                               P.Remote_config.connection_backlog = 10 ;
                               P.Remote_config.node_ip = "1.2.3.4" ;
                               P.Remote_config.remote_nodes = [] ;
                             } in  
  let received_message = ref None in
  let mres = ref None in       
  let send_failed = ref false in                  
  let recv_proc () = P.(        
      receive [
        case (
          function 
          | "sent message" -> Some (fun () -> return (received_message := Some "sent message"))
          | _ -> None
        ) ;
        case (fun _ -> Some (fun _ -> return ()))
      ] >>= fun _ ->
      return ()      
    ) in      
  let test_proc () = P.(      
      get_self_node >>= fun local_node ->
      spawn ~monitor:true local_node recv_proc >>= fun (spawned_pid,mref) ->
      mres := mref ;
      send spawned_pid "sent message" >>= fun () ->
      receive ~timeout_duration:0.05 [
        termination_case 
          (function
            | Normal _ -> 
              catch 
                (fun () -> send spawned_pid "sent message")
                (function
                  |  _ -> return (send_failed := true)                    
                )
            | _ -> return (send_failed := true)
          )        
      ] >>= fun _ ->           
      return ()
    ) in           
  Lwt.(
    test_run_wrapper (fun () -> 
      P.run_node node_config ~process:test_proc >>= fun () -> 
      (get_option !exit_fn) () >>= fun () ->
      assert_bool "spawn and monitor failed" (!mres <> None) ;
      assert_equal ~msg:"send failed" (Some "sent message") !received_message ;
      assert_bool "sending to invalid process should have succeeded" (not !send_failed) ;   
      assert_equal ~msg:"remote config with only a single node should have establised 1 connection" 1 (Hashtbl.length established_connections) ;
      
      return ()    
    )
  )

let test_send_remote_remote _ =
  let module Producer = Distributed.Make (Test_io) (M) in
  let module Consumer = Distributed.Make (Test_io) (M) in  
  let node_config = Producer.Remote { Producer.Remote_config.node_name = "producer" ; 
                                      Producer.Remote_config.local_port = 100 ;
                                      Producer.Remote_config.heart_beat_frequency = 200.0 ;
                                      Producer.Remote_config.heart_beat_timeout = 500.0 ;
                                      Producer.Remote_config.connection_backlog = 10 ;
                                      Producer.Remote_config.node_ip = "1.2.3.4" ;
                                      Producer.Remote_config.remote_nodes = [("5.6.7.8",101,"consumer")] ;
                                    } in
  let remote_config = Consumer.Remote { Consumer.Remote_config.node_name = "consumer" ; 
                                        Consumer.Remote_config.local_port = 101 ;
                                        Consumer.Remote_config.heart_beat_frequency = 200.0 ;
                                        Consumer.Remote_config.heart_beat_timeout = 500.0 ;
                                        Consumer.Remote_config.connection_backlog = 10 ;
                                        Consumer.Remote_config.node_ip = "5.6.7.8" ;
                                        Consumer.Remote_config.remote_nodes = [] ;
                                      } in
  let consumer_proc () = Consumer.(
      return () >>= fun _ ->
      return (
        let (conns,server_fn) = Hashtbl.find established_connections (Unix.ADDR_INET (Unix.inet6_addr_any , 101)) in
        Hashtbl.remove established_connections (Unix.ADDR_INET (Unix.inet6_addr_any , 101)) ;
        Hashtbl.replace established_connections (Unix.ADDR_INET (Unix.inet_addr_of_string "5.6.7.8" , 101)) (conns,server_fn) ; 
      )
    ) in
  let sent_received = ref 0 in
  let send_failed = ref false in                               
  let recv_proc to_send_pid () = Consumer.(        
      receive [
        case (
          function
          | "sent message" -> Some (fun () -> send to_send_pid "incr")
          | _ -> None
        ) ;
        case (fun _ -> Some (fun () -> return ()))
      ] >>= fun _ ->
      return ()      
    ) in      
  let producer_proc () = Producer.(
      return () >>= fun _ ->
      return (
        let (conns,server_fn) = Hashtbl.find established_connections (Unix.ADDR_INET (Unix.inet6_addr_any , 100)) in
        Hashtbl.remove established_connections (Unix.ADDR_INET (Unix.inet6_addr_any , 100)) ;
        Hashtbl.replace established_connections (Unix.ADDR_INET (Unix.inet_addr_of_string "1.2.3.4" , 100)) (conns,server_fn) ; 
      ) >>= fun _ ->
      get_self_node >>= fun local_node ->
      get_self_pid >>= fun my_pid ->
      spawn ~monitor:true local_node (recv_proc my_pid) >>= fun (pid1,_) ->
      spawn ~monitor:true local_node (recv_proc my_pid) >>= fun (pid2,_) ->
      send pid1 "sent message" >>= fun () ->
      send pid2 "sent message" >>= fun () ->
      get_remote_nodes >>= fun rnodes ->
      spawn ~monitor:true (List.hd rnodes) (recv_proc my_pid) >>= fun (pid3,_) ->
      spawn ~monitor:true (List.hd rnodes) (recv_proc my_pid) >>= fun (pid4,_) ->
      send pid3 "sent message" >>= fun () ->
      send pid4 "sent message" >>= fun () ->
      let rec receive_loop () =
        receive ~timeout_duration:0.05 [
          case (
            function 
            | "incr" -> Some (fun () -> return (sent_received := !sent_received +1))
            | _ -> None
          ) ;
          termination_case 
            (function
              | Normal term_pid -> 
                catch 
                  (fun () -> send term_pid "sent message")
                  (function
                    | _ -> return (send_failed := true)                      
                  )
              | _ -> return (send_failed := true)
            ) ;                         
          case (fun _ -> Some (fun () -> return ()))       
        ] >>= fun res ->
        if res = None
        then return ()
        else receive_loop ()
      in 
      receive_loop ()      
    ) in      
  Lwt.(
    test_run_wrapper (fun () -> 
      Lwt.async (fun () -> Consumer.run_node remote_config ~process:consumer_proc) ;            
      Producer.run_node node_config ~process:producer_proc >>= fun () ->
      (get_option !exit_fn) () >>= fun () ->
      assert_equal ~msg:"send fail" 4 !sent_received ;
      assert_bool "sending to invalid process should have succeeded" (not !send_failed) ;      
      assert_equal ~msg:"remote config with 2 remote nodes should have establised 2 connections" 2 (Hashtbl.length established_connections) ;
      
      return () 
    ) 
  )                               

(* tests for receive with empty matchers for local and remote configurations *)

let test_empty_matchers_local_only _ =
  let module P = Distributed.Make (Test_io) (M) in  
  let node_config = P.Local {P.Local_config.node_name = "test" ;} in  
  let expected_exception_happened = ref false in        
  let test_proc () = P.(      
      catch 
        (fun () -> receive []) 
        (function
          | Empty_matchers -> expected_exception_happened := true ; return None
          | _ -> assert false
        ) >>= fun _ ->           
      return ()
    ) in           
  Lwt.(test_run_wrapper (fun () -> P.run_node node_config ~process:test_proc >>= fun () -> (get_option !exit_fn) ())) ;
  assert_bool "expected empty matchers exception did not occur" !expected_exception_happened;
  assert_equal ~msg:"local config should hav establised 0 connections" 0 (Hashtbl.length established_connections)

let test_empty_matchers_remote_local _ =
  let module P = Distributed.Make (Test_io) (M) in  
  let node_config = P.Remote { P.Remote_config.node_name = "producer" ; 
                               P.Remote_config.local_port = 100 ;
                               P.Remote_config.heart_beat_frequency = 2.0 ;
                               P.Remote_config.heart_beat_timeout = 5.0 ;
                               P.Remote_config.connection_backlog = 10 ;
                               P.Remote_config.node_ip = "1.2.3.4" ;
                               P.Remote_config.remote_nodes = [] ;
                             } in 
  let expected_exception_happened = ref false in        
  let test_proc () = P.(      
      catch 
        (fun () -> receive []) 
        (function
          | Empty_matchers -> expected_exception_happened := true ; return None
          | _ -> assert false
        ) >>= fun _ ->           
      return ()
    ) in            
  Lwt.(
    test_run_wrapper (fun () -> 
      P.run_node node_config ~process:test_proc >>= fun () -> 
      (get_option !exit_fn) () >>= fun () ->
      assert_bool "expected empty matchers exception did not occur" !expected_exception_happened;   
      assert_equal ~msg:"remote config with only a single node should have establised 1 connection" 1 (Hashtbl.length established_connections) ;
      
      return () 
    )
  )

let test_empty_matchers_remote_remote _ =
  let module Producer = Distributed.Make (Test_io) (M) in
  let module Consumer = Distributed.Make (Test_io) (M) in  
  let node_config = Producer.Remote { Producer.Remote_config.node_name = "producer" ; 
                                      Producer.Remote_config.local_port = 100 ;
                                      Producer.Remote_config.heart_beat_frequency = 200.0 ;
                                      Producer.Remote_config.heart_beat_timeout = 500.0 ;
                                      Producer.Remote_config.connection_backlog = 10 ;
                                      Producer.Remote_config.node_ip = "1.2.3.4" ;
                                      Producer.Remote_config.remote_nodes = [("5.6.7.8",101,"consumer")] ;
                                    } in
  let remote_config = Consumer.Remote { Consumer.Remote_config.node_name = "consumer" ; 
                                        Consumer.Remote_config.local_port = 101 ;
                                        Consumer.Remote_config.heart_beat_frequency = 200.0 ;
                                        Consumer.Remote_config.heart_beat_timeout = 500.0 ;
                                        Consumer.Remote_config.connection_backlog = 10 ;
                                        Consumer.Remote_config.node_ip = "5.6.7.8" ;
                                        Consumer.Remote_config.remote_nodes = [] ;
                                      } in
  let consumer_proc () = Consumer.(
      return () >>= fun _ ->
      return (
        let (conns,server_fn) = Hashtbl.find established_connections (Unix.ADDR_INET (Unix.inet6_addr_any , 101)) in
        Hashtbl.remove established_connections (Unix.ADDR_INET (Unix.inet6_addr_any , 101)) ;
        Hashtbl.replace established_connections (Unix.ADDR_INET (Unix.inet_addr_of_string "5.6.7.8" , 101)) (conns,server_fn) ; 
      )
    ) in
  let expected_exception_happened = ref false in    
  let producer_proc () = Producer.(
      return () >>= fun _ ->
      return (
        let (conns,server_fn) = Hashtbl.find established_connections (Unix.ADDR_INET (Unix.inet6_addr_any , 100)) in
        Hashtbl.remove established_connections (Unix.ADDR_INET (Unix.inet6_addr_any , 100)) ;
        Hashtbl.replace established_connections (Unix.ADDR_INET (Unix.inet_addr_of_string "1.2.3.4" , 100)) (conns,server_fn) ; 
      ) >>= fun _ ->
      catch 
        (fun () -> receive []) 
        (function
          | Empty_matchers -> expected_exception_happened := true ; return None
          | _ -> assert false
        ) >>= fun _ ->           
      return ()       
    ) in      
  Lwt.(
    test_run_wrapper (fun () -> 
      Lwt.async (fun () -> Consumer.run_node remote_config ~process:consumer_proc) ;            
      Producer.run_node node_config ~process:producer_proc >>= fun () ->
      (get_option !exit_fn) () >>= fun () ->
      assert_bool "expected empty matchers exception did not occur" !expected_exception_happened;   
      assert_equal ~msg:"remote config with 2 remote nodes should have establised 2 connections" 2 (Hashtbl.length established_connections) ;
      
      return () 
    ) 
  )                               

(* tests for raise excpetions for local and remote configurations *)

let test_raise_local_config _ =
  let module P = Distributed.Make (Test_io) (M) in
  let node_config = P.Local {P.Local_config.node_name = "test" ;} in
  let expected_exception_happened = ref false in 
  let receive_exception_proc () = P.(
    receive [
      case (fun _ -> Some (fun () -> fail Test_ex) ) ;
    ] >>= fun _ -> return ()
  ) in
  let test_proc () = P.(                      
      get_self_node >>= fun local_node ->
      spawn ~monitor:true local_node (fun () -> return () >>= fun _ -> receive_exception_proc ()) >>= fun (new_pid, _) ->
      new_pid >! "foobar" >>= fun _ ->
      receive [
        termination_case 
          (function
            | Exception (_,Test_ex) -> return (expected_exception_happened := true) ;
            | _ -> assert false
          )
      ] >>= fun _ ->            
      return ()        
    ) in             
  Lwt.(test_run_wrapper (fun () -> P.run_node node_config ~process:test_proc >>= fun () -> (get_option !exit_fn) ())) ;
  assert_bool "expceted exception did not occur" !expected_exception_happened ;
  assert_equal ~msg:"local config should hav establised 0 connections" 0 (Hashtbl.length established_connections) ;
  assert_equal 0 (Hashtbl.length established_connections)    

let test_raise_local_remote_config _ =
  let module P = Distributed.Make (Test_io) (M) in
  let node_config = P.Remote { P.Remote_config.node_name = "producer" ; 
                               P.Remote_config.local_port = 100 ;
                               P.Remote_config.heart_beat_frequency = 200.0 ;
                               P.Remote_config.heart_beat_timeout = 500.0 ;
                               P.Remote_config.connection_backlog = 10 ;
                               P.Remote_config.node_ip = "1.2.3.4" ;
                               P.Remote_config.remote_nodes = [] ;
                             } in
  let expected_exception_happened = ref false in 
  let receive_exception_proc () = P.(
    receive ~timeout_duration:0.05 [
      case (fun _ -> Some (fun () -> fail Test_ex) ) ;
    ] >>= fun _ -> return ()
  ) in 
  let test_proc () = P.(                      
      get_self_node >>= fun local_node ->
      spawn ~monitor:true local_node (fun () -> return () >>= fun _ -> receive_exception_proc ()) >>= fun (new_pid, _) ->
      new_pid >! "foobar" >>= fun _ ->
      receive [
        termination_case 
          (function
            | Exception (_,Test_ex) -> return (expected_exception_happened := true) ;
            | _ -> assert false
          )
      ] >>= fun _ ->            
      return ()        
    ) in        
  Lwt.(
    test_run_wrapper (fun () -> 
      P.run_node node_config ~process:test_proc >>= fun () -> 
      (get_option !exit_fn) () >>= fun () ->
      assert_bool "expceted exception did not occur" !expected_exception_happened ;  
      assert_equal ~msg:"remote config with only a single node should have establised 1 connection" 1 (Hashtbl.length established_connections) ;  
      
      return ()    
    )
  )

(* the following tests uses a workaround (compare their constructor names) to compare excpetions since
   exceptions which are unmarshalled can't be pattern matched against 
   see http://caml.inria.fr/pub/docs/manual-ocaml/libref/Marshal.html
*)
let test_raise_remote_remote_config _ =
  let module Producer = Distributed.Make (Test_io) (M) in
  let module Consumer = Distributed.Make (Test_io) (M) in  
  let node_config = Producer.Remote { Producer.Remote_config.node_name = "producer" ; 
                                      Producer.Remote_config.local_port = 100 ;
                                      Producer.Remote_config.heart_beat_frequency = 200.0 ;
                                      Producer.Remote_config.heart_beat_timeout = 500.0 ;
                                      Producer.Remote_config.connection_backlog = 10 ;
                                      Producer.Remote_config.node_ip = "1.2.3.4" ;
                                      Producer.Remote_config.remote_nodes = [("5.6.7.8",101,"consumer")] ;
                                    } in
  let remote_config = Consumer.Remote { Consumer.Remote_config.node_name = "consumer" ; 
                                        Consumer.Remote_config.local_port = 101 ;
                                        Consumer.Remote_config.heart_beat_frequency = 200.0 ;
                                        Consumer.Remote_config.heart_beat_timeout = 500.0 ;
                                        Consumer.Remote_config.connection_backlog = 10 ;
                                        Consumer.Remote_config.node_ip = "5.6.7.8" ;
                                        Consumer.Remote_config.remote_nodes = [] ;
                                      } in
  let expected_exception_happened = ref false in                                 
  let consumer_proc () = Consumer.(
      return () >>= fun _ ->
      return (
        let (conns,server_fn) = Hashtbl.find established_connections (Unix.ADDR_INET (Unix.inet6_addr_any , 101)) in
        Hashtbl.remove established_connections (Unix.ADDR_INET (Unix.inet6_addr_any , 101)) ;
        Hashtbl.replace established_connections (Unix.ADDR_INET (Unix.inet_addr_of_string "5.6.7.8" , 101)) (conns,server_fn) ; 
      )
    ) in    
  let receive_exception_proc () = Consumer.(
      receive [
        case (fun _ -> Some (fun () -> fail Test_ex) ) ;
      ] >>= fun _ -> return ()
    ) in 
  let producer_proc () = Producer.(
      return () >>= fun _ ->
      return (
        let (conns,server_fn) = Hashtbl.find established_connections (Unix.ADDR_INET (Unix.inet6_addr_any , 100)) in
        Hashtbl.remove established_connections (Unix.ADDR_INET (Unix.inet6_addr_any , 100)) ;
        Hashtbl.replace established_connections (Unix.ADDR_INET (Unix.inet_addr_of_string "1.2.3.4" , 100)) (conns,server_fn) ; 
      ) >>= fun _ -> 
      get_remote_nodes >>= fun nodes ->
      get_self_pid >>= fun _ ->
      spawn ~monitor:true (List.hd nodes) (Consumer.(fun () -> return () >>= fun () -> receive_exception_proc ())) >>= fun (remote_pid, _) ->      
      remote_pid >! "foobar" >>= fun _ ->
      receive [
        termination_case 
          (function
            | Exception (_,ex) ->
              begin
                if (Printexc.exn_slot_name ex) = (Printexc.exn_slot_name Test_ex)
                then return (expected_exception_happened := true)
                else assert false
              end 
            | _ -> assert false
          )
      ] >>= fun _ ->
      return ()                       
    ) in
  Lwt.(
    test_run_wrapper (fun () -> 
      Lwt.async (fun () -> Consumer.run_node remote_config ~process:consumer_proc) ;                
      Producer.run_node node_config ~process:producer_proc >>= fun () ->
      (get_option !exit_fn) () >>= fun () ->      
      assert_bool "expceted exception did not occur" !expected_exception_happened ;  
      assert_equal ~msg:"remote config with 2 remote nodes should have establised 2 connections" 2 (Hashtbl.length established_connections) ;      
      
      return () 
    ) 
  )

(* monitor tests for local and remote configurations when monitored processes has already ended *)

let test_monitor_dead_process_local_local_config _ =
  let module P = Distributed.Make (Test_io) (M) in
  let node_config = P.Local {P.Local_config.node_name = "test" ;} in
  let result = ref false in
  let result_monitor = ref None in  
  let test_proc () = P.(                                        
      get_self_node >>= fun local_node ->
      assert_bool "Process should not have spawned yet" (not !result) ;
      spawn ~monitor:true local_node (fun () -> return () >>= fun _ -> return (result := true)) >>= fun (new_pid, _) ->
      receive [
        termination_case
        (function
          | Normal _ -> return @@ Some ()
          | _ -> assert false
        )
      ] >>= fun _ ->
      monitor new_pid >>= fun _ ->
      receive [
        termination_case
          (function
            | NoProcess _ -> return (result_monitor := Some ("got noprocess"))
            | _ -> assert false
          )
      ] >>= fun _ ->
      return ()        
    ) in             
  Lwt.(test_run_wrapper (fun () -> P.run_node node_config ~process:test_proc >>= fun () -> (get_option !exit_fn) ()));
  assert_bool "process was not spawned" (!result && !result_monitor <> None) ; 
  assert_equal ~msg:"did not get expected NoProcess monitor message" (Some "got noprocess") !result_monitor ;    
  assert_equal ~msg:"local config should hav establised 0 connections" 0 (Hashtbl.length established_connections)    

let test_monitor_dead_process_local_remote_config _ =
  let module P = Distributed.Make (Test_io) (M) in  
  let node_config = P.Remote { P.Remote_config.node_name = "producer" ; 
                               P.Remote_config.local_port = 100 ;
                               P.Remote_config.heart_beat_frequency = 2.0 ;
                               P.Remote_config.heart_beat_timeout = 5.0 ;
                               P.Remote_config.connection_backlog = 10 ;
                               P.Remote_config.node_ip = "1.2.3.4" ;
                               P.Remote_config.remote_nodes = [] ;
                             } in
  let result = ref false in
  let result_monitor = ref None in  
  let test_proc () = P.(                                        
      get_self_node >>= fun local_node ->
      assert_bool "Process should not have spawned yet" (not !result) ;
      spawn ~monitor:true local_node (fun () -> return () >>= fun _ -> return (result := true)) >>= fun (new_pid, _) ->
      receive [
        termination_case
        (function
          | Normal _ -> return @@ Some ()
          | _ -> assert false
        )
      ] >>= fun _ ->      
      monitor new_pid >>= fun _ ->
      receive [
        termination_case
          (function
            | NoProcess _ -> return (result_monitor := Some ("got noprocess"))
            | _ -> assert false
          )
      ] >>= fun _ ->
      return ()        
    ) in             
  Lwt.(
    test_run_wrapper (fun () -> 
      P.run_node node_config ~process:test_proc >>= fun () -> 
      (get_option !exit_fn) () >>= fun () ->
      assert_bool "process was not spawned" (!result && !result_monitor <> None) ;
      assert_equal ~msg:"did not get expected NoProcess monitor message" (Some "got noprocess") !result_monitor ;      
      assert_equal ~msg:"remote config with only a single node should have establised 1 connection" 1 (Hashtbl.length established_connections) ;
      
      return ()    
    )
  )

let test_monitor_dead_process_remote_remote_config _ =
  let module Producer = Distributed.Make (Test_io) (M) in
  let module Consumer = Distributed.Make (Test_io) (M) in  
  let node_config = Producer.Remote { Producer.Remote_config.node_name = "producer" ; 
                                      Producer.Remote_config.local_port = 100 ;
                                      Producer.Remote_config.heart_beat_frequency = 200.0 ;
                                      Producer.Remote_config.heart_beat_timeout = 500.0 ;
                                      Producer.Remote_config.connection_backlog = 10 ;
                                      Producer.Remote_config.node_ip = "1.2.3.4" ;
                                      Producer.Remote_config.remote_nodes = [("5.6.7.8",101,"consumer")] ;
                                    } in
  let remote_config = Consumer.Remote { Consumer.Remote_config.node_name = "consumer" ; 
                                        Consumer.Remote_config.local_port = 101 ;
                                        Consumer.Remote_config.heart_beat_frequency = 200.0 ;
                                        Consumer.Remote_config.heart_beat_timeout = 500.0 ;
                                        Consumer.Remote_config.connection_backlog = 10 ;
                                        Consumer.Remote_config.node_ip = "5.6.7.8" ;
                                        Consumer.Remote_config.remote_nodes = [] ;
                                      } in
  let consumer_proc () = Consumer.(
      return () >>= fun _ ->
      return (
        let (conns,server_fn) = Hashtbl.find established_connections (Unix.ADDR_INET (Unix.inet6_addr_any , 101)) in
        Hashtbl.remove established_connections (Unix.ADDR_INET (Unix.inet6_addr_any , 101)) ;
        Hashtbl.replace established_connections (Unix.ADDR_INET (Unix.inet_addr_of_string "5.6.7.8" , 101)) (conns,server_fn) ; 
      )
    ) in    
  let result_monitor = ref None in    
  let producer_proc () = Producer.(
      return () >>= fun _ ->
      return (
        let (conns,server_fn) = Hashtbl.find established_connections (Unix.ADDR_INET (Unix.inet6_addr_any , 100)) in
        Hashtbl.remove established_connections (Unix.ADDR_INET (Unix.inet6_addr_any , 100)) ;
        Hashtbl.replace established_connections (Unix.ADDR_INET (Unix.inet_addr_of_string "1.2.3.4" , 100)) (conns,server_fn) ; 
      ) >>= fun _ -> 
      get_remote_nodes >>= fun nodes ->
      spawn ~monitor:true (List.hd nodes) (fun () -> return () >>= fun () -> return ()) >>= fun (remote_pid, _) ->
      receive [
        termination_case
        (function
          | Normal _ -> return @@ Some ()
          | _ -> assert false
        )
      ] >>= fun _ ->
      monitor remote_pid >>= fun _ ->      
      receive [
        termination_case
          (function
            | NoProcess _ -> return (result_monitor := Some ("got noprocess"))
            | _ -> assert false
          )
      ] >>= fun _ ->
      return ()                 
    ) in
  Lwt.(
    test_run_wrapper (fun () -> 
      Lwt.async (fun () -> Consumer.run_node remote_config ~process:consumer_proc) ;            
      Producer.run_node node_config ~process:producer_proc >>= fun () ->
      (get_option !exit_fn) () >>= fun () ->      
      assert_equal ~msg:"did not get expected NoProcess monitor message" (Some "got noprocess") !result_monitor ;      
      assert_equal ~msg:"remote config with 2 remote nodes should have establised 2 connections" 2 (Hashtbl.length established_connections) ;
      
      return () 
    ) 
  )                       

(* add/remove remote nodes in local config tests *)

let test_add_remove_remote_nodes_in_local_config _ =
  let module P = Distributed.Make (Test_io) (M) in
  let node_config = P.Local {P.Local_config.node_name = "test" ;} in
  let add_pass = ref false in
  let remove_pass = ref false in
  let test_proc () = P.(                                        
      catch
        (fun () -> 
           add_remote_node "9.9.9.9" 7777 "foobar" >>= fun _ ->
           return () 
        )
        (function
          | Local_only_mode -> 
            catch 
              (fun () ->
                 add_pass := true ;
                 get_self_node >>= fun local_node ->
                 remove_remote_node local_node >>= fun _ ->
                 return ()
              )
              (function
                | Local_only_mode -> return (remove_pass := true)
                | _ -> assert false
              )
          | _ -> assert false  
        )
      >>= fun _ ->
      return ()        
    ) in             
  Lwt.(test_run_wrapper (fun () -> P.run_node node_config ~process:test_proc >>= fun () -> (get_option !exit_fn) ()));
  assert_bool "add_remote_node should have throw Local_only_mode exception when running with a local only config" !add_pass ; 
  assert_bool "remove_remote_node should have throw Local_only_mode exception when running with a local only config" !remove_pass ;    
  assert_equal ~msg:"local config should hav establised 0 connections" 0 (Hashtbl.length established_connections)    

(* test adding/removing nodes in remote configurations. *)

let test_add_remove_nodes_remote_config _ =
  let module Producer = Distributed.Make (Test_io) (M) in
  let module Consumer = Distributed.Make (Test_io) (M) in  
  let node_config = Producer.Remote { Producer.Remote_config.node_name = "producer" ; 
                                      Producer.Remote_config.local_port = 100 ;
                                      Producer.Remote_config.heart_beat_frequency = 200.0 ;
                                      Producer.Remote_config.heart_beat_timeout = 500.0 ;
                                      Producer.Remote_config.connection_backlog = 10 ;
                                      Producer.Remote_config.node_ip = "1.2.3.4" ;
                                      Producer.Remote_config.remote_nodes = [] ; (* start with with no node connections *)
                                    } in
  let remote_config = Consumer.Remote { Consumer.Remote_config.node_name = "consumer" ; 
                                        Consumer.Remote_config.local_port = 101 ;
                                        Consumer.Remote_config.heart_beat_frequency = 200.0 ;
                                        Consumer.Remote_config.heart_beat_timeout = 500.0 ;
                                        Consumer.Remote_config.connection_backlog = 10 ;
                                        Consumer.Remote_config.node_ip = "5.6.7.8" ;
                                        Consumer.Remote_config.remote_nodes = [] ;
                                      } in
  let consumer_proc () = Consumer.(
      return () >>= fun _ ->
      let (conns,server_fn) = Hashtbl.find established_connections (Unix.ADDR_INET (Unix.inet6_addr_any , 101)) in
      Hashtbl.remove established_connections (Unix.ADDR_INET (Unix.inet6_addr_any , 101)) ;
      Hashtbl.replace established_connections (Unix.ADDR_INET (Unix.inet_addr_of_string "5.6.7.8" , 101)) (conns,server_fn) ; 
      lift_io @@ Test_io.sleep 0.1 (* sleep to let producer node add/remove consumer *)
    ) in    
  let remote_nodes_at_start = ref [] in
  let remote_nodes_after_remove = ref [] in
  let remote_nodes_after_add = ref [] in   
  let remote_nodes_after_dup_add = ref [] in   
  let expected_spawn_exception = ref false in
  let expected_monitor_exception = ref false in
  let expected_broadcast_exception = ref false in   
  let expected_send_exception = ref false in
  let producer_proc () = Producer.(
      return () >>= fun _ ->
      return (
        let (conns,server_fn) = Hashtbl.find established_connections (Unix.ADDR_INET (Unix.inet6_addr_any , 100)) in
        Hashtbl.remove established_connections (Unix.ADDR_INET (Unix.inet6_addr_any , 100)) ;
        Hashtbl.replace established_connections (Unix.ADDR_INET (Unix.inet_addr_of_string "1.2.3.4" , 100)) (conns,server_fn) ; 
      ) >>= fun _ -> 
      get_remote_nodes >>= fun nodes ->
      remote_nodes_at_start := nodes ;
      add_remote_node "5.6.7.8" 101 "consumer" >>= fun consumer_node ->
      get_remote_nodes >>= fun nodes_after_add ->
      remote_nodes_after_add := nodes_after_add ;
      add_remote_node "5.6.7.8" 101 "consumer" >>= fun _ ->
      get_remote_nodes >>= fun nodes_after_add_dup ->
      remote_nodes_after_dup_add := nodes_after_add_dup ;
      spawn consumer_node (fun () -> lift_io @@ Test_io.sleep 0.1) >>= fun (rpid,_) ->
      remove_remote_node consumer_node >>= fun () ->
      get_remote_nodes >>= fun nodes_after_remove ->
      remote_nodes_after_remove := nodes_after_remove ;
      catch
        (fun () -> monitor rpid >>= fun _ -> return ())
        (function
          | InvalidNode n -> if Distributed.Node_id.get_name n = "consumer" then return (expected_monitor_exception := true) else return ()
          | _ -> assert false      
        ) >>= fun () ->
      catch
        (fun () -> spawn consumer_node (fun () -> return () >>= fun () -> return ()) >>= fun _ -> return ())
        (function
          | InvalidNode n -> if Distributed.Node_id.get_name n = "consumer" then return (expected_spawn_exception := true) else return ()
          | _ -> assert false      
        ) >>= fun () ->
      catch
        (fun () -> broadcast consumer_node "a broadcast message")
        (function
          | InvalidNode n -> if Distributed.Node_id.get_name n = "consumer" then return (expected_broadcast_exception := true) else return ()
          | _ -> assert false      
        ) >>= fun () ->
      catch
        (fun () -> send rpid "a message")
        (function
          | InvalidNode n -> if Distributed.Node_id.get_name n = "consumer" then return (expected_send_exception := true) else return ()
          | _ -> assert false      
        )
    ) in
  Lwt.(
    test_run_wrapper (fun () -> 
      Lwt.async (fun () -> Consumer.run_node remote_config ~process:consumer_proc) ;            
      Producer.run_node node_config ~process:producer_proc >>= fun () ->
      (get_option !exit_fn) () >>= fun () ->      
      assert_equal ~msg:"remote nodes should have been empty before adding" 0 (List.length !remote_nodes_at_start) ;
      assert_equal ~msg:"remote nodes should have 1 remote node after adding" 1 (List.length !remote_nodes_after_add) ;      
      assert_equal ~msg:"remote nodes should have 1 remote node after adding a dup" 1 (List.length !remote_nodes_after_dup_add) ;      
      assert_equal ~msg:"remote nodes should have been empty after removing" 0 (List.length !remote_nodes_after_remove) ;
      assert_bool "expected InvalidNode exception did not occur when monitoring on removed node" !expected_monitor_exception ;
      assert_bool "expected InvalidNode exception did not occur when spawning on removed node" !expected_spawn_exception ;
      assert_bool "expected InvalidNode exception did not occur when broadcasting on removed node" !expected_broadcast_exception ;
      assert_bool "expected InvalidNode exception did not occur when sending message on removed node" !expected_send_exception ;            
      assert_equal ~msg:"remote config with 2 remote nodes should have establised 2 connections" 2 (Hashtbl.length established_connections) ;
      
      return () 
    ) 
  )         

(* test selective receive, test that matching against a message will leve the others in the correct order. *)

let test_selective_receive_local_config _ =
  let module P = Distributed.Make (Test_io) (M) in
  let node_config = P.Local {P.Local_config.node_name = "test" ;} in
  let selective_message = ref None in
  let other_messages_inorder = ref [] in  
  let receiver_proc () = P.(                      
      receive [
        case (
          function 
          | "the one" -> Some (fun () -> return (selective_message := Some "the one"))
          | _ -> None
        ) 
      ] >>= fun _ ->
      receive_loop ~timeout_duration:0.1 [
        case (fun v -> Some (fun () -> other_messages_inorder := (v::(!other_messages_inorder)) ; return (v <> "0")))
      ]         
    ) in             
  let sender_proc receiver_pid () = P.(
      receiver_pid >! "5" >>= fun () ->
      receiver_pid >! "4" >>= fun () ->
      receiver_pid >! "3" >>= fun () ->
      receiver_pid >! "the one" >>= fun () ->
      receiver_pid >! "2" >>= fun () ->
      receiver_pid >! "1" >>= fun () ->
      receiver_pid >! "0"    
    ) in
  let main_proc () = P.(
      get_self_node >>= fun self_node ->
      spawn self_node receiver_proc >>= fun (rpid,_) ->
      spawn self_node (sender_proc rpid) >>= fun _ ->
      lift_io (Test_io.sleep 0.2) >>= fun () ->
      return () 
    ) in
  Lwt.(test_run_wrapper (fun () -> P.run_node node_config ~process:main_proc >>= fun () -> (get_option !exit_fn) ())) ;
  assert_equal ~msg:"selective receive failed, candidate message" (Some "the one") !selective_message ;
  assert_equal ~msg:"selective receive failed, other messages" ["0" ; "1" ; "2" ; "3" ; "4" ; "5"] !other_messages_inorder ;
  assert_equal ~msg:"local config should hav establised 0 connections" 0 (Hashtbl.length established_connections) ;
  assert_equal 0 (Hashtbl.length established_connections)        

let test_selective_receive_local_remote_config _ =
  let module P = Distributed.Make (Test_io) (M) in
  let node_config = P.Remote { P.Remote_config.node_name = "producer" ; 
                               P.Remote_config.local_port = 100 ;
                               P.Remote_config.heart_beat_frequency = 2.0 ;
                               P.Remote_config.heart_beat_timeout = 5.0 ;
                               P.Remote_config.connection_backlog = 10 ;
                               P.Remote_config.node_ip = "1.2.3.4" ;
                               P.Remote_config.remote_nodes = [] ;
                             } in
  let selective_message = ref None in
  let other_messages_inorder = ref [] in  
  let receiver_proc () = P.(                      
      receive [
        case (
          function 
          | "the one" -> Some (fun () -> return (selective_message := Some "the one"))
          | _ -> None
        ) 
      ] >>= fun _ ->
      receive_loop ~timeout_duration:0.1 [
        case (fun v -> Some (fun () -> other_messages_inorder := (v::(!other_messages_inorder)) ; return (v <> "0")))
      ]         
    ) in             
  let sender_proc receiver_pid () = P.(
      receiver_pid >! "5" >>= fun () ->
      receiver_pid >! "4" >>= fun () ->
      receiver_pid >! "3" >>= fun () ->
      receiver_pid >! "the one" >>= fun () ->
      receiver_pid >! "2" >>= fun () ->
      receiver_pid >! "1" >>= fun () ->
      receiver_pid >! "0"    
    ) in
  let main_proc () = P.(
      get_self_node >>= fun self_node ->
      spawn self_node receiver_proc >>= fun (rpid,_) ->
      spawn self_node (sender_proc rpid) >>= fun _ ->
      lift_io (Test_io.sleep 0.2) >>= fun () ->
      return () 
    ) in
  Lwt.(
    test_run_wrapper (fun () -> 
      P.run_node node_config ~process:main_proc >>= fun () -> 
      (get_option !exit_fn) () >>= fun () ->
      assert_equal ~msg:"selective receive failed, candidate message" (Some "the one") !selective_message ;
      assert_equal ~msg:"selective receive failed, other messages" ["0" ; "1" ; "2" ; "3" ; "4" ; "5"] !other_messages_inorder ;
      assert_equal ~msg:"remote config with only a single node should have establised 1 connection" 1 (Hashtbl.length established_connections) ;
      
      return ()                      
    )
  )

let test_selective_receive_remote_remote_config _ =
  let module Producer = Distributed.Make (Test_io) (M) in
  let module Consumer = Distributed.Make (Test_io) (M) in  
  let node_config = Producer.Remote { Producer.Remote_config.node_name = "producer" ; 
                                      Producer.Remote_config.local_port = 100 ;
                                      Producer.Remote_config.heart_beat_frequency = 200.0 ;
                                      Producer.Remote_config.heart_beat_timeout = 500.0 ;
                                      Producer.Remote_config.connection_backlog = 10 ;
                                      Producer.Remote_config.node_ip = "1.2.3.4" ;
                                      Producer.Remote_config.remote_nodes = [("5.6.7.8",101,"consumer")] ;
                                    } in
  let remote_config = Consumer.Remote { Consumer.Remote_config.node_name = "consumer" ; 
                                        Consumer.Remote_config.local_port = 101 ;
                                        Consumer.Remote_config.heart_beat_frequency = 200.0 ;
                                        Consumer.Remote_config.heart_beat_timeout = 500.0 ;
                                        Consumer.Remote_config.connection_backlog = 10 ;
                                        Consumer.Remote_config.node_ip = "5.6.7.8" ;
                                        Consumer.Remote_config.remote_nodes = [] ;
                                      } in
  
  let messages_inorder = ref [] in 
                                                                            
  let consumer_proc () = Consumer.(
      return () >>= fun _ ->
      let (conns,server_fn) = Hashtbl.find established_connections (Unix.ADDR_INET (Unix.inet6_addr_any , 101)) in
      Hashtbl.remove established_connections (Unix.ADDR_INET (Unix.inet6_addr_any , 101)) ;
      Hashtbl.replace established_connections (Unix.ADDR_INET (Unix.inet_addr_of_string "5.6.7.8" , 101)) (conns,server_fn) ; 
      return ()      
    ) in 
  let receiver_proc result_pid () = Consumer.(
      let to_send = ref [] in
      let rec send_all msgs () =
        match msgs with
        | [] -> return ()
        | m::ms -> result_pid >! m >>= fun () -> send_all ms ()        
      in                      
      receive [
        case (
          function 
          | "the one" -> Some (fun () -> return (to_send := "the one"::!to_send))
          | _ -> None
        ) 
      ] >>= fun _ ->
      receive_loop ~timeout_duration:0.1 [
        case (fun v -> Some (fun () -> to_send := (v::(!to_send)) ; return (v <> "0")))
      ] >>= fun _ ->
      send_all !to_send ()         
    ) in             
  let sender_proc receiver_pid () = Producer.(
      receiver_pid >! "5" >>= fun () ->
      receiver_pid >! "4" >>= fun () ->
      receiver_pid >! "3" >>= fun () ->
      receiver_pid >! "the one" >>= fun () ->
      receiver_pid >! "2" >>= fun () ->
      receiver_pid >! "1" >>= fun () ->
      receiver_pid >! "0"    
    ) in    
    let result_receiver () = Producer.(
      receive_loop ~timeout_duration:0.1 [
        case (fun v -> Some (fun () -> messages_inorder := (v::(!messages_inorder)) ; return (v <> "the one")))
      ]      
    ) in
  let main_proc () = Producer.(
      get_self_node >>= fun self_node ->
      get_remote_nodes >>= fun remote_nodes ->
      spawn self_node result_receiver >>= fun (res_rec_pid,_) ->
      spawn (List.hd remote_nodes) (receiver_proc res_rec_pid) >>= fun (rpid,_) ->
      spawn self_node (sender_proc rpid) >>= fun _ ->
      lift_io (Test_io.sleep 0.1)
    ) in
  Lwt.(
    test_run_wrapper (fun () -> 
      Lwt.async (fun () -> Consumer.run_node remote_config ~process:consumer_proc) ;  
      Producer.run_node node_config ~process:main_proc >>= fun () ->
      (get_option !exit_fn) () >>= fun () ->  
      assert_equal ~msg:"selective receive failed, other messages"  ["the one" ; "5" ; "4" ; "3" ; "2" ; "1" ; "0"] !messages_inorder ;
      assert_equal ~msg:"remote config with 2 remote nodes should have establised 2 connections" 2 (Hashtbl.length established_connections) ;
      
      return ()
    ) 
  )   

(* test that calling run_node more than once results in an exception. *)

let test_multiple_run_node _ =
  let module P = Distributed.Make (Test_io) (M) in
  let node_config = P.Local {P.Local_config.node_name = "test" ;} in
  let exception_thrown = ref None in

  Lwt.(test_run_wrapper (fun () -> 
      P.run_node node_config >>= fun () ->
      catch
        (fun () -> P.run_node node_config)
        (function
          | P.Init_more_than_once -> exception_thrown := Some true ; return ()
          | _ -> return ()) >>= fun () -> (get_option !exit_fn) ()
    )) ;
  assert_equal ~msg:"Init more than once failed, did not get exception" (Some true) !exception_thrown ;
  assert_equal 0 (Hashtbl.length established_connections) 

(* test get_remote_node*)

let test_get_remote_node_local_only _ =
  let module P = Distributed.Make (Test_io) (M) in
  let node_config = P.Local {P.Local_config.node_name = "test" ; } in
  let nonexistent_remote_node_result = ref (Some "") in
  let self_remote_node_result = ref (Some "") in

  let p () = P.(                      
      get_remote_node "test" >>= (function
          | None -> self_remote_node_result := Some "ran" ; return ()
          | Some _ -> self_remote_node_result := Some "fail" ; return ()) >>= fun () ->
      get_remote_node "foobar" >>= (function
          | None -> nonexistent_remote_node_result := Some "ran" ; return ()
          | Some _ -> nonexistent_remote_node_result := Some "fail" ; return ())
    ) in 

  Lwt.(test_run_wrapper (fun () -> (P.run_node node_config ~process:p)  >>= fun _ -> (get_option !exit_fn) ())) ;
  assert_equal ~msg:"get_remote_node failed locally, self node was in remote nodes" (Some "ran") !self_remote_node_result ;
  assert_equal ~msg:"get_remote_node failed locally, nonexistent node was in remote nodes" (Some "ran") !nonexistent_remote_node_result ;
  assert_equal 0 (Hashtbl.length established_connections)

let test_get_remote_node_local_remote_config _ =
  let module P = Distributed.Make (Test_io) (M) in
  let node_config = P.Remote { P.Remote_config.node_name = "producer" ; 
                               P.Remote_config.local_port = 100 ;
                               P.Remote_config.heart_beat_frequency = 2.0 ;
                               P.Remote_config.heart_beat_timeout = 5.0 ;
                               P.Remote_config.connection_backlog = 10 ;
                               P.Remote_config.node_ip = "1.2.3.4" ;
                               P.Remote_config.remote_nodes = [] ;
                             } in
  let nonexistent_remote_node_result = ref (Some "") in
  let self_remote_node_result = ref (Some "") in

  let p () = P.(                      
      get_remote_node "test" >>= (function
          | None -> self_remote_node_result := Some "ran" ; return ()
          | Some _ -> self_remote_node_result := Some "fail" ; return ()) >>= fun () ->
      get_remote_node "foobar" >>= (function
          | None -> nonexistent_remote_node_result := Some "ran" ; return ()
          | Some _ -> nonexistent_remote_node_result := Some "fail" ; return ())
    ) in 
  Lwt.(
    test_run_wrapper (fun () -> 
      (P.run_node node_config ~process:p)  >>= fun () -> 
      (get_option !exit_fn) () >>= fun () ->
      assert_equal ~msg:"get_remote_node failed locally with remote config, self node was in remote nodes" (Some "ran") !self_remote_node_result ;
      assert_equal ~msg:"get_remote_node failed locally with remote config, nonexistent node was in remote nodes" (Some "ran") !nonexistent_remote_node_result ;
      
      return ()  
    )
  )

let test_get_remote_node_remote_remote_config _ =
  let module Producer = Distributed.Make (Test_io) (M) in
  let module Consumer = Distributed.Make (Test_io) (M) in  
  let node_config = Producer.Remote { Producer.Remote_config.node_name = "producer" ; 
                                      Producer.Remote_config.local_port = 100 ;
                                      Producer.Remote_config.heart_beat_frequency = 200.0 ;
                                      Producer.Remote_config.heart_beat_timeout = 500.0 ;
                                      Producer.Remote_config.connection_backlog = 10 ;
                                      Producer.Remote_config.node_ip = "1.2.3.4" ;
                                      Producer.Remote_config.remote_nodes = [("5.6.7.8",101,"consumer")] ;
                                    } in
  let remote_config = Consumer.Remote { Consumer.Remote_config.node_name = "consumer" ; 
                                        Consumer.Remote_config.local_port = 101 ;
                                        Consumer.Remote_config.heart_beat_frequency = 200.0 ;
                                        Consumer.Remote_config.heart_beat_timeout = 500.0 ;
                                        Consumer.Remote_config.connection_backlog = 10 ;
                                        Consumer.Remote_config.node_ip = "5.6.7.8" ;
                                        Consumer.Remote_config.remote_nodes = [] ;
                                      } in

  let nonexistent_remote_node_result = ref (Some "") in
  let self_remote_node_result = ref (Some "") in
  let exitent_remote_node_result = ref (Some "") in

  let consumer_proc () = Consumer.(
      return () >>= fun _ ->
      return (
        let (conns,server_fn) = Hashtbl.find established_connections (Unix.ADDR_INET (Unix.inet6_addr_any , 101)) in
        Hashtbl.remove established_connections (Unix.ADDR_INET (Unix.inet6_addr_any , 101)) ;
        Hashtbl.replace established_connections (Unix.ADDR_INET (Unix.inet_addr_of_string "5.6.7.8" , 101)) (conns,server_fn) ; 
      )
    ) in  

  let p2 () = Consumer.(    
      exitent_remote_node_result := Some "ran" ; return ()    
    ) in 

  let p () = Producer.(      
      return () >>= fun _ ->
      return (
        let (conns,server_fn) = Hashtbl.find established_connections (Unix.ADDR_INET (Unix.inet6_addr_any , 100)) in
        Hashtbl.remove established_connections (Unix.ADDR_INET (Unix.inet6_addr_any , 100)) ;
        Hashtbl.replace established_connections (Unix.ADDR_INET (Unix.inet_addr_of_string "1.2.3.4" , 100)) (conns,server_fn) ; 
      ) >>= fun _ -> 
      get_remote_node "test" >>= (function
          | None -> self_remote_node_result := Some "ran" ; return ()
          | Some _ -> self_remote_node_result := Some "fail" ; return ()) >>= fun () ->
      get_remote_node "foobar" >>= (function
          | None -> nonexistent_remote_node_result := Some "ran" ; return ()
          | Some _ -> nonexistent_remote_node_result := Some "fail" ; return ()) >>= fun () ->
      get_remote_node "consumer" >>= (function
          | None -> exitent_remote_node_result := Some "fail" ; return ()
          | Some n -> 
            (spawn ~monitor:true n p2 >>= fun (_,_) ->
             receive [termination_case (function _ -> return ())]) >>= fun _ -> return ())
    ) in 

  Lwt.(
    test_run_wrapper (fun () -> 
      Lwt.async (fun () -> Consumer.run_node remote_config ~process:consumer_proc) ;  
      Producer.run_node node_config ~process:p >>= fun () ->
      (get_option !exit_fn) () >>= fun () ->      
      assert_equal ~msg:"get_remote_node failed remotely, self node was in remote nodes" (Some "ran") !self_remote_node_result ;
      assert_equal ~msg:"get_remote_node failed remotely, nonexistent node was in remote nodes" (Some "ran") !nonexistent_remote_node_result ;
      assert_equal ~msg:"get_remote_node failed remotely, existent node was not in remote nodes" (Some "ran") !exitent_remote_node_result ;
      assert_equal ~msg:"remote config with 2 remote nodes should have establised 2 connections" 2 (Hashtbl.length established_connections) ;
      
      return () 
    ) 
  )    

(* test heart beat process/receive *)
let test_heart_beat _ =
  let module Producer = Distributed.Make (Test_io) (M) in
  let module Consumer = Distributed.Make (Test_io) (M) in  
  let node_config = Producer.Remote { Producer.Remote_config.node_name = "producer" ; 
                                      Producer.Remote_config.local_port = 100 ;
                                      Producer.Remote_config.heart_beat_frequency = 0.3 ;
                                      Producer.Remote_config.heart_beat_timeout = 0.12 ;
                                      Producer.Remote_config.connection_backlog = 10 ;
                                      Producer.Remote_config.node_ip = "1.2.3.4" ;
                                      Producer.Remote_config.remote_nodes = [("5.6.7.8",101,"consumer")] ;
                                    } in
  let remote_config = Consumer.Remote { Consumer.Remote_config.node_name = "consumer" ; 
                                        Consumer.Remote_config.local_port = 101 ;
                                        Consumer.Remote_config.heart_beat_frequency = 0.1 ;
                                        Consumer.Remote_config.heart_beat_timeout = 0.1 ;
                                        Consumer.Remote_config.connection_backlog = 10 ;
                                        Consumer.Remote_config.node_ip = "5.6.7.8" ;
                                        Consumer.Remote_config.remote_nodes = [] ;
                                      } in

  let node_at_start_consumer = ref [] in  
  let node_after_02_millseconds_consumer = ref [] in
  let node_at_start_producer = ref [] in
  let node_after_015_millseconds_producer = ref [] in
  let node_at_03_milliseconds_producer = ref [] in
  let node_went_down_consumer = ref None in
  let node_went_down_producer = ref None in
  let invalid_node_exception_spawn = ref None in
  let invalid_node_exception_monitor = ref None in

  let monitor_fn_consumer n = Consumer.(
     return (node_went_down_consumer := Some (Distributed.Node_id.get_name n))
  ) in

  let monitor_fn_producer n = Producer.(
     return (node_went_down_producer := Some (Distributed.Node_id.get_name n))
  ) in

  let consumer_proc () = Consumer.(
      return () >>= fun _ ->
      let (conns,server_fn) = Hashtbl.find established_connections (Unix.ADDR_INET (Unix.inet6_addr_any , 101)) in
      Hashtbl.remove established_connections (Unix.ADDR_INET (Unix.inet6_addr_any , 101)) ;
      Hashtbl.replace established_connections (Unix.ADDR_INET (Unix.inet_addr_of_string "5.6.7.8" , 101)) (conns,server_fn) ;
      lift_io (Test_io.sleep 0.05) >>= fun () -> (* sleep a little bit to allow for the producer to connect *)      
      get_remote_nodes >>= fun nodes_at_start ->
      node_at_start_consumer := nodes_at_start ;
      lift_io (Test_io.sleep 0.15) >>= fun () ->
      get_remote_nodes >>= fun nodes_at_02 ->
      node_after_02_millseconds_consumer := nodes_at_02 ;
      catch 
        (fun () -> spawn (List.hd nodes_at_start) (fun () -> return () >>= fun _ -> return ()) >>= fun _ -> return ())
        (function | InvalidNode _ -> return (invalid_node_exception_spawn := Some true) | _ -> assert false)      
    ) in  

  let p () = Producer.(      
      return () >>= fun _ ->
      return (
        let (conns,server_fn) = Hashtbl.find established_connections (Unix.ADDR_INET (Unix.inet6_addr_any , 100)) in
        Hashtbl.remove established_connections (Unix.ADDR_INET (Unix.inet6_addr_any , 100)) ;
        Hashtbl.replace established_connections (Unix.ADDR_INET (Unix.inet_addr_of_string "1.2.3.4" , 100)) (conns,server_fn) ; 
      ) >>= fun _ -> 
      get_remote_nodes >>= fun nodes_at_start ->
      spawn ~monitor:true (List.hd nodes_at_start) (fun () -> lift_io (Test_io.sleep 0.05) >>= fun _ -> return ()) >>= fun (_,mref) -> 
      node_at_start_producer := nodes_at_start ; 
      lift_io (Test_io.sleep 0.15) >>= fun () -> 
      get_remote_nodes >>= fun nodes_at_015 ->
      node_after_015_millseconds_producer := nodes_at_015 ;      
      lift_io (Test_io.sleep 0.15) >>= fun () ->
      get_remote_nodes >>= fun nodes_at_03 ->
      node_at_03_milliseconds_producer := nodes_at_03 ;
      catch 
        (fun () -> unmonitor @@ Potpourri.get_option mref)
        (function | InvalidNode _ -> return (invalid_node_exception_monitor := Some true) | _ -> assert false)      
    ) in 

  Lwt.(
    test_run_wrapper (fun () -> 
      Lwt.async (fun () -> Consumer.run_node remote_config ~process:consumer_proc ~node_monitor_fn:monitor_fn_consumer) ;  
      Producer.run_node node_config ~process:p ~node_monitor_fn:monitor_fn_producer >>= fun () ->
      (get_option !exit_fn) () >>= fun () ->      
      assert_equal ~msg:"test_heart_beat failed remotely, nodes at start should be of length 1 for consumer" 1 (List.length !node_at_start_consumer) ;
      assert_equal ~msg:"test_heart_beat failed remotely, nodes at start should be of length 1 for producer" 1 (List.length !node_at_start_producer) ;
      assert_equal ~msg:"test_heart_beat failed remotely, nodes at 015 should be of length 1 for producer" 1 (List.length !node_after_015_millseconds_producer) ;
      assert_equal ~msg:"test_heart_beat failed remotely, nodes at 02 should be of length 0 for consumer" 0 (List.length !node_after_02_millseconds_consumer) ;
      assert_equal ~msg:"test_heart_beat failed remotely, nodes at 03 should be of length 0 for producer" 0 (List.length !node_at_03_milliseconds_producer) ;
      assert_equal ~msg:"test_heart_beat failed remotely, consumer node monitor function should have been called" (Some "producer") !node_went_down_consumer ;
      assert_equal ~msg:"test_heart_beat failed remotely, producer node monitor function should have been called" (Some "consumer") !node_went_down_producer ;
      assert_equal ~msg:"test_heart_beat failed remotely, should have gotten InvalidNode exception when spawning on non-existent node" (Some true) !invalid_node_exception_spawn ;
      assert_equal ~msg:"test_heart_beat failed remotely, should have gotten InvalidNode exception when monitoring on non-existent node" (Some true) !invalid_node_exception_monitor ;
      assert_equal ~msg:"remote config with 2 remote nodes should have establised 2 connections" 2 (Hashtbl.length established_connections) ;
      
      return () 
    ) 
  )                  

let suite = "Test Distributed" >::: [
    "Test return and bind"                                                >:: test_return_bind ;

    "Test spawn local with local config"                                  >:: test_spawn_local_local_config ;
    "Test spawn local with remote config"                                 >:: test_spawn_local_remote_config ;
    "Test spawn remote with remote config"                                >:: test_spawn_remote_remote_config ;

    "Test spawn monitor local with local config"                          >:: test_spawn_monitor_local_local_config;
    "test spawn monitor local with remote config"                         >:: test_spawn_monitor_local_remote_config ;
    "Test spawn monitor remote with remote config"                        >:: test_spawn_monitor_remote_remote_config ;

    "Test monitor local with local config"                                >:: test_monitor_local_local_config ;  
    "Test monitor local remote config"                                    >:: test_monitor_local_remote_config ;
    "Test monitor remote with remote config"                              >:: test_monitor_remote_remote_config ;

    "Test monitor dead process local with local config"                   >:: test_monitor_dead_process_local_local_config ;  
    "Test monitor dead local remote config"                               >:: test_monitor_dead_process_local_remote_config ;
    "Test monitor dead remote with remote config"                         >:: test_monitor_dead_process_remote_remote_config ;

    "Test unmonitor local with local config"                              >:: test_unmonitor_local_local_config ;  
    "Test unmonitor local with remote config"                             >:: test_unmonitor_local_remote_config ;
    "Test unmonitor remote with remote config"                            >:: test_unmonitor_remote_remote_config ;    

    "Test unmonitor from spawn monitor local with local config"           >:: test_unmonitor_from_spawn_monitor_local_local_config ;  
    "Test unmonitor from spawn monitor local remote config"               >:: test_unmonitor_from_spawn_monitor_local_remote_config ;
    "Test unmonitor from spawn monitor remote with remote config"         >:: test_unmonitor_from_spawn_monitor_remote_remote_config ;    

    "Test get remote nodes local with local config"                       >:: test_get_remote_nodes_local_only ;
    "Test get remote nodes local with remote config"                      >:: test_get_remote_nodes_remote_local ;             
    "Test get remote nodes remote with remote conifg"                     >:: test_get_remote_nodes_remote_conifg ; 

    "Test broadcast local with local config"                              >:: test_broadcast_local_only ;
    "Test broadcast local with remote config"                             >:: test_broadcast_remote_local ; 
    "Test broadcast remote and local with remote config"                  >:: test_broadcast_remote_remote ;    

    "Test send local with local config"                                   >:: test_send_local_only ;  
    "Test send local with remote config"                                  >:: test_send_remote_local ; 
    "Test send remote and local with remote config"                       >:: test_send_remote_remote ; 

    "Test receive with empty matchers local with local config"            >:: test_empty_matchers_local_only ;  
    "Test receive with empty matchers local with remote config"           >:: test_empty_matchers_remote_local ;
    "Test receive with empty matchers remote with remote config"          >:: test_empty_matchers_remote_remote ;  

    "Test raise exception on monitored process local with local config"   >:: test_raise_local_config;
    "Test raise exception on monitored process local with remote config"  >:: test_raise_local_remote_config ;
    "Test raise exception on monitored process remote with remote config" >:: test_raise_remote_remote_config ;

    "Test add/remove remote node in local only config"                    >:: test_add_remove_remote_nodes_in_local_config ;
    "Test add/remove remote nodes with remote config"                     >:: test_add_remove_nodes_remote_config ; 

    "Test selective receive with local only config"                       >:: test_selective_receive_local_config ;
    "Test selective_receive local with remoteconfig"                      >:: test_selective_receive_local_remote_config ;
    "Test selective receive remote with remote config"                    >:: test_selective_receive_remote_remote_config ;

    "Test multiple run node calls"                                        >:: test_multiple_run_node ;

    "Test get_remote_node local only"                                     >:: test_get_remote_node_local_only ;
    "Test get_remote_node local with remote config"                       >:: test_get_remote_node_local_remote_config ;
    "Test get_remote_node_remote remote config"                           >:: test_get_remote_node_remote_remote_config ;

    "Test heartbeat"                                                      >:: test_heart_beat ;
  ]

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

let log_it_stdout str_fn = Lwt_io.write Lwt_io.stdout @@ str_fn

let log_it_quiet _ = Lwt.return ()

let _ =
  Logs.Src.set_level log_src (Some Logs.Debug) ;
  Logs.set_reporter @@ lwt_reporter log_it_quiet ;
  try ignore @@ run_test_tt ~verbose:false suite ;
  with _ -> (assert_failure @@ "Encountered exception during test run : " ^ Printexc.get_backtrace ())  

(*BISECT-IGNORE-END*)
