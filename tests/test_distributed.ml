(*BISECT-IGNORE-BEGIN*)

module type Test_IO_sig = sig
  include Distributed.Nonblock_io 
  val run_fn : 'a t -> 'a  
  val get_established_connection_count : unit -> int
  val reset_established_connection_count : unit -> unit
  val get_atexit_fns : unit -> (unit -> unit t) list
  val clear_atexit_fnns : unit -> unit
end

module type Tests = sig
  val run_suite : unit -> unit
end

module Make (Test_io : Test_IO_sig) : Tests = struct

  exception Test_ex

  exception Test_failure of string

let get_option (v : 'a option) : 'a = 
  match v with
  | None -> assert false
  | Some v' -> v'    

module M  = struct
  type t = string

  let string_of_message m = m  
end

let assert_equal msg expect actual =
  if expect = actual then () else raise (Test_failure msg)

let run_tests suite_name tests =
  Printf.printf "Running suite %s\n" suite_name ;
  let buff_test_output = Buffer.create 1024 in  
  let total_tests_ran = ref 0 in
  let total_tests_failed = ref 0 in
  let formatter_test_output = Format.formatter_of_buffer buff_test_output in
  let iter_fn (test_name, test_fn) =
    try (total_tests_ran := !total_tests_ran + 1 ; test_fn ())
    with Test_failure reason -> 
      (total_tests_failed := !total_tests_failed + 1; 
       Format.fprintf formatter_test_output "Test \"%s\" failed : %s.\n" test_name reason)
  in
  let start_time = Sys.time () in
  List.iter iter_fn tests ;
  let duration = Sys.time() -. start_time in
  Printf.printf "Ran %d tests in %f seconds (processor time), %d passed, %d failed.\n" !total_tests_ran duration (!total_tests_ran - !total_tests_failed) !total_tests_failed ;
  Printf.printf "%s" @@ Buffer.contents buff_test_output ; 
  if !total_tests_failed > 0 then exit 1 else exit 0

let test_run_wrapper test_name io_exp =
    Test_io.(run_fn (
      reset_established_connection_count () ; 
      clear_atexit_fnns () ; 
      log Debug (fun () -> Format.sprintf "------ Starting test %s ------" test_name) >>= fun () ->
      io_exp ()))         

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
  test_run_wrapper "test_return_bind" (fun () -> P.run_node node_config ~process:test_proc) ;
  assert_equal "return, bind failed" (Some 5) !result ;     
    assert_equal "local config should hav establised 0 connections" 0 (Test_io.get_established_connection_count ())

(* spawn, spawn monitor tests for local and remote configurations *)

let test_spawn_local_local_config _ =
  let module P = Distributed.Make (Test_io) (M) in
  let node_config = P.Local {P.Local_config.node_name = "test" ;} in
  let result = ref false in
  let mres = ref None in
  let test_proc () = P.(                    
      get_self_node >>= fun local_node ->
      assert_equal "process should not have spawned yet" true (not !result) ;
      spawn local_node (fun () -> return () >>= fun _ -> return (result := true)) >>= fun (_, mon_res) ->
      mres := mon_res ;
      return ()        
    ) in             
  test_run_wrapper "test_spawn_local_local_config" (fun () -> P.run_node node_config ~process:test_proc) ;
  assert_equal "monitor result should have been None" true (!mres = None) ;
  assert_equal "process was not spawned" true !result ;     
    assert_equal "local config should hav establised 0 connections" 0 (Test_io.get_established_connection_count ())

let test_spawn_local_remote_config _ =
  let module P = Distributed.Make (Test_io) (M) in  
  let node_config = P.Remote { P.Remote_config.node_name = "producer" ; 
                               P.Remote_config.local_port = 45000 ;                               
                               P.Remote_config.connection_backlog = 10 ;
                               P.Remote_config.node_ip = "127.0.0.1" ;
                               P.Remote_config.remote_nodes = [] ;
                             } in
  let result = ref false in
  let mres = ref None in
  let test_proc () = P.(        
      get_self_node >>= fun local_node ->
      assert_equal "process should not have spawned yet" true (not !result) ;
      spawn local_node (fun () -> return () >>= fun _ -> result := true ; return ()) >>= fun (_, mon_res) ->
      mres := mon_res ;      
      return ()        
    ) in             
    Test_io.(
    test_run_wrapper "test_spawn_local_remote_config" (fun () -> 
      P.run_node node_config ~process:test_proc >>= fun () ->       
        List.fold_right (fun v acc -> v () >>= fun () -> acc) (get_atexit_fns ()) (return ()) >>= fun () ->
      assert_equal "process was not spawned" true !result ;
      assert_equal "monitor result should have been none" true (!mres = None) ;    
        assert_equal "remote config with only a single node should have establised 1 connection" 1 (Test_io.get_established_connection_count ()) ;  
      
      return ()
    )
  )

let test_spawn_remote_remote_config _ =
  let module Producer = Distributed.Make (Test_io) (M) in
  let module Consumer = Distributed.Make (Test_io) (M) in  
  let node_config = Producer.Remote { Producer.Remote_config.node_name = "producer" ; 
                                      Producer.Remote_config.local_port = 45000 ;                                      
                                      Producer.Remote_config.connection_backlog = 10 ;
                                      Producer.Remote_config.node_ip = "127.0.0.1" ;
                                      Producer.Remote_config.remote_nodes = [("127.0.0.1",46000,"consumer")] ;
                                    } in
  let remote_config = Consumer.Remote { Consumer.Remote_config.node_name = "consumer" ; 
                                        Consumer.Remote_config.local_port = 46000 ;                                        
                                        Consumer.Remote_config.connection_backlog = 10 ;
                                        Consumer.Remote_config.node_ip = "127.0.0.1" ;
                                        Consumer.Remote_config.remote_nodes = [] ;
                                      } in
  let spawn_res = ref None in
  let mres = ref None in                                  
  let producer_proc () = Producer.(
      get_remote_nodes >>= fun nodes ->
      get_self_pid >>= fun pid_to_send_to ->
      assert_equal "process should not have spawned yet" true (!spawn_res = None) ;
      spawn (List.hd nodes) (fun () -> Consumer.send pid_to_send_to "spawned") >>= fun (_, mon_res) ->      
      receive ~timeout_duration:0.05 @@
        case (fun v -> Some (fun () -> return v))
      >>= fun msg ->
      spawn_res := msg ;
      mres := mon_res ;
      return ()                   
    ) in
    Test_io.(
    test_run_wrapper "test_spawn_remote_remote_config" (fun () -> 
        Test_io.async (fun () -> Consumer.run_node remote_config) ;                
      Producer.run_node node_config ~process:producer_proc >>= fun () ->
        List.fold_right (fun v acc -> v () >>= fun () -> acc) (get_atexit_fns ()) (return ()) >>= fun () ->
        assert_equal "remote config with 2 remote nodes should have establised 2 connections" 2 (Test_io.get_established_connection_count ()) ;
      assert_equal "process did not spawn" (Some "spawned") !spawn_res ;
      
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
      assert_equal "process should not have spawned yet" true (not @@ !result && !result_monitor = None) ;      
      spawn ~monitor:true local_node (fun () -> return () >>= fun _ -> return (result := true)) >>= fun (_, mon_res) ->
      receive  @@
        termination_case 
          (function
            | Normal _ -> return (result_monitor := Some "got normal termination")
            | _ -> assert false
          )
      >>= fun _ ->    
      mres := mon_res ;  
      return ()        
    ) in             
    Test_io.(test_run_wrapper "test_spawn_monitor_local_local_config" (fun () -> P.run_node node_config ~process:test_proc >>= fun () -> List.fold_right (fun v acc -> v () >>= fun () -> acc) (get_atexit_fns ()) (return ()))) ;
  assert_equal "process was not spawned" true (!result && !result_monitor <> None) ;
  assert_equal "termination monitor result not received" (Some "got normal termination") !result_monitor ;     
    assert_equal "local config should hav establised 0 connections" 0 (Test_io.get_established_connection_count ()) ;
  assert_equal "spawn monitor failed" true (None <> !mres) ;
    assert_equal "should have established 0 connections" 0 (Test_io.get_established_connection_count ())    

let test_spawn_monitor_local_remote_config _ =
  let module P = Distributed.Make (Test_io) (M) in
  let node_config = P.Remote { P.Remote_config.node_name = "producer" ; 
                               P.Remote_config.local_port = 45000 ;                               
                               P.Remote_config.connection_backlog = 10 ;
                               P.Remote_config.node_ip = "127.0.0.1" ;
                               P.Remote_config.remote_nodes = [] ;
                             } in
  let result = ref false in
  let result_monitor = ref None in
  let mres = ref None in
  let test_proc () = P.(                          
      get_self_node >>= fun local_node ->
      assert_equal "Process should not have spawned yet" true (not @@ !result && !result_monitor = None) ;      
      spawn ~monitor:true local_node (fun () -> return () >>= fun _ -> return (result := true)) >>= fun (_, mon_res) ->
      receive @@
        termination_case 
          (function
            | Normal _ -> return (result_monitor := Some "got normal termination")
            | _ -> assert false
          )
      >>= fun _ ->
      mres := mon_res ;  
      return ()             
    ) in             
    Test_io.(
    test_run_wrapper "test_spawn_monitor_local_remote_config" (fun () -> 
      P.run_node node_config ~process:test_proc >>= fun () -> 
        List.fold_right (fun v acc -> v () >>= fun () -> acc) (get_atexit_fns ()) (return ()) >>= fun () ->
      assert_equal "process was not spawned" true (!result && !result_monitor <> None) ;
      assert_equal "termination monitor result not received" (Some "got normal termination") !result_monitor ;     
        assert_equal "remote config with only a single node should have establised 1 connection" 1 (Test_io.get_established_connection_count ()) ;
      assert_equal "spawn monitor failed" true (None <> !mres) ;
      
      return ()    
    )
  )

let test_spawn_monitor_remote_remote_config _ =
  let module Producer = Distributed.Make (Test_io) (M) in
  let module Consumer = Distributed.Make (Test_io) (M) in  
  let node_config = Producer.Remote { Producer.Remote_config.node_name = "producer" ; 
                                      Producer.Remote_config.local_port = 45000 ;                                      
                                      Producer.Remote_config.connection_backlog = 10 ;
                                      Producer.Remote_config.node_ip = "127.0.0.1" ;
                                      Producer.Remote_config.remote_nodes = [("127.0.0.1",46000,"consumer")] ;
                                    } in
  let remote_config = Consumer.Remote { Consumer.Remote_config.node_name = "consumer" ; 
                                        Consumer.Remote_config.local_port = 46000 ;                                        
                                        Consumer.Remote_config.connection_backlog = 10 ;
                                        Consumer.Remote_config.node_ip = "127.0.0.1" ;
                                        Consumer.Remote_config.remote_nodes = [] ;
                                      } in
  let result_monitor = ref None in
  let mres = ref None in                                  
  let producer_proc () = Producer.(      
      get_remote_nodes >>= fun nodes ->
      get_self_pid >>= fun pid_to_send_to ->
      spawn ~monitor:true (List.hd nodes) (fun () -> Consumer.send pid_to_send_to "spawned") >>= fun (_, mon_res) ->      
      receive  @@
        termination_case 
          (function
            | Normal _ -> return (result_monitor := Some "got normal termination")
            | _ -> assert false
          )
      >>= fun _ ->
      mres := mon_res ;  
      return ()                       
    ) in
    Test_io.(
    test_run_wrapper "test_spawn_monitor_remote_remote_config" (fun () -> 
        Test_io.async (fun () -> Consumer.run_node remote_config) ;            
      Producer.run_node node_config ~process:producer_proc >>= fun () ->
        List.fold_right (fun v acc -> v () >>= fun () -> acc) (get_atexit_fns ()) (return ()) >>= fun () ->      
      assert_equal "termination monitor result not received" (Some "got normal termination") !result_monitor ;     
        assert_equal "remote config with 2 remote nodes should have establised 2 connections" 2 (Test_io.get_established_connection_count ()) ;
      assert_equal "spawn monitor failed" true (None <> !mres) ;
      
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
    receive  @@
      termination_case
        (function
          | Normal _ -> return (result_monitor2 := Some "got normal termination")
          | _ -> assert false
        )
    >>= fun _ -> return ()
  ) in 
  let test_proc () = P.(                                        
      get_self_node >>= fun local_node ->
      assert_equal "Process should not have spawned yet" true (not !result) ;
      spawn local_node (fun () -> return () >>= fun _ -> lift_io (Test_io.sleep 0.05) >>= fun () -> return (result := true)) >>= fun (new_pid, _) ->
      spawn local_node (another_monitor_proc new_pid) >>= fun _ ->
      monitor new_pid >>= fun _ ->
      receive  @@
        termination_case 
          (function
            | Normal _ -> return (result_monitor := Some "got normal termination")
            | _ -> assert false
          )
      >>= fun _ ->
      return ()        
    ) in             
    Test_io.(test_run_wrapper "test_monitor_local_local_config" (fun () -> P.run_node node_config ~process:test_proc >>= fun () -> List.fold_right (fun v acc -> v () >>= fun () -> acc) (get_atexit_fns ()) (return ())));
  assert_equal "process was not spawned" true (!result && !result_monitor <> None) ; 
  assert_equal "monitor failed" (Some "got normal termination") !result_monitor ;    
  assert_equal "monitor 2 failed" (Some "got normal termination") !result_monitor2 ;    
    assert_equal "local config should hav establised 0 connections" 0 (Test_io.get_established_connection_count ())    

let test_monitor_local_remote_config _ =
  let module P = Distributed.Make (Test_io) (M) in  
  let node_config = P.Remote { P.Remote_config.node_name = "producer" ; 
                               P.Remote_config.local_port = 45000 ;                               
                               P.Remote_config.connection_backlog = 10 ;
                               P.Remote_config.node_ip = "127.0.0.1" ;
                               P.Remote_config.remote_nodes = [] ;
                             } in
  let result = ref false in
  let result_monitor = ref None in
  let result_monitor2 = ref None in
  let another_monitor_proc pid_to_monitor () = P.(
    monitor pid_to_monitor >>= fun _ ->
    receive  @@
      termination_case
        (function
          | Normal _ -> return (result_monitor2 := Some "got normal termination")
          | _ -> assert false
        )
    >>= fun _ -> return ()
  ) in   
  let test_proc () = P.(                                        
      get_self_node >>= fun local_node ->
      assert_equal "Process should not have spawned yet" true (not !result) ;
      spawn local_node (fun () -> return () >>= fun _ -> lift_io (Test_io.sleep 0.05) >>= fun () -> return (result := true)) >>= fun (new_pid, _) ->
      spawn local_node (another_monitor_proc new_pid) >>= fun _ ->
      monitor new_pid >>= fun _ ->
      receive  @@
        termination_case 
          (function
            | Normal _ -> return (result_monitor := Some "got normal termination")
            | _ -> assert false
          )
      >>= fun _ ->
      return ()       
    ) in             
    Test_io.(
    test_run_wrapper "test_monitor_local_remote_config" (fun () -> 
      P.run_node node_config ~process:test_proc >>= fun () -> 
        List.fold_right (fun v acc -> v () >>= fun () -> acc) (get_atexit_fns ()) (return ()) >>= fun () ->
      assert_equal "process was not spawned" true (!result && !result_monitor <> None) ;
      assert_equal "monitor failed" (Some "got normal termination") !result_monitor ;      
      assert_equal "monitor 2 failed" (Some "got normal termination") !result_monitor2 ;      
        assert_equal "remote config with only a single node should have establised 1 connection" 1 (Test_io.get_established_connection_count ()) ;
      
      return ()    
    )
  )

let test_monitor_remote_remote_config _ =
  let module Producer = Distributed.Make (Test_io) (M) in
  let module Consumer = Distributed.Make (Test_io) (M) in  
  let node_config = Producer.Remote { Producer.Remote_config.node_name = "producer" ; 
                                      Producer.Remote_config.local_port = 45000 ;                                      
                                      Producer.Remote_config.connection_backlog = 10 ;
                                      Producer.Remote_config.node_ip = "127.0.0.1" ;
                                      Producer.Remote_config.remote_nodes = [("127.0.0.1",46000,"consumer")] ;
                                    } in
  let remote_config = Consumer.Remote { Consumer.Remote_config.node_name = "consumer" ; 
                                        Consumer.Remote_config.local_port = 46000 ;                                        
                                        Consumer.Remote_config.connection_backlog = 10 ;
                                        Consumer.Remote_config.node_ip = "127.0.0.1" ;
                                        Consumer.Remote_config.remote_nodes = [] ;
                                      } in
  let result_monitor = ref None in 
  let result_monitor2 = ref None in 
  let another_monitor_proc pid_to_monitor () = Producer.(
    monitor pid_to_monitor >>= fun _ ->
    receive  @@
      termination_case
        (function
          | Normal _ -> return (result_monitor2 := Some "got normal termination")
          | _ -> assert false
        )
    >>= fun _ -> return ()
  ) in      
  let producer_proc () = Producer.(        
      get_remote_nodes >>= fun nodes ->
      get_self_node >>= fun local_node ->
      spawn (List.hd nodes) (fun () -> return () >>= fun _ -> lift_io (Test_io.sleep 0.05)) >>= fun (remote_pid, _) ->
      spawn local_node (another_monitor_proc remote_pid) >>= fun _ ->
      monitor remote_pid >>= fun _ ->      
      receive  @@
        termination_case 
          (function
            | Normal _ -> return (result_monitor := Some "got normal termination")
            | _ -> assert false
          )
      >>= fun _ ->
      return ()                 
    ) in
    Test_io.(
    test_run_wrapper "test_monitor_remote_remote_config" (fun () -> 
        Test_io.async (fun () -> Consumer.run_node remote_config) ;            
      Producer.run_node node_config ~process:producer_proc >>= fun () ->
        List.fold_right (fun v acc -> v () >>= fun () -> acc) (get_atexit_fns ()) (return ()) >>= fun () ->      
      assert_equal "monitor failed" (Some "got normal termination") !result_monitor ;      
      assert_equal "monitor 2 failed" (Some "got normal termination") !result_monitor2 ;      
        assert_equal "remote config with 2 remote nodes should have establised 2 connections" 2 (Test_io.get_established_connection_count ()) ;
      
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
    receive ~timeout_duration:0.05  @@
      termination_case
        (function
          | Normal _ -> return (unmon_res2 := Some "got normal termination")
          | _ -> assert false
        )
    >>= fun _ -> return ()
  ) in     
  let test_proc () = P.(                                        
      get_self_node >>= fun local_node ->
      assert_equal "process should not have spawned yet" true (not !result) ;
      spawn local_node (fun () -> return () >>= fun _ -> lift_io (Test_io.sleep 0.05) >>= fun () -> return (result := true)) >>= fun (new_pid, _) ->
      spawn local_node (another_monitor_proc new_pid) >>= fun _ ->
      monitor new_pid >>= fun mon_res ->
      unmonitor mon_res >>= fun () ->
      receive ~timeout_duration:0.05  @@
        termination_case 
          (function
            | Normal _ -> return "got normal termination"
            | _ -> assert false
          )
      >>= fun received ->
      unmon_res := received ;
      return ()        
    ) in             
    Test_io.(test_run_wrapper "test_unmonitor_local_local_config" (fun () -> P.run_node node_config ~process:test_proc >>= fun () -> List.fold_right (fun v acc -> v () >>= fun () -> acc) (get_atexit_fns ()) (return ())));
  assert_equal "process was not spawned" true !result ;
  assert_equal "unmonitor failed" None !unmon_res ;     
  assert_equal "unmonitor 2 failed" None !unmon_res2 ;     
    assert_equal "local config should hav establised 0 connections" 0 (Test_io.get_established_connection_count ())    

let test_unmonitor_local_remote_config _ =
  let module P = Distributed.Make (Test_io) (M) in  
  let node_config = P.Remote { P.Remote_config.node_name = "producer" ; 
                               P.Remote_config.local_port = 45000 ;                               
                               P.Remote_config.connection_backlog = 10 ;
                               P.Remote_config.node_ip = "127.0.0.1" ;
                               P.Remote_config.remote_nodes = [] ;
                             } in
  let result = ref false in
  let unmon_res = ref None in
  let unmon_res2 = ref None in 
  let another_monitor_proc pid_to_monitor () = P.(
    monitor pid_to_monitor >>= fun mres ->
    unmonitor mres >>= fun _ ->
    receive ~timeout_duration:0.05  @@
      termination_case
        (function
          | Normal _ -> return (unmon_res2 := Some "got normal termination")
          | _ -> assert false
        )
    >>= fun _ -> return ()
  ) in     
  let test_proc () = P.(                                        
      get_self_node >>= fun local_node ->
      assert_equal "Process should not have spawned yet" true (not !result) ;
      spawn local_node (fun () -> result := true ; lift_io (Test_io.sleep 0.05)) >>= fun (new_pid, _) ->
      spawn local_node (another_monitor_proc new_pid) >>= fun _ ->
      monitor new_pid >>= fun mon_res ->
      unmonitor mon_res >>= fun () ->
      receive ~timeout_duration:0.05  @@
        termination_case
          (function
            | Normal _ -> return "got normal termination"
            | _ -> assert false
          )
      >>= fun received ->
      unmon_res := received ;
      return ()        
    ) in             
    Test_io.(
    test_run_wrapper "test_unmonitor_local_remote_config" (fun () -> 
      P.run_node node_config ~process:test_proc >>= fun () -> 
        List.fold_right (fun v acc -> v () >>= fun () -> acc) (get_atexit_fns ()) (return ()) >>= fun () ->
      assert_equal "process was not spawned" true !result ;  
      assert_equal "unmonitor failed" None !unmon_res ;    
      assert_equal "unmonitor 2 failed" None !unmon_res2 ;    
        assert_equal "remote config with only a single node should have establised 1 connection" 1 (Test_io.get_established_connection_count ()) ;
      
      return ()    
    )
  )

let test_unmonitor_remote_remote_config _ =
  let module Producer = Distributed.Make (Test_io) (M) in
  let module Consumer = Distributed.Make (Test_io) (M) in  
  let node_config = Producer.Remote { Producer.Remote_config.node_name = "producer" ; 
                                      Producer.Remote_config.local_port = 45000 ;                                      
                                      Producer.Remote_config.connection_backlog = 10 ;
                                      Producer.Remote_config.node_ip = "127.0.0.1" ;
                                      Producer.Remote_config.remote_nodes = [("127.0.0.1",46000,"consumer")] ;
                                    } in
  let remote_config = Consumer.Remote { Consumer.Remote_config.node_name = "consumer" ; 
                                        Consumer.Remote_config.local_port = 46000 ;                                        
                                        Consumer.Remote_config.connection_backlog = 10 ;
                                        Consumer.Remote_config.node_ip = "127.0.0.1" ;
                                        Consumer.Remote_config.remote_nodes = [] ;
                                      } in
  let unmon_res = ref None in
  let unmon_res2 = ref None in 
  let another_monitor_proc pid_to_monitor () = Producer.(
    monitor pid_to_monitor >>= fun mres ->
    unmonitor mres >>= fun _ ->
    receive ~timeout_duration:0.05  @@
      termination_case
        (function
          | Normal _ -> return (unmon_res2 := Some "got normal termination")
          | _ -> assert false
        )
    >>= fun _ -> return ()
  ) in     
  let producer_proc () = Producer.(      
      get_remote_nodes >>= fun nodes ->    
      get_self_node >>= fun local_node ->  
      spawn (List.hd nodes) (fun () -> return () >>= fun _ -> lift_io (Test_io.sleep 0.05)) >>= fun (remote_pid, _) ->
      spawn local_node (another_monitor_proc remote_pid) >>= fun _ ->
      monitor remote_pid >>= fun mon_res ->      
      unmonitor mon_res >>= fun () ->
      receive ~timeout_duration:0.05  @@
        termination_case
          (function
            | Normal _ -> return "got normal termination"
            | _ -> assert false
          )
      >>= fun received ->
      unmon_res := received ;     
      return ()                 
    ) in
    Test_io.(
    test_run_wrapper "test_unmonitor_remote_remote_config" (fun () -> 
        Test_io.async (fun () -> Consumer.run_node remote_config) ;            
      Producer.run_node node_config ~process:producer_proc >>= fun () ->
        List.fold_right (fun v acc -> v () >>= fun () -> acc) (get_atexit_fns ()) (return ()) >>= fun () ->      
      assert_equal "unmonitor failed" None !unmon_res ;    
      assert_equal "unmonitor 2 failed" None !unmon_res2 ;    
        assert_equal "remote config with 2 remote nodes should have establised 2 connections" 2 (Test_io.get_established_connection_count ()) ;
      
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
      assert_equal "Process should not have spawned yet" true (not !result) ;
      spawn ~monitor:true local_node (fun () -> return () >>= fun _ -> lift_io (Test_io.sleep 0.05) >>= fun () -> result := true ; return ()) >>= fun (_, spawn_mon_res) ->
      mres := spawn_mon_res ;
      unmonitor (get_option spawn_mon_res) >>= fun () ->
      receive ~timeout_duration:0.05  @@
        termination_case
          (function
            | Normal _ -> return "got normal termination"
            | _ -> assert false
          )
      >>= fun received ->
      unmon_res := received ;
      return ()             
    ) in             
    Test_io.(test_run_wrapper "test_unmonitor_from_spawn_monitor_local_local_config" (fun () -> P.run_node node_config ~process:test_proc >>= fun () -> List.fold_right (fun v acc -> v () >>= fun () -> acc) (get_atexit_fns ()) (return ())));
  assert_equal "Process was not spawned and monitored" true (!result && !mres <> None) ;
  assert_equal "unmonitor failed" None !unmon_res ;       
    assert_equal "local config should hav establised 0 connections" 0 (Test_io.get_established_connection_count ())    

let test_unmonitor_from_spawn_monitor_local_remote_config _ =
  let module P = Distributed.Make (Test_io) (M) in  
  let node_config = P.Remote { P.Remote_config.node_name = "producer" ; 
                               P.Remote_config.local_port = 45000 ;                               
                               P.Remote_config.connection_backlog = 10 ;
                               P.Remote_config.node_ip = "127.0.0.1" ;
                               P.Remote_config.remote_nodes = [] ;
                             } in
  let result = ref false in
  let mres = ref None in
  let unmon_res = ref None in  
  let test_proc () = P.(     
      get_self_node >>= fun local_node ->
      assert_equal "Process should not have spawned yet" true (not !result) ;
      spawn ~monitor:true local_node (fun () -> return () >>= fun _ -> lift_io (Test_io.sleep 0.05) >>= fun () -> result := true ; return ()) >>= fun (_, spawn_mon_res) ->      
      mres := spawn_mon_res ;
      unmonitor (get_option spawn_mon_res) >>= fun () ->
      receive ~timeout_duration:0.05  @@
        termination_case
          (function
            | Normal _ -> return "got normal termination"
            | _ -> assert false
          )
      >>= fun received ->
      unmon_res := received ;
      return ()                 
    ) in             
    Test_io.(
    test_run_wrapper "test_unmonitor_from_spawn_monitor_local_remote_config" (fun () -> 
      P.run_node node_config ~process:test_proc >>= fun () -> 
        List.fold_right (fun v acc -> v () >>= fun () -> acc) (get_atexit_fns ()) (return ()) >>= fun () ->
      assert_equal "Process was not spawned and monitored" true (!result && !mres <> None) ;
      assert_equal "unmonitor failed" None !unmon_res ;       
        assert_equal "remote config with only a single node should have establised 1 connection" 1 (Test_io.get_established_connection_count ()) ;
      
      return ()    
    )
  )

let test_unmonitor_from_spawn_monitor_remote_remote_config _ =
  let module Producer = Distributed.Make (Test_io) (M) in
  let module Consumer = Distributed.Make (Test_io) (M) in  
  let node_config = Producer.Remote { Producer.Remote_config.node_name = "producer" ; 
                                      Producer.Remote_config.local_port = 45000 ;                                      
                                      Producer.Remote_config.connection_backlog = 10 ;
                                      Producer.Remote_config.node_ip = "127.0.0.1" ;
                                      Producer.Remote_config.remote_nodes = [("127.0.0.1",46000,"consumer")] ;
                                    } in
  let remote_config = Consumer.Remote { Consumer.Remote_config.node_name = "consumer" ; 
                                        Consumer.Remote_config.local_port = 46000 ;                                        
                                        Consumer.Remote_config.connection_backlog = 10 ;
                                        Consumer.Remote_config.node_ip = "127.0.0.1" ;
                                        Consumer.Remote_config.remote_nodes = [] ;
                                      } in
  let mres = ref None in
  let unmon_res = ref None in 
  let producer_proc () = Producer.(
      get_remote_nodes >>= fun nodes ->      
      spawn ~monitor:true (List.hd nodes) (fun () -> return () >>= fun _ -> lift_io (Test_io.sleep 0.05)) >>= fun (_, spawn_mon_res) ->
      mres := spawn_mon_res ;
      unmonitor (get_option spawn_mon_res) >>= fun () ->
      receive ~timeout_duration:0.05  @@
        termination_case
          (function
            | Normal _ -> return "got normal termination"
            | _ -> assert false
          )
      >>= fun received ->
      unmon_res := received ;
      return ()                  
    ) in
    Test_io.(
    test_run_wrapper "test_unmonitor_from_spawn_monitor_remote_remote_config" (fun () -> 
        Test_io.async (fun () -> Consumer.run_node remote_config) ;            
      Producer.run_node node_config ~process:producer_proc >>= fun () ->
        List.fold_right (fun v acc -> v () >>= fun () -> acc) (get_atexit_fns ()) (return ()) >>= fun () ->      
      assert_equal "unmonitor failed" None !unmon_res ;       
        assert_equal "remote config with 2 remote nodes should have establised 2 connections" 2 (Test_io.get_established_connection_count ()) ;
      
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
  test_run_wrapper "test_get_remote_nodes_local_only" (fun () -> P.run_node node_config ~process:test_proc) ;     
  assert_equal "get remote nodes in local config should return 0" 0 !num_remote_nodes ;
    assert_equal "local config should hav establised 0 connections" 0 (Test_io.get_established_connection_count ())

let test_get_remote_nodes_remote_local _ =
  let module P = Distributed.Make (Test_io) (M) in  
  let node_config = P.Remote { P.Remote_config.node_name = "producer" ; 
                               P.Remote_config.local_port = 45000 ;                               
                               P.Remote_config.connection_backlog = 10 ;
                               P.Remote_config.node_ip = "127.0.0.1" ;
                               P.Remote_config.remote_nodes = [] ;
                             } in
  let num_remote_nodes = ref (-1) in
  let test_proc () = P.(        
      get_remote_nodes >>= fun nodes ->
      return (num_remote_nodes := (List.length nodes))         
    ) in             
    Test_io.(
    test_run_wrapper "test_get_remote_nodes_local_only" (fun () -> 
      P.run_node node_config ~process:test_proc >>= fun () -> 
        List.fold_right (fun v acc -> v () >>= fun () -> acc) (get_atexit_fns ()) (return ()) >>= fun () ->
      assert_equal "get remote nodes in remote config with no remote nodes should return 0" 0 !num_remote_nodes ;
        assert_equal "remote config with only a single node should have establised 1 connection" 1 (Test_io.get_established_connection_count ()) ;
      
      return ()
    )
  )

let test_get_remote_nodes_remote_conifg _ =
  let module Producer = Distributed.Make (Test_io) (M) in
  let module Consumer = Distributed.Make (Test_io) (M) in  
  let node_config = Producer.Remote { Producer.Remote_config.node_name = "producer" ; 
                                      Producer.Remote_config.local_port = 45000 ;                                      
                                      Producer.Remote_config.connection_backlog = 10 ;
                                      Producer.Remote_config.node_ip = "127.0.0.1" ;
                                      Producer.Remote_config.remote_nodes = [("127.0.0.1",46000,"consumer")] ;
                                    } in
  let remote_config = Consumer.Remote { Consumer.Remote_config.node_name = "consumer" ; 
                                        Consumer.Remote_config.local_port = 46000 ;                                        
                                        Consumer.Remote_config.connection_backlog = 10 ;
                                        Consumer.Remote_config.node_ip = "127.0.0.1" ;
                                        Consumer.Remote_config.remote_nodes = [] ;
                                      } in
  let num_remote_nodes = ref (-1) in    
  let producer_proc () = Producer.(
      get_remote_nodes >>= fun nodes ->
      return (num_remote_nodes := (List.length nodes))                          
    ) in
    Test_io.(
    test_run_wrapper "test_get_remote_nodes_remote_conifg" (fun () -> 
        Test_io.async (fun () -> Consumer.run_node remote_config) ;            
      Producer.run_node node_config ~process:producer_proc >>= fun () ->
        List.fold_right (fun v acc -> v () >>= fun () -> acc) (get_atexit_fns ()) (return ()) >>= fun () ->
      assert_equal "get remote nodes in remote config with 1 remote nodes should return 1" 1 !num_remote_nodes ;
        assert_equal "remote config with 2 remote nodes should have establised 2 connections" 2 (Test_io.get_established_connection_count ()) ;
      
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
      receive 
        begin
          case (
            function 
            | "broadcast message" -> Some (fun () -> return (broadcast_received := !broadcast_received +1))
            | _ -> None
          )
          |. case (fun _ -> Some (fun () -> return ()))
        end
      >>= fun _ ->
      return ()      
    ) in      
  let test_proc () = P.(
      get_self_node >>= fun local_node ->
      spawn local_node recv_proc >>= fun _ ->
      spawn local_node recv_proc >>= fun _ ->
      broadcast local_node "broadcast message" >>= fun () ->
      receive ~timeout_duration:0.05 @@
        case (fun m -> Some (fun () -> return m))       
      >>= fun recv_res ->
      loop_back_received := recv_res ;            
      return ()
    ) in           
    Test_io.(test_run_wrapper "test_broadcast_local_only" (fun () -> P.run_node node_config ~process:test_proc >>= fun () -> List.fold_right (fun v acc -> v () >>= fun () -> acc) (get_atexit_fns ()) (return ()))) ;
  assert_equal "broacast failed" 2 !broadcast_received ;
  assert_equal "broadcast message sent to originator" None !loop_back_received ;    
    assert_equal "local config should hav establised 0 connections" 0 (Test_io.get_established_connection_count ())

let test_broadcast_remote_local _ =
  let module P = Distributed.Make (Test_io) (M) in  
  let node_config = P.Remote { P.Remote_config.node_name = "producer" ; 
                               P.Remote_config.local_port = 45000 ;                               
                               P.Remote_config.connection_backlog = 10 ;
                               P.Remote_config.node_ip = "127.0.0.1" ;
                               P.Remote_config.remote_nodes = [] ;
                             } in
  let broadcast_received = ref 0 in 
  let loop_back_received = ref None in                            
  let recv_proc () = P.(        
      receive 
        begin
          case (
            function 
            | "broadcast message" -> Some (fun () -> return (broadcast_received := !broadcast_received +1))
            | _ -> None
          ) 
          |. case (fun _ -> Some (fun () -> return ()))
        end
      >>= fun _ ->
      return ()      
    ) in      
  let test_proc () = P.(
      get_self_node >>= fun local_node ->
      spawn local_node recv_proc >>= fun _ ->
      spawn local_node recv_proc >>= fun _ ->
      broadcast local_node "broadcast message" >>= fun () ->
      receive ~timeout_duration:0.05 @@
        case (fun m -> Some (fun () -> return m))       
      >>= fun recv_res ->
      loop_back_received := recv_res ;      
      return ()
    ) in           
    Test_io.(
    test_run_wrapper "test_broadcast_remote_local" (fun () -> 
      P.run_node node_config ~process:test_proc >>= fun () -> 
        List.fold_right (fun v acc -> v () >>= fun () -> acc) (get_atexit_fns ()) (return ()) >>= fun () ->
        assert_equal "remote config with only a single node should have establised 1 connection" 1 (Test_io.get_established_connection_count ()) ;
      assert_equal "broacast fail" 2 !broadcast_received ; 
      assert_equal "broadcast message sent to originator" None !loop_back_received ;     
      
      return ()  
    )
  )

let test_broadcast_remote_remote _ =
  let module Producer = Distributed.Make (Test_io) (M) in
  let module Consumer = Distributed.Make (Test_io) (M) in  
  let node_config = Producer.Remote { Producer.Remote_config.node_name = "producer" ; 
                                      Producer.Remote_config.local_port = 45000 ;                                      
                                      Producer.Remote_config.connection_backlog = 10 ;
                                      Producer.Remote_config.node_ip = "127.0.0.1" ;
                                      Producer.Remote_config.remote_nodes = [("127.0.0.1",46000,"consumer")] ;
                                    } in
  let remote_config = Consumer.Remote { Consumer.Remote_config.node_name = "consumer" ; 
                                        Consumer.Remote_config.local_port = 46000 ;                                        
                                        Consumer.Remote_config.connection_backlog = 10 ;
                                        Consumer.Remote_config.node_ip = "127.0.0.1" ;
                                        Consumer.Remote_config.remote_nodes = [] ;
                                      } in
  let broadcast_received = ref 0 in
  let loop_back_received = ref None in                             
  let recv_proc to_send_pid () = Consumer.(        
      receive
        begin
          case (
            function 
            | "broadcast message" -> Some (fun () -> send to_send_pid "incr")
            | _ -> None
          ) 
          |. case (fun _ -> Some (fun _ -> return ()))
        end
      >>= fun _ ->
      return ()      
    ) in      
  let producer_proc () = Producer.(
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
        receive ~timeout_duration:0.05
          begin
            case (
              function 
              | "incr" -> Some (fun () -> return (broadcast_received := !broadcast_received +1))
              | _ -> None
            ) 
            |. case (
              function
              | "broadcast message" -> Some (fun _ -> return (loop_back_received := Some "broadcast message"))
              | _ -> None
            )           
            |. case (fun _ -> Some (fun _ -> return ()))       
          end
        >>= fun res ->
        if res = None
        then return ()
        else receive_loop ()
      in 
      receive_loop ()      
    ) in      
    Test_io.(
    test_run_wrapper "test_broadcast_remote_remote" (fun () -> 
        Test_io.async (fun () -> Consumer.run_node remote_config) ;            
      Producer.run_node node_config ~process:producer_proc >>= fun () ->
        List.fold_right (fun v acc -> v () >>= fun () -> acc) (get_atexit_fns ()) (return ()) >>= fun () ->
      assert_equal "broacast fail" 4 !broadcast_received ;
      assert_equal "broadcast message sent to originator" None !loop_back_received ;
        assert_equal "remote config with 2 remote nodes should have establised 2 connections" 2 (Test_io.get_established_connection_count ()) ;
      
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
      receive
        begin
          case (
            function 
            | "sent message" -> Some (fun () -> return (received_message := Some "sent message"))
            | _ -> None
          ) 
          |. case (fun _ -> Some (fun _ -> return ()))
        end
      >>= fun _ ->
      return ()      
    ) in      
  let test_proc () = P.(      
      get_self_node >>= fun local_node ->
      spawn ~monitor:true local_node recv_proc >>= fun (spawned_pid,mref) ->
      mres := mref ;
      send spawned_pid "sent message" >>= fun () ->
      receive ~timeout_duration:0.05 @@
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
      >>= fun _ ->           
      return ()
    ) in           
    Test_io.(test_run_wrapper "test_send_local_only" (fun () -> P.run_node node_config ~process:test_proc >>= fun () -> List.fold_right (fun v acc -> v () >>= fun () -> acc) (get_atexit_fns ()) (return ()))) ;
  assert_equal "spawn and monitor failed" true (!mres <> None) ;
  assert_equal "send failed" (Some "sent message") !received_message ;
  assert_equal "sending to invalid process should have succeeded" true (not !send_failed) ;    
    assert_equal "local config should hav establised 0 connections" 0 (Test_io.get_established_connection_count ())  

let test_send_remote_local _ =
  let module P = Distributed.Make (Test_io) (M) in  
  let node_config = P.Remote { P.Remote_config.node_name = "producer" ; 
                               P.Remote_config.local_port = 45000 ;                               
                               P.Remote_config.connection_backlog = 10 ;
                               P.Remote_config.node_ip = "127.0.0.1" ;
                               P.Remote_config.remote_nodes = [] ;
                             } in  
  let received_message = ref None in
  let mres = ref None in       
  let send_failed = ref false in                  
  let recv_proc () = P.(        
      receive
        begin
          case (
            function 
            | "sent message" -> Some (fun () -> return (received_message := Some "sent message"))
            | _ -> None
          ) 
          |. case (fun _ -> Some (fun _ -> return ()))
        end
      >>= fun _ ->
      return ()      
    ) in      
  let test_proc () = P.(      
      get_self_node >>= fun local_node ->
      spawn ~monitor:true local_node recv_proc >>= fun (spawned_pid,mref) ->
      mres := mref ;
      send spawned_pid "sent message" >>= fun () ->
      receive ~timeout_duration:0.05 @@
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
      >>= fun _ ->           
      return ()
    ) in           
    Test_io.(
    test_run_wrapper "test_send_remote_local" (fun () -> 
      P.run_node node_config ~process:test_proc >>= fun () -> 
        List.fold_right (fun v acc -> v () >>= fun () -> acc) (get_atexit_fns ()) (return ()) >>= fun () ->
      assert_equal "spawn and monitor failed" true (!mres <> None) ;
      assert_equal "send failed" (Some "sent message") !received_message ;
      assert_equal "sending to invalid process should have succeeded" true (not !send_failed) ;   
        assert_equal "remote config with only a single node should have establised 1 connection" 1 (Test_io.get_established_connection_count ()) ;
      
      return ()    
    )
  )

let test_send_remote_remote _ =
  let module Producer = Distributed.Make (Test_io) (M) in
  let module Consumer = Distributed.Make (Test_io) (M) in  
  let node_config = Producer.Remote { Producer.Remote_config.node_name = "producer" ; 
                                      Producer.Remote_config.local_port = 45000 ;                                      
                                      Producer.Remote_config.connection_backlog = 10 ;
                                      Producer.Remote_config.node_ip = "127.0.0.1" ;
                                      Producer.Remote_config.remote_nodes = [("127.0.0.1",46000,"consumer")] ;
                                    } in
  let remote_config = Consumer.Remote { Consumer.Remote_config.node_name = "consumer" ; 
                                        Consumer.Remote_config.local_port = 46000 ;                                        
                                        Consumer.Remote_config.connection_backlog = 10 ;
                                        Consumer.Remote_config.node_ip = "127.0.0.1" ;
                                        Consumer.Remote_config.remote_nodes = [] ;
                                      } in
  let sent_received = ref 0 in
  let send_failed = ref false in                               
  let recv_proc to_send_pid () = Consumer.(        
      receive
        begin
          case (
            function
            | "sent message" -> Some (fun () -> send to_send_pid "incr")
            | _ -> None
          ) 
          |. case (fun _ -> Some (fun () -> return ()))
        end
      >>= fun _ ->
      return ()      
    ) in      
  let producer_proc () = Producer.(
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
        receive ~timeout_duration:0.05
          begin
            case (
              function 
              | "incr" -> Some (fun () -> return (sent_received := !sent_received +1))
              | _ -> None
            )
            |. termination_case 
              (function
                | Normal term_pid -> 
                  catch 
                    (fun () -> send term_pid "sent message")
                    (function| _ -> return (send_failed := true))
                | _ -> return (send_failed := true)
              ) 
            |. case (fun _ -> Some (fun () -> return ()))       
          end
        >>= fun res ->
        if res = None
        then return ()
        else receive_loop ()
      in 
      receive_loop ()      
    ) in      
    Test_io.(
    test_run_wrapper "test_send_remote_remote" (fun () -> 
        Test_io.async (fun () -> Consumer.run_node remote_config) ;            
      Producer.run_node node_config ~process:producer_proc >>= fun () ->
        List.fold_right (fun v acc -> v () >>= fun () -> acc) (get_atexit_fns ()) (return ()) >>= fun () ->
      assert_equal "send fail" 4 !sent_received ;
      assert_equal "sending to invalid process should have succeeded" true (not !send_failed) ;      
        assert_equal "remote config with 2 remote nodes should have establised 2 connections" 2 (Test_io.get_established_connection_count ()) ;
      
      return () 
    ) 
  )                               

(* tests for raise excpetions for local and remote configurations *)

let test_raise_local_config _ =
  let module P = Distributed.Make (Test_io) (M) in
  let node_config = P.Local {P.Local_config.node_name = "test" ;} in
  let expected_exception_happened = ref false in 
  let receive_exception_proc () = P.(
    receive @@
      case (fun _ -> Some (fun () -> fail Test_ex) )
    >>= fun _ -> return ()
  ) in
  let test_proc () = P.(                      
      get_self_node >>= fun local_node ->
      spawn ~monitor:true local_node (fun () -> return () >>= fun _ -> receive_exception_proc ()) >>= fun (new_pid, _) ->
      new_pid >! "foobar" >>= fun _ ->
      receive @@
        termination_case 
          (function
            | Exception (_,Test_ex) -> return (expected_exception_happened := true)
            | _ -> assert false
          )
      >>= fun _ ->            
      return ()        
    ) in             
    Test_io.(test_run_wrapper "test_raise_local_config" (fun () -> P.run_node node_config ~process:test_proc >>= fun () -> List.fold_right (fun v acc -> v () >>= fun () -> acc) (get_atexit_fns ()) (return ()))) ;
  assert_equal "expceted exception did not occur" true !expected_exception_happened ;
    assert_equal "local config should hav establised 0 connections" 0 (Test_io.get_established_connection_count ())    

let test_raise_local_remote_config _ =
  let module P = Distributed.Make (Test_io) (M) in
  let node_config = P.Remote { P.Remote_config.node_name = "producer" ; 
                               P.Remote_config.local_port = 45000 ;                               
                               P.Remote_config.connection_backlog = 10 ;
                               P.Remote_config.node_ip = "127.0.0.1" ;
                               P.Remote_config.remote_nodes = [] ;
                             } in
  let expected_exception_happened = ref false in 
  let receive_exception_proc () = P.(
    receive ~timeout_duration:0.05 @@
      case (fun _ -> Some (fun () -> fail Test_ex) )
    >>= fun _ -> return ()
  ) in 
  let test_proc () = P.(                      
      get_self_node >>= fun local_node ->
      spawn ~monitor:true local_node (fun () -> return () >>= fun _ -> receive_exception_proc ()) >>= fun (new_pid, _) ->
      new_pid >! "foobar" >>= fun _ ->
      receive @@
        termination_case 
          (function
            | Exception (_,Test_ex) -> return (expected_exception_happened := true)
            | _ -> assert false
          )
      >>= fun _ ->            
      return ()        
    ) in        
    Test_io.(
    test_run_wrapper "test_raise_local_remote_config" (fun () -> 
      P.run_node node_config ~process:test_proc >>= fun () -> 
        List.fold_right (fun v acc -> v () >>= fun () -> acc) (get_atexit_fns ()) (return ()) >>= fun () ->
      assert_equal "expceted exception did not occur" true !expected_exception_happened ;  
        assert_equal "remote config with only a single node should have establised 1 connection" 1 (Test_io.get_established_connection_count ()) ;  
      
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
                                      Producer.Remote_config.local_port = 45000 ;                                      
                                      Producer.Remote_config.connection_backlog = 10 ;
                                      Producer.Remote_config.node_ip = "127.0.0.1" ;
                                      Producer.Remote_config.remote_nodes = [("127.0.0.1",46000,"consumer")] ;
                                    } in
  let remote_config = Consumer.Remote { Consumer.Remote_config.node_name = "consumer" ; 
                                        Consumer.Remote_config.local_port = 46000 ;                                        
                                        Consumer.Remote_config.connection_backlog = 10 ;
                                        Consumer.Remote_config.node_ip = "127.0.0.1" ;
                                        Consumer.Remote_config.remote_nodes = [] ;
                                      } in
  let expected_exception_happened = ref false in                                 
  let receive_exception_proc () = Consumer.(
      receive @@
        case (fun _ -> Some (fun () -> fail Test_ex) )
      >>= fun _ -> return ()
    ) in 
  let producer_proc () = Producer.(
      get_remote_nodes >>= fun nodes ->
      get_self_pid >>= fun _ ->
      spawn ~monitor:true (List.hd nodes) (Consumer.(fun () -> return () >>= fun () -> receive_exception_proc ())) >>= fun (remote_pid, _) ->      
      remote_pid >! "foobar" >>= fun _ ->
      receive @@
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
      >>= fun _ ->
      return ()                       
    ) in
    Test_io.(
    test_run_wrapper "test_raise_remote_remote_config" (fun () -> 
        Test_io.async (fun () -> Consumer.run_node remote_config) ;                
      Producer.run_node node_config ~process:producer_proc >>= fun () ->
        List.fold_right (fun v acc -> v () >>= fun () -> acc) (get_atexit_fns ()) (return ()) >>= fun () ->      
      assert_equal "expceted exception did not occur" true !expected_exception_happened ;  
        assert_equal "remote config with 2 remote nodes should have establised 2 connections" 2 (Test_io.get_established_connection_count ()) ;      
      
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
      assert_equal "Process should not have spawned yet" true (not !result) ;
      spawn ~monitor:true local_node (fun () -> return () >>= fun _ -> return (result := true)) >>= fun (new_pid, _) ->
      receive @@
        termination_case
        (function
          | Normal _ -> return @@ Some ()
          | _ -> assert false
        )
      >>= fun _ ->
      monitor new_pid >>= fun _ ->
      receive @@
        termination_case
          (function
            | NoProcess _ -> return (result_monitor := Some ("got noprocess"))
            | _ -> assert false
          )
      >>= fun _ ->
      return ()        
    ) in             
    Test_io.(test_run_wrapper "test_monitor_dead_process_local_local_config" (fun () -> P.run_node node_config ~process:test_proc >>= fun () -> List.fold_right (fun v acc -> v () >>= fun () -> acc) (get_atexit_fns ()) (return ())));
  assert_equal "process was not spawned" true (!result && !result_monitor <> None) ; 
  assert_equal "did not get expected NoProcess monitor message" (Some "got noprocess") !result_monitor ;    
    assert_equal "local config should hav establised 0 connections" 0 (Test_io.get_established_connection_count ())    

let test_monitor_dead_process_local_remote_config _ =
  let module P = Distributed.Make (Test_io) (M) in  
  let node_config = P.Remote { P.Remote_config.node_name = "producer" ; 
                               P.Remote_config.local_port = 45000 ;                               
                               P.Remote_config.connection_backlog = 10 ;
                               P.Remote_config.node_ip = "127.0.0.1" ;
                               P.Remote_config.remote_nodes = [] ;
                             } in
  let result = ref false in
  let result_monitor = ref None in  
  let test_proc () = P.(                                        
      get_self_node >>= fun local_node ->
      assert_equal "Process should not have spawned yet" true (not !result) ;
      spawn ~monitor:true local_node (fun () -> return () >>= fun _ -> return (result := true)) >>= fun (new_pid, _) ->
      receive @@
        termination_case
        (function
          | Normal _ -> return @@ Some ()
          | _ -> assert false
        )
      >>= fun _ ->      
      monitor new_pid >>= fun _ ->
      receive @@
        termination_case
          (function
            | NoProcess _ -> return (result_monitor := Some ("got noprocess"))
            | _ -> assert false
          )
      >>= fun _ ->
      return ()        
    ) in             
    Test_io.(
    test_run_wrapper "test_monitor_dead_process_local_remote_config" (fun () -> 
      P.run_node node_config ~process:test_proc >>= fun () -> 
        List.fold_right (fun v acc -> v () >>= fun () -> acc) (get_atexit_fns ()) (return ()) >>= fun () ->
      assert_equal "process was not spawned" true (!result && !result_monitor <> None) ;
      assert_equal "did not get expected NoProcess monitor message" (Some "got noprocess") !result_monitor ;      
        assert_equal "remote config with only a single node should have establised 1 connection" 1 (Test_io.get_established_connection_count ()) ;
      
      return ()    
    )
  )

let test_monitor_dead_process_remote_remote_config _ =
  let module Producer = Distributed.Make (Test_io) (M) in
  let module Consumer = Distributed.Make (Test_io) (M) in  
  let node_config = Producer.Remote { Producer.Remote_config.node_name = "producer" ; 
                                      Producer.Remote_config.local_port = 45000 ;                                      
                                      Producer.Remote_config.connection_backlog = 10 ;
                                      Producer.Remote_config.node_ip = "127.0.0.1" ;
                                      Producer.Remote_config.remote_nodes = [("127.0.0.1",46000,"consumer")] ;
                                    } in
  let remote_config = Consumer.Remote { Consumer.Remote_config.node_name = "consumer" ; 
                                        Consumer.Remote_config.local_port = 46000 ;                                        
                                        Consumer.Remote_config.connection_backlog = 10 ;
                                        Consumer.Remote_config.node_ip = "127.0.0.1" ;
                                        Consumer.Remote_config.remote_nodes = [] ;
                                      } in
  let result_monitor = ref None in    
  let producer_proc () = Producer.(
      get_remote_nodes >>= fun nodes ->
      spawn ~monitor:true (List.hd nodes) (fun () -> return () >>= fun () -> return ()) >>= fun (remote_pid, _) ->
      receive @@
        termination_case
        (function
          | Normal _ -> return @@ Some ()
          | _ -> assert false
        )
      >>= fun _ ->
      monitor remote_pid >>= fun _ ->      
      receive @@
        termination_case
          (function
            | NoProcess _ -> return (result_monitor := Some ("got noprocess"))
            | _ -> assert false
          )
      >>= fun _ ->
      return ()                 
    ) in
    Test_io.(
    test_run_wrapper "test_monitor_dead_process_remote_remote_config" (fun () -> 
        Test_io.async (fun () -> Consumer.run_node remote_config) ;            
      Producer.run_node node_config ~process:producer_proc >>= fun () ->
        List.fold_right (fun v acc -> v () >>= fun () -> acc) (get_atexit_fns ()) (return ()) >>= fun () ->      
      assert_equal "did not get expected NoProcess monitor message" (Some "got noprocess") !result_monitor ;      
        assert_equal "remote config with 2 remote nodes should have establised 2 connections" 2 (Test_io.get_established_connection_count ()) ;
      
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
    Test_io.(test_run_wrapper "test_add_remove_remote_nodes_in_local_config" (fun () -> P.run_node node_config ~process:test_proc >>= fun () -> List.fold_right (fun v acc -> v () >>= fun () -> acc) (get_atexit_fns ()) (return ())));
  assert_equal "add_remote_node should have throw Local_only_mode exception when running with a local only config" true !add_pass ; 
  assert_equal "remove_remote_node should have throw Local_only_mode exception when running with a local only config" true !remove_pass ;    
    assert_equal "local config should hav establised 0 connections" 0 (Test_io.get_established_connection_count ())    

(* test adding/removing nodes in remote configurations. *)

let test_add_remove_nodes_remote_config _ =
  let module Producer = Distributed.Make (Test_io) (M) in
  let module Consumer = Distributed.Make (Test_io) (M) in  
  let node_config = Producer.Remote { Producer.Remote_config.node_name = "producer" ; 
                                      Producer.Remote_config.local_port = 45000 ;                                      
                                      Producer.Remote_config.connection_backlog = 10 ;
                                      Producer.Remote_config.node_ip = "127.0.0.1" ;
                                      Producer.Remote_config.remote_nodes = [] ; (* start with with no node connections *)
                                    } in
  let remote_config = Consumer.Remote { Consumer.Remote_config.node_name = "consumer" ; 
                                        Consumer.Remote_config.local_port = 46000 ;                                        
                                        Consumer.Remote_config.connection_backlog = 10 ;
                                        Consumer.Remote_config.node_ip = "127.0.0.1" ;
                                        Consumer.Remote_config.remote_nodes = [] ;
                                      } in
  let remote_nodes_at_start = ref [] in
  let remote_nodes_after_remove = ref [] in
  let remote_nodes_after_add = ref [] in   
  let remote_nodes_after_dup_add = ref [] in   
  let expected_spawn_exception = ref false in
  let expected_monitor_exception = ref false in
  let expected_broadcast_exception = ref false in   
  let expected_send_exception = ref false in
  let producer_proc () = Producer.(
      get_remote_nodes >>= fun nodes ->
      remote_nodes_at_start := nodes ;
      add_remote_node "127.0.0.1" 46000 "consumer" >>= fun consumer_node ->
      get_remote_nodes >>= fun nodes_after_add ->
      remote_nodes_after_add := nodes_after_add ;
      add_remote_node "127.0.0.1" 46000 "consumer" >>= fun _ ->
      get_remote_nodes >>= fun nodes_after_add_dup ->
      remote_nodes_after_dup_add := nodes_after_add_dup ;
      spawn consumer_node (fun () -> return () >>= fun _ -> lift_io @@ Test_io.sleep 0.1) >>= fun (rpid,_) ->
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
    Test_io.(
    test_run_wrapper "test_add_remove_nodes_remote_config" (fun () -> 
        Test_io.async (fun () -> Consumer.run_node remote_config) ;            
      Producer.run_node node_config ~process:producer_proc >>= fun () ->
        List.fold_right (fun v acc -> v () >>= fun () -> acc) (get_atexit_fns ()) (return ()) >>= fun () ->      
      assert_equal "remote nodes should have been empty before adding" 0 (List.length !remote_nodes_at_start) ;
      assert_equal "remote nodes should have 1 remote node after adding" 1 (List.length !remote_nodes_after_add) ;      
      assert_equal "remote nodes should have 1 remote node after adding a dup" 1 (List.length !remote_nodes_after_dup_add) ;      
      assert_equal "remote nodes should have been empty after removing" 0 (List.length !remote_nodes_after_remove) ;
      assert_equal "expected InvalidNode exception did not occur when monitoring on removed node" true !expected_monitor_exception ;
      assert_equal "expected InvalidNode exception did not occur when spawning on removed node" true !expected_spawn_exception ;
      assert_equal "expected InvalidNode exception did not occur when broadcasting on removed node" true !expected_broadcast_exception ;
      assert_equal "expected InvalidNode exception did not occur when sending message on removed node" true !expected_send_exception ;            
        assert_equal "remote config with 2 remote nodes should have establised 2 connections" 2 (Test_io.get_established_connection_count ()) ;
      
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
      receive @@
        case (
          function 
          | "the one" -> Some (fun () -> return (selective_message := Some "the one"))
          | _ -> None
        ) 
      >>= fun _ ->
      receive_loop ~timeout_duration:0.1 @@
        case (fun v -> Some (fun () -> other_messages_inorder := (v::(!other_messages_inorder)) ; return (v <> "0")))         
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
    Test_io.(test_run_wrapper "test_selective_receive_local_config" (fun () -> P.run_node node_config ~process:main_proc >>= fun () -> List.fold_right (fun v acc -> v () >>= fun () -> acc) (get_atexit_fns ()) (return ()))) ;
  assert_equal "selective receive failed, candidate message" (Some "the one") !selective_message ;
  assert_equal "selective receive failed, other messages" ["0" ; "1" ; "2" ; "3" ; "4" ; "5"] !other_messages_inorder ;
    assert_equal "local config should hav establised 0 connections" 0 (Test_io.get_established_connection_count ())     

let test_selective_receive_local_remote_config _ =
  let module P = Distributed.Make (Test_io) (M) in
  let node_config = P.Remote { P.Remote_config.node_name = "producer" ; 
                               P.Remote_config.local_port = 45000 ;                               
                               P.Remote_config.connection_backlog = 10 ;
                               P.Remote_config.node_ip = "127.0.0.1" ;
                               P.Remote_config.remote_nodes = [] ;
                             } in
  let selective_message = ref None in
  let other_messages_inorder = ref [] in  
  let receiver_proc () = P.(                      
      receive @@
        case (
          function 
          | "the one" -> Some (fun () -> return (selective_message := Some "the one"))
          | _ -> None
        ) 
      >>= fun _ ->
      receive_loop ~timeout_duration:0.1 @@
        case (fun v -> Some (fun () -> other_messages_inorder := (v::(!other_messages_inorder)) ; return (v <> "0")))               
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
    Test_io.(
    test_run_wrapper "test_selective_receive_local_remote_config" (fun () -> 
      P.run_node node_config ~process:main_proc >>= fun () -> 
        List.fold_right (fun v acc -> v () >>= fun () -> acc) (get_atexit_fns ()) (return ()) >>= fun () ->
      assert_equal "selective receive failed, candidate message" (Some "the one") !selective_message ;
      assert_equal "selective receive failed, other messages" ["0" ; "1" ; "2" ; "3" ; "4" ; "5"] !other_messages_inorder ;
        assert_equal "remote config with only a single node should have establised 1 connection" 1 (Test_io.get_established_connection_count ()) ;
      
      return ()                      
    )
  )

let test_selective_receive_remote_remote_config _ =
  let module Producer = Distributed.Make (Test_io) (M) in
  let module Consumer = Distributed.Make (Test_io) (M) in  
  let node_config = Producer.Remote { Producer.Remote_config.node_name = "producer" ; 
                                      Producer.Remote_config.local_port = 45000 ;                                      
                                      Producer.Remote_config.connection_backlog = 10 ;
                                      Producer.Remote_config.node_ip = "127.0.0.1" ;
                                      Producer.Remote_config.remote_nodes = [("127.0.0.1",46000,"consumer")] ;
                                    } in
  let remote_config = Consumer.Remote { Consumer.Remote_config.node_name = "consumer" ; 
                                        Consumer.Remote_config.local_port = 46000 ;                                        
                                        Consumer.Remote_config.connection_backlog = 10 ;
                                        Consumer.Remote_config.node_ip = "127.0.0.1" ;
                                        Consumer.Remote_config.remote_nodes = [] ;
                                      } in
  
  let messages_inorder = ref [] in 
                                                                            
  let receiver_proc result_pid () = Consumer.(
      let to_send = ref [] in
      let rec send_all msgs () =
        match msgs with
        | [] -> return ()
        | m::ms -> result_pid >! m >>= fun () -> send_all ms ()        
      in                      
      receive @@
        case (
          function 
          | "the one" -> Some (fun () -> return (to_send := "the one"::!to_send))
          | _ -> None
        ) 
      >>= fun _ ->
      receive_loop ~timeout_duration:0.1 @@
        case (fun v -> Some (fun () -> to_send := (v::(!to_send)) ; return (v <> "0")))
      >>= fun _ ->
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
      receive_loop ~timeout_duration:0.1 @@
        case (fun v -> Some (fun () -> messages_inorder := (v::(!messages_inorder)) ; return (v <> "the one")))           
    ) in
  let main_proc () = Producer.(
      get_self_node >>= fun self_node ->
      get_remote_nodes >>= fun remote_nodes ->
      spawn self_node result_receiver >>= fun (res_rec_pid,_) ->
      spawn (List.hd remote_nodes) (receiver_proc res_rec_pid) >>= fun (rpid,_) ->
      spawn self_node (sender_proc rpid) >>= fun _ ->
      lift_io (Test_io.sleep 0.1)
    ) in
    Test_io.(
    test_run_wrapper "test_selective_receive_remote_remote_config" (fun () -> 
        Test_io.async (fun () -> Consumer.run_node remote_config) ;  
      Producer.run_node node_config ~process:main_proc >>= fun () ->
        List.fold_right (fun v acc -> v () >>= fun () -> acc) (get_atexit_fns ()) (return ()) >>= fun () ->  
      assert_equal "selective receive failed, other messages"  ["the one" ; "5" ; "4" ; "3" ; "2" ; "1" ; "0"] !messages_inorder ;
        assert_equal "remote config with 2 remote nodes should have establised 2 connections" 2 (Test_io.get_established_connection_count ()) ;
      
      return ()
    ) 
  )   

(* test that calling run_node more than once results in an exception. *)

let test_multiple_run_node _ =
  let module P = Distributed.Make (Test_io) (M) in
  let node_config = P.Local {P.Local_config.node_name = "test" ;} in
  let exception_thrown = ref None in

    Test_io.(test_run_wrapper "test_multiple_run_node" (fun () -> 
      P.run_node node_config >>= fun () ->
      catch
        (fun () -> P.run_node node_config)
        (function
          | P.Init_more_than_once -> exception_thrown := Some true ; return ()
            | _ -> return ()) >>= fun () -> List.fold_right (fun v acc -> v () >>= fun () -> acc) (get_atexit_fns ()) (return ())
    )) ;
    assert_equal "Init more than once failed, did not get exception" (Some true) !exception_thrown ;
      assert_equal "should hav established 0 connections" 0 (Test_io.get_established_connection_count ()) 

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

    Test_io.(test_run_wrapper "test_get_remote_node_local_only" (fun () -> (P.run_node node_config ~process:p)  >>= fun _ -> List.fold_right (fun v acc -> v () >>= fun () -> acc) (get_atexit_fns ()) (return ()))) ;
  assert_equal "get_remote_node failed locally, self node was in remote nodes" (Some "ran") !self_remote_node_result ;
  assert_equal "get_remote_node failed locally, nonexistent node was in remote nodes" (Some "ran") !nonexistent_remote_node_result ;
    assert_equal "should have established 0 connections" 0 (Test_io.get_established_connection_count ())

let test_get_remote_node_local_remote_config _ =
  let module P = Distributed.Make (Test_io) (M) in
  let node_config = P.Remote { P.Remote_config.node_name = "producer" ; 
                               P.Remote_config.local_port = 45000 ;                               
                               P.Remote_config.connection_backlog = 10 ;
                               P.Remote_config.node_ip = "127.0.0.1" ;
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
    Test_io.(
    test_run_wrapper "test_get_remote_node_local_remote_config" (fun () -> 
      (P.run_node node_config ~process:p)  >>= fun () -> 
        List.fold_right (fun v acc -> v () >>= fun () -> acc) (get_atexit_fns ()) (return ()) >>= fun () ->
      assert_equal "get_remote_node failed locally with remote config, self node was in remote nodes" (Some "ran") !self_remote_node_result ;
      assert_equal "get_remote_node failed locally with remote config, nonexistent node was in remote nodes" (Some "ran") !nonexistent_remote_node_result ;
      
      return ()  
    )
  )

let test_get_remote_node_remote_remote_config _ =
  let module Producer = Distributed.Make (Test_io) (M) in
  let module Consumer = Distributed.Make (Test_io) (M) in  
  let node_config = Producer.Remote { Producer.Remote_config.node_name = "producer" ; 
                                      Producer.Remote_config.local_port = 45000 ;                                      
                                      Producer.Remote_config.connection_backlog = 10 ;
                                      Producer.Remote_config.node_ip = "127.0.0.1" ;
                                      Producer.Remote_config.remote_nodes = [("127.0.0.1",46000,"consumer")] ;
                                    } in
  let remote_config = Consumer.Remote { Consumer.Remote_config.node_name = "consumer" ; 
                                        Consumer.Remote_config.local_port = 46000 ;                                        
                                        Consumer.Remote_config.connection_backlog = 10 ;
                                        Consumer.Remote_config.node_ip = "127.0.0.1" ;
                                        Consumer.Remote_config.remote_nodes = [] ;
                                      } in

  let nonexistent_remote_node_result = ref (Some "") in
  let self_remote_node_result = ref (Some "") in
  let exitent_remote_node_result = ref (Some "") in

  let p2 () = Consumer.(    
      exitent_remote_node_result := Some "ran" ; return ()    
    ) in 

  let p () = Producer.(      
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
             receive @@ termination_case (function _ -> return ())) >>= fun _ -> return ())
    ) in 

    Test_io.(
    test_run_wrapper "test_get_remote_node_remote_remote_config" (fun () -> 
        Test_io.async (fun () -> Consumer.run_node remote_config) ;  
      Producer.run_node node_config ~process:p >>= fun () ->
        List.fold_right (fun v acc -> v () >>= fun () -> acc) (get_atexit_fns ()) (return ()) >>= fun () ->      
      assert_equal "get_remote_node failed remotely, self node was in remote nodes" (Some "ran") !self_remote_node_result ;
      assert_equal "get_remote_node failed remotely, nonexistent node was in remote nodes" (Some "ran") !nonexistent_remote_node_result ;
      assert_equal "get_remote_node failed remotely, existent node was not in remote nodes" (Some "ran") !exitent_remote_node_result ;
        assert_equal "remote config with 2 remote nodes should have establised 2 connections" 2 (Test_io.get_established_connection_count ()) ;
      
      return () 
    ) 
  )    

  let suite = [
    "Test return and bind"                                                ,   test_return_bind ;

    "Test spawn local with local config"                                  ,   test_spawn_local_local_config ;
    "Test spawn local with remote config"                                 ,   test_spawn_local_remote_config ;
    "Test spawn remote with remote config"                                ,   test_spawn_remote_remote_config ;

    "Test spawn monitor local with local config"                          ,   test_spawn_monitor_local_local_config;
    "test spawn monitor local with remote config"                         ,   test_spawn_monitor_local_remote_config ;
    "Test spawn monitor remote with remote config"                        ,   test_spawn_monitor_remote_remote_config ;

    "Test monitor local with local config"                                ,   test_monitor_local_local_config ;  
    "Test monitor local remote config"                                    ,   test_monitor_local_remote_config ;
    "Test monitor remote with remote config"                              ,   test_monitor_remote_remote_config ;

    "Test monitor dead process local with local config"                   ,   test_monitor_dead_process_local_local_config ;  
    "Test monitor dead local remote config"                               ,   test_monitor_dead_process_local_remote_config ;
    "Test monitor dead remote with remote config"                         ,   test_monitor_dead_process_remote_remote_config ;

    "Test unmonitor local with local config"                              ,   test_unmonitor_local_local_config ;  
    "Test unmonitor local with remote config"                             ,   test_unmonitor_local_remote_config ;
    "Test unmonitor remote with remote config"                            ,   test_unmonitor_remote_remote_config ;    

    "Test unmonitor from spawn monitor local with local config"           ,   test_unmonitor_from_spawn_monitor_local_local_config ;  
    "Test unmonitor from spawn monitor local remote config"               ,   test_unmonitor_from_spawn_monitor_local_remote_config ;
    "Test unmonitor from spawn monitor remote with remote config"         ,   test_unmonitor_from_spawn_monitor_remote_remote_config ;    

    "Test get remote nodes local with local config"                       ,   test_get_remote_nodes_local_only ;
    "Test get remote nodes local with remote config"                      ,   test_get_remote_nodes_remote_local ;             
    "Test get remote nodes remote with remote conifg"                     ,   test_get_remote_nodes_remote_conifg ; 

    "Test broadcast local with local config"                              ,   test_broadcast_local_only ;
    "Test broadcast local with remote config"                             ,   test_broadcast_remote_local ; 
    "Test broadcast remote and local with remote config"                  ,   test_broadcast_remote_remote ;    

    "Test send local with local config"                                   ,   test_send_local_only ;  
    "Test send local with remote config"                                  ,   test_send_remote_local ; 
    "Test send remote and local with remote config"                       ,   test_send_remote_remote ; 

    "Test raise exception on monitored process local with local config"   ,   test_raise_local_config;
    "Test raise exception on monitored process local with remote config"  ,   test_raise_local_remote_config ;
    "Test raise exception on monitored process remote with remote config" ,   test_raise_remote_remote_config ;

    "Test add/remove remote node in local only config"                    ,   test_add_remove_remote_nodes_in_local_config ;
    "Test add/remove remote nodes with remote config"                     ,   test_add_remove_nodes_remote_config ; 

    "Test selective receive with local only config"                       ,   test_selective_receive_local_config ;
    "Test selective_receive local with remoteconfig"                      ,   test_selective_receive_local_remote_config ;
    "Test selective receive remote with remote config"                    ,   test_selective_receive_remote_remote_config ;

    "Test multiple run node calls"                                        ,   test_multiple_run_node ;

    "Test get_remote_node local only"                                     ,   test_get_remote_node_local_only ;
    "Test get_remote_node local with remote config"                       ,   test_get_remote_node_local_remote_config ;
    "Test get_remote_node_remote remote config"                           ,   test_get_remote_node_remote_remote_config ;       
  ]

  let run_suite () =
    run_tests "Test Distributed" suite ;   

end

(*BISECT-IGNORE-END*)
