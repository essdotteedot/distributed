module Node_id = struct

  type t = { ip   : Unix.inet_addr option ;
             port : int option;
             name : string ; 
           }

  let make_local_node name = 
    {ip = None ; port = None ; name }

  let make_remote_node ipStr port name = 
    {ip = Some (Unix.inet_addr_of_string ipStr) ; port = Some port ; name}

  let is_local node local_node = node.ip = local_node.ip && node.port = local_node.port  

  let get_name {name ; _} = name 

  let string_of_node node =
    let string_of_ip = if node.ip = None then "None" else Unix.string_of_inet_addr @@ Potpourri.get_option node.ip in 
    let string_of_port = if node.port = None then "None" else string_of_int @@ Potpourri.get_option node.port in
    Format.sprintf "{ip : %s ; port : %s ; name : %s}" string_of_ip string_of_port node.name

  let get_ip {ip ; _} = ip

  let get_port {port ; _} = port        

end

module Node_id_seeded_hash_type = struct
  type t = Node_id.t 

  let equal (n1 : t) (n2 : t) : bool = 
    (Node_id.get_ip n1, Node_id.get_port n1) = (Node_id.get_ip n2, Node_id.get_port n2)  

  let hash (seed : int) (n : t) : int = 
    Hashtbl.seeded_hash seed (Node_id.get_ip n,Node_id.get_port n)
end

module Node_id_hashtbl = Hashtbl.MakeSeeded(Node_id_seeded_hash_type)

module Process_id = struct

  let next_process_id = ref 0

  type t = {node    : Node_id.t ; 
            proc_id : int ;                 
           }

  let make nid pid = {node = nid ; proc_id = pid}         

  let make_local name = 
    {node = Node_id.make_local_node name ; proc_id = Potpourri.post_incr next_process_id} 

  let make_remote ipStr port name =
    {node = Node_id.make_remote_node ipStr port name ; proc_id = Potpourri.post_incr next_process_id}

  let is_local {node ; _} local_node = Node_id.is_local node local_node     

  let get_node {node ; _ } = node       

  let get_id {proc_id ; _} = proc_id

  let string_of_pid p = Format.sprintf "{node : %s ; id : %d}" (Node_id.string_of_node p.node) p.proc_id

end

module Process_id_seeed_hash_type = struct
  type t = Process_id.t 

  let equal (p1 : t) (p2 : t) : bool = 
    let p1_ip = Node_id.get_ip @@ Process_id.get_node p1 in
    let p1_port = Node_id.get_port @@ Process_id.get_node p1 in
    let p1_id = Process_id.get_id p1 in

    let p2_ip = Node_id.get_ip @@ Process_id.get_node p2 in
    let p2_port = Node_id.get_port @@ Process_id.get_node p2 in
    let p2_id = Process_id.get_id p2 in

    (p1_ip,p1_port,p1_id) = (p2_ip,p2_port,p2_id) 

  let hash (seed : int) (p : t) : int = 
    let p_ip = Node_id.get_ip @@ Process_id.get_node p in
    let p_port = Node_id.get_port @@ Process_id.get_node p in
    let p_id = Process_id.get_id p in
    Hashtbl.seeded_hash seed (p_ip,p_port,p_id)
end

module Process_id_hashtbl = Hashtbl.MakeSeeded(Process_id_seeed_hash_type)

module type Nonblock_io = sig

  type 'a t

  type 'a stream

  type input_channel

  type output_channel

  type server

  type logger

  type level = Debug 
             | Info
             | Notice
             | Warning
             | Error
             | Fatal

  exception Timeout

  val lib_name : string 

  val lib_version : string 

  val lib_description : string            

  val return : 'a -> 'a t

  val (>>=) : 'a t -> ('a -> 'b t) -> 'b t

  val ignore_result : 'a t -> unit

  val fail : exn -> 'a t    

  val catch : (unit -> 'a t) -> (exn -> 'a t) -> 'a t

  val async : (unit -> 'a t) -> unit

  val create_stream : unit -> 'a stream * ('a option -> unit)

  val get : 'a stream -> 'a option t          

  val stream_append :  'a stream -> 'a stream -> 'a stream          

  val close_input : input_channel -> unit t

  val close_output : output_channel -> unit t    

  val read_value : input_channel -> 'a t

  val write_value : output_channel -> ?flags:Marshal.extern_flags list -> 'a -> unit t   

  val open_connection : Unix.sockaddr -> (input_channel * output_channel) t

  val establish_server : ?backlog:int -> Unix.sockaddr -> (input_channel * output_channel -> unit) -> server

  val shutdown_server : server -> unit  

  val log : ?exn:exn -> ?location:string * int * int -> logger:logger -> level:level -> string -> unit t 

  val sleep : float -> unit t     

  val timeout : float -> 'a t

  val pick : 'a t list -> 'a t  

  val at_exit : (unit -> unit t) -> unit

end

module type Message_type = sig

  type t

  val string_of_message : t -> string
end

module type Process = sig
  exception Init_more_than_once

  exception Empty_matchers    

  exception InvalidNode of Node_id.t

  exception Local_only_mode

  type 'a io

  type 'a t

  type message_type    

  type 'a matcher

  type monitor_ref

  type logger

  type monitor_reason = Normal of Process_id.t                  
                      | Exception of Process_id.t * exn         
                      | UnkownNodeId of Process_id.t * Node_id.t 
                      | NoProcess of Process_id.t                                          

  module Remote_config : sig
    type t = { remote_nodes         : (string * int * string) list ;
               local_port           : int                          ;
               heart_beat_timeout   : float                        ;
               heart_beat_frequency : float                        ;
               connection_backlog   : int                          ;  
               node_name            : string                       ;
               node_ip              : string                       ;
               logger               : logger                       ;
             }    
  end                 

  module Local_config : sig
    type t = { node_name          : string ;
               logger             : logger ;
             }
  end

  type node_config = Local of Local_config.t
                   | Remote of Remote_config.t 

  val return : 'a -> 'a t

  val (>>=) : 'a t -> ('a -> 'b t) -> 'b t

  val fail : exn -> 'a t        

  val catch : (unit -> 'a t) -> (exn -> 'a t) -> 'a t        

  val spawn : ?monitor:bool -> Node_id.t -> unit t -> (Process_id.t * monitor_ref option) t 

  val case : (message_type -> bool) -> (message_type -> 'a t) -> 'a matcher

  val termination_case : (monitor_reason -> 'a t) -> 'a matcher

  val receive : ?timeout_duration:float -> 'a matcher list -> 'a option t

  val receive_loop : ?timeout_duration:float -> bool matcher list -> unit t    

  val send : Process_id.t -> message_type -> unit t

  val (>!) : Process_id.t -> message_type -> unit t

  val broadcast : Node_id.t -> message_type -> unit t

  val monitor : Process_id.t -> monitor_ref t

  val unmonitor : monitor_ref -> unit t 

  val get_self_pid : Process_id.t t

  val get_self_node : Node_id.t t

  val get_remote_node : string -> Node_id.t option t

  val get_remote_nodes : Node_id.t list t

  val add_remote_node : string -> int -> string -> Node_id.t t   

  val remove_remote_node : Node_id.t -> unit t    

  val lift_io : 'a io -> 'a t    

  val run_node : ?process:unit t -> ?node_monitor_fn:(Node_id.t -> unit t) -> node_config -> unit io

end

module Make (I : Nonblock_io) (M : Message_type) : (Process with type message_type = M.t and type 'a io = 'a I.t and type logger = I.logger) = struct    
  exception Init_more_than_once    

  exception Empty_matchers

  exception InvalidNode of Node_id.t

  exception Local_only_mode          

  type 'a io = 'a I.t    

  type message_type = M.t    

  type monitor_ref = Monitor_Ref of int * Process_id.t * Process_id.t  (* unique id, the process doing the monitoring and the process being monitored *)

  type logger = I.logger

  type monitor_reason = Normal of Process_id.t                  
                      | Exception of Process_id.t * exn         
                      | UnkownNodeId of Process_id.t * Node_id.t 
                      | NoProcess of Process_id.t   

  module Remote_config = struct
    type t = { remote_nodes         : (string * int * string) list ;
               local_port           : int                          ;
               heart_beat_timeout   : float                        ;
               heart_beat_frequency : float                        ;
               connection_backlog   : int                          ;  
               node_name            : string                       ;
               node_ip              : string                       ;
               logger               : logger                       ;
             }    
  end                 

  module Local_config = struct
    type t = { node_name          : string ;
               logger             : logger ;
             }
  end

  type node_config = Local of Local_config.t
                   | Remote of Remote_config.t

  module Monitor_ref_order_type = struct
    type t = monitor_ref

    let compare (Monitor_Ref (id1,_,_) : t) (Monitor_Ref (id2,_,_) : t) : int = 
      compare id1 id2
  end                 

  module Monitor_ref_set = Set.Make(Monitor_ref_order_type)  

  type message = Data of Process_id.t * Process_id.t * message_type                  (* sending process id, receiving process id and the message *)
               | Broadcast of Process_id.t * Node_id.t * message_type                (* sending process id, receiving node and the message *)
               | Proc of unit t * Process_id.t                                       (* the process to be spawned elsewhere and the process that requested the spawning *)
               | Spawn_monitor of unit t * Process_id.t * Process_id.t               (* the process to be spawned elsewhere, the monitoring process and the process that requested the spawning.*)
               | Node of Node_id.t                                                   (* initial message sent to remote node to identify ourselves *)
               | Heartbeat                                                           (* heartbeat message *)
               | Exit of Process_id.t * monitor_reason                               (* process that was being monitored and the reason for termination *)
               | Monitor of Process_id.t * Process_id.t * Process_id.t               (* the process doing the monitoring and the id of the process to be monitored and the process that requested the monitoring *)
               | Unmonitor of monitor_ref * Process_id.t                             (* process to unmonitor and the process that requested the unmonitor *)
               | Proc_result of Process_id.t * Process_id.t                          (* result of spawning a process and the receiver process id *)
               | Spawn_monitor_result of message option * monitor_ref * Process_id.t (* result of spawning and monitoring a process and the receiver process id *)
               | Monitor_result of message option * monitor_ref * Process_id.t       (* result of monitor and the receiving process *)
               | Unmonitor_result of monitor_ref * Process_id.t                      (* monitor ref that was requested to be unmonitored and the receiving process *)                     

  and node_state = { mailboxes                : (int, message I.stream * (message option -> unit)) Hashtbl.t ; 
                     remote_nodes             : I.output_channel Node_id_hashtbl.t ;
                     remote_nodes_heart_beats : bool Node_id_hashtbl.t ;
                     monitor_table            : Monitor_ref_set.t Process_id_hashtbl.t ;
                     local_node               : Node_id.t ;
                     logger                   : I.logger ;
                     monitor_ref_id           : int ref ;
                     config                   : Remote_config.t option ref ;
                     node_mon_fn              : (Node_id.t -> unit t) option
                   }                   

  and 'a t = (node_state * Process_id.t) -> (node_state * Process_id.t * 'a) io                    

  type 'a matcher = (message -> bool) * (message -> 'a t)                 

  let initalised = ref false    

  let dist_lib_version = "0.3.0"       

  let string_of_termination_reason (reason : monitor_reason) : string =
    match reason with
    | Normal pid -> 
      Format.sprintf "{termination reason : normal ; pid : %s}" @@ Process_id.string_of_pid pid
    | Exception (pid,e) -> 
      Format.sprintf "{termination reason : exception %s ; pid : %s}" (Printexc.to_string e) (Process_id.string_of_pid pid) 
    | UnkownNodeId (pid,n) -> 
      Format.sprintf "{termination reason : unknown node id %s ; pid : %s}" (Node_id.string_of_node n) (Process_id.string_of_pid pid)
    | NoProcess p -> 
      Format.sprintf "{termination reason : unknown process %s}" @@ Process_id.string_of_pid p 

  let string_of_monitor_ref (Monitor_Ref (id,pid,monitee_pid)) : string =
    Format.sprintf "{id : %d ; monitor process : %s : monitee process %s}" 
      id 
      (Process_id.string_of_pid pid) 
      (Process_id.string_of_pid monitee_pid)            

  let string_of_monitor_notification (Monitor_Ref (id,pid,monitee_pid)) (reason : monitor_reason) : string =
    Format.sprintf "{id : %d ; monitor process : %s : monitee process %s ; reason : %s}" 
      id 
      (Process_id.string_of_pid pid) 
      (Process_id.string_of_pid monitee_pid) 
      (string_of_termination_reason reason)

  let rec string_of_message (m : message) : string =
    match m with
    | Data (sender,recver,msg) -> 
      Format.sprintf "Data : {sender pid : %s ; receiver pid : %s ;  message : %s}" 
        (Process_id.string_of_pid sender) (Process_id.string_of_pid recver) (M.string_of_message msg)
    | Broadcast (sender, recv_node, msg) -> 
      Format.sprintf "Broadcast : {sender pid : %s ; receiver node : %s ; message : %s}" 
        (Process_id.string_of_pid sender) (Node_id.string_of_node recv_node) (M.string_of_message msg)
    | Proc (_,sender_pid) -> 
      Format.sprintf "Proc { <process> ; sender pid : %s" (Process_id.string_of_pid sender_pid)
    | Spawn_monitor (_,pid,sender) -> 
      Format.sprintf "Spawn and monitor {<process> ; monitor pid : %s ; sender pid %s}" (Process_id.string_of_pid pid) (Process_id.string_of_pid sender)
    | Node nid -> 
      Format.sprintf "Node %s" (Node_id.string_of_node nid)
    | Heartbeat -> 
      "Heartbeat"
    | Exit (pid,mreason) -> 
      Format.sprintf "Exit : {exit pid : %s ; reason : %s}" (Process_id.string_of_pid pid) (string_of_termination_reason mreason)
    | Monitor (monitor_pid,monitee_pid,sender) -> 
      Format.sprintf "Monitor : {monitor pid : %s ; monitee pid : %s ; sender pid : %s}"
        (Process_id.string_of_pid monitor_pid) (Process_id.string_of_pid monitee_pid) (Process_id.string_of_pid sender)
    | Unmonitor (mref,sender) -> 
      Format.sprintf "Unmonitor : {monitor reference to unmonitor : %s ; sender pid : %s}" (string_of_monitor_ref mref) (Process_id.string_of_pid sender)
    | Proc_result (pid, recv_pid) -> 
      Format.sprintf "Proc result {spawned pid : %s ; receiver pid : %s}" (Process_id.string_of_pid pid) (Process_id.string_of_pid recv_pid)
    | Spawn_monitor_result (monitor_msg,monitor_res,receiver) -> 
      Format.sprintf "Spawn and monitor result {monitor message : %s ; monitor result : %s : receiver pid : %s}" 
        (Potpourri.map_default_option string_of_message "" monitor_msg) (string_of_monitor_ref monitor_res) (Process_id.string_of_pid receiver)
    | Monitor_result (monitor_msg,monitor_res,receiver) -> 
      Format.sprintf "Monitor result {monitor message : %s ; monitor result : %s ; receiver pid : %s}" 
        (Potpourri.map_default_option string_of_message "" monitor_msg) (string_of_monitor_ref monitor_res) (Process_id.string_of_pid receiver)
    | Unmonitor_result (mref,pid) -> 
      Format.sprintf "Unmonitor result : {monittor reference to unmonitor: %s ; receiver pid : %s}" 
        (string_of_monitor_ref mref) (Process_id.string_of_pid pid)       

  let string_of_config (c : node_config) : string =
    match c with
    | Local l -> Format.sprintf "{node type : local ; node name : %s}" l.Local_config.node_name
    | Remote r ->      
      let remote_nodes = 
        Potpourri.pp_list 
          ~first:"[" ~last:"]" ~sep:";" r.Remote_config.remote_nodes
          (fun (ip,port,name) -> Format.sprintf "%s:%d, name : %s" ip port name) in               
      Format.sprintf 
        "{node type : remote ; remote nodes : %s ; local port : %d ; heart beat time out : %f ;heart beat frequency : %f ; \
         connection backlog : %d ; node name : %s ; node ip : %s}" 
        remote_nodes r.Remote_config.local_port r.Remote_config.heart_beat_timeout r.Remote_config.heart_beat_frequency 
        r.Remote_config.connection_backlog r.Remote_config.node_name r.Remote_config.node_ip

  let log_msg (ns : node_state) ~(level:I.level) ?exn (action : string) ?pid (details : string) : unit I.t =      
    let msg = 
      if pid = None then Format.sprintf "Node {%s} - Action {%s} Details {%s}" (Node_id.string_of_node ns.local_node) action details 
      else 
        Format.sprintf "Node {%s}|Process {%d} - Action {%s} Details {%s}" 
          (Node_id.string_of_node ns.local_node) (Potpourri.get_option pid) action details 
    in
    I.log ~logger:(ns.logger) ~level ?exn msg

  let safe_close_channel (ns : node_state) (ch : [`Out of I.output_channel | `In of I.input_channel]) 
      (action : string) (details : string) : unit I.t =
    let open I in
    catch 
      (fun () -> 
         match ch with
         | `Out out_ch -> close_output out_ch
         | `In in_ch -> close_input in_ch
      )  
      (fun e -> log_msg ns ~level:Warning ~exn:e action details)                      

  let return (v : 'a) : 'a t = 
    fun (ns,pid) -> I.return (ns,pid, v)

  let (>>=) (p : 'a t) (f : 'a -> 'b t) : 'b t = 
    fun (ns,pid) -> I.(p (ns,pid) >>= fun (ns',pid',v) -> (f v) (ns',pid'))

  let fail (e : exn) : 'a t =
    fun _ -> I.fail e        

  let catch (p:(unit -> 'a t)) (handler:(exn -> 'a t)) : 'a t =
    fun (ns,pid) ->        
      I.catch (fun () -> (p ()) (ns,pid)) (fun e -> (handler e) (ns,pid))   

  let lift_io (io_comp : 'a io) : 'a t =
    fun (ns,pid) -> I.(io_comp >>= fun res -> return (ns,pid,res))  

  let send_monitor_response (ns : node_state) (monitors : Monitor_ref_set.t option) (termination_reason : monitor_reason) : unit io =
    let open I in

    let send_monitor_response_local (Monitor_Ref (_,pid,_)) =
      match (Potpourri.of_option @@ fun () -> Hashtbl.find ns.mailboxes (Process_id.get_id pid)) with
      | None -> return ()
      | Some (_,push_fn) -> return @@ push_fn @@ Some (Exit (pid,termination_reason)) in

    let send_monitor_response_remote (Monitor_Ref (_,monitoring_process,monitored_process) as mref) =
      catch 
        (fun () ->
           match (Potpourri.of_option @@ fun () -> Node_id_hashtbl.find ns.remote_nodes (Process_id.get_node monitoring_process)) with
           | None -> 
             log_msg ns ~level:Info "sending remote monitor notification" 
               (Format.sprintf "monitor reference %s, remote node %s is down, skipping sending monitor message" 
                  (string_of_monitor_ref mref) (Node_id.string_of_node (Process_id.get_node monitoring_process)))
           | Some out_ch -> 
             write_value out_ch (Exit (monitored_process, termination_reason)) >>= fun () ->
             log_msg ns ~level:Info "sending remote monitor notification" 
               (Format.sprintf "sent monitor notification for monitor ref %s to remote node %s" 
                  (string_of_monitor_ref mref) (Node_id.string_of_node @@ Process_id.get_node monitoring_process))
        )
        (fun e -> 
           log_msg ns ~exn:e ~level:Error "sending remote monitor notification" 
             (Format.sprintf "monitor reference %s, error sending monitor message to remote node %s, marking node as down" 
                (string_of_monitor_ref mref) (Node_id.string_of_node @@ Process_id.get_node monitoring_process)) >>= fun () ->
           return @@ Node_id_hashtbl.remove ns.remote_nodes (Process_id.get_node monitoring_process)
        ) in           

    let iter_fn (Monitor_Ref (_,pid,_) as mref) _ =
      if Process_id.is_local pid ns.local_node
      then
        send_monitor_response_local mref >>= fun () ->
        log_msg ns ~level:Debug "sent local monitor notification" (string_of_monitor_notification mref termination_reason)                     
      else
        log_msg ns ~level:Debug "start sending remote monitor notification" 
          (Format.sprintf "monitor reference : %s" (string_of_monitor_notification mref termination_reason)) >>= fun () ->
        send_monitor_response_remote mref >>= fun () ->
        log_msg ns ~level:Debug "finished sending remote monitor notification" 
          (Format.sprintf "monitor reference : %s" (string_of_monitor_notification mref termination_reason)) in

    match monitors with
    | None -> return ()
    | Some monitors' -> Monitor_ref_set.fold iter_fn monitors' (return ())      

  let run_process' (ns : node_state) (pid : Process_id.t) (p : unit t) : unit io =
    let open I in
    catch 
      (fun () ->
         log_msg ns ~level:Notice "starting process" (Process_id.string_of_pid pid) >>= fun () ->
         p (ns,pid) >>= fun _ ->
         log_msg ns ~level:Notice "process terminated successfully" (Process_id.string_of_pid pid) >>= fun () ->
         send_monitor_response ns ((Potpourri.of_option @@ fun () -> Process_id_hashtbl.find ns.monitor_table pid)) (Normal pid) >>= fun () ->
         Process_id_hashtbl.remove ns.monitor_table pid ;
         return @@ Hashtbl.remove ns.mailboxes (Process_id.get_id pid) ;                       
      )         
      (fun e -> 
         log_msg ns ~exn:e ~level:Error "process failed with error" (Process_id.string_of_pid pid) >>= fun () ->
         Hashtbl.remove ns.mailboxes (Process_id.get_id pid) ;
         begin
           match e with
           | InvalidNode n -> 
             send_monitor_response 
               ns 
               ((Potpourri.of_option @@ fun () -> Process_id_hashtbl.find ns.monitor_table pid)) 
               (UnkownNodeId (pid,n))             
           | _ -> 
             send_monitor_response 
               ns 
               ((Potpourri.of_option @@ fun () -> Process_id_hashtbl.find ns.monitor_table pid)) 
               (Exception (pid,e))
         end >>= fun () ->
         return @@ Process_id_hashtbl.remove ns.monitor_table pid 
      )

  let sync_send pid ns ?flags out_ch msg_create_fn response_fn =
    let open I in
    let remote_config = Potpourri.get_option !(ns.config) in
    let new_pid = Process_id.make_remote remote_config.Remote_config.node_ip
        remote_config.Remote_config.local_port remote_config.Remote_config.node_name in
    let new_mailbox,push_fn = I.create_stream () in 
    Hashtbl.replace ns.mailboxes (Process_id.get_id new_pid) (new_mailbox,push_fn) ;
    let msg_to_send = msg_create_fn new_pid in
    log_msg ns ~pid ~level:Notice "sync send start" 
      (Format.sprintf "created new process %s for sync send of %s" (Process_id.string_of_pid new_pid) (string_of_message msg_to_send)) >>= fun () -> 
    write_value out_ch ?flags msg_to_send >>= fun () ->      
    get new_mailbox >>= fun result_pid ->      
    Hashtbl.remove ns.mailboxes (Process_id.get_id new_pid) ;
    log_msg ns ~pid ~level:Notice "sync send end" 
      (Format.sprintf "process %s finished for sync send of %s" (Process_id.string_of_pid new_pid) (string_of_message msg_to_send)) >>= fun () ->
    response_fn (Potpourri.get_option result_pid) (* we do not send None on mailboxes *)

  let monitor_helper (ns : node_state) (monitor_pid : Process_id.t) (monitee_pid : Process_id.t) : (message option * monitor_ref) =
    let new_monitor_ref = Monitor_Ref (Potpourri.post_incr ns.monitor_ref_id, monitor_pid, monitee_pid) in
    match (Potpourri.of_option @@ fun () -> Hashtbl.find ns.mailboxes (Process_id.get_id monitee_pid)) with
    | None -> (Some (Exit (monitee_pid, NoProcess monitee_pid)), new_monitor_ref)        
    | Some _ ->
      begin
        match (Potpourri.of_option @@ fun () -> Process_id_hashtbl.find ns.monitor_table monitee_pid) with
        | None -> Process_id_hashtbl.add ns.monitor_table monitee_pid (Monitor_ref_set.of_list [new_monitor_ref])              
        | Some curr_monitor_set -> Process_id_hashtbl.replace ns.monitor_table monitee_pid (Monitor_ref_set.add new_monitor_ref curr_monitor_set)
      end ; 
      (None, new_monitor_ref) 

  let monitor_response_handler (ns : node_state) (res : message option * monitor_ref) : monitor_ref =
    match res with
    | (Some msg, (Monitor_Ref (_,monitor_pid,_) as mref)) ->
      let _,push_fn = Hashtbl.find ns.mailboxes (Process_id.get_id monitor_pid) in (* process is currently running so mailbox must be present *) 
      push_fn @@ Some msg ;
      mref
    | (None, (Monitor_Ref (_, _, monitee_pid) as mref)) -> 
      begin
        match (Potpourri.of_option @@ fun () -> Process_id_hashtbl.find ns.monitor_table monitee_pid) with
        | None -> Process_id_hashtbl.add ns.monitor_table monitee_pid (Monitor_ref_set.of_list [mref])              
        | Some curr_monitor_set -> Process_id_hashtbl.replace ns.monitor_table monitee_pid (Monitor_ref_set.add mref curr_monitor_set)
      end ;
      mref          

  let monitor_local (ns : node_state) (monitor_pid : Process_id.t) (monitee_pid : Process_id.t) : monitor_ref =
    monitor_response_handler ns @@ monitor_helper ns monitor_pid monitee_pid

  let make_new_pid (node_to_spwan_on : Node_id.t) (ns : node_state) : Process_id.t =
    if !(ns.config) = None 
    then Process_id.make_local (Node_id.get_name node_to_spwan_on) 
    else
      let remote_config = Potpourri.get_option !(ns.config) in 
      Process_id.make_remote remote_config.Remote_config.node_ip remote_config.Remote_config.local_port remote_config.Remote_config.node_name   

  let spawn ?(monitor=false) (node_id : Node_id.t) (p : unit t) : (Process_id.t * monitor_ref option) t =
    let open I in 
    fun (ns,pid) ->
      if Node_id.is_local node_id ns.local_node
      then
        let new_pid = make_new_pid node_id ns in                      
        Hashtbl.replace ns.mailboxes (Process_id.get_id new_pid) (I.create_stream ()) ;
        if monitor
        then
          begin
            let monitor_res = monitor_local ns pid new_pid in
            async (fun () -> run_process' ns new_pid p) ;           
            log_msg ~pid:(Process_id.get_id pid) ns ~level:Debug "spawned and monitored local process" 
              ((Format.sprintf "result pid %s, result monitor reference : %s") (Process_id.string_of_pid new_pid) (string_of_monitor_ref monitor_res)) >>= fun () -> 
            return (ns,pid,(new_pid, Some monitor_res))
          end
        else 
          begin 
            async (fun () -> run_process' ns new_pid p) ;           
            log_msg ~pid:(Process_id.get_id pid) ns ~level:Debug "spawned local process" 
              (Format.sprintf "result pid %s" (Process_id.string_of_pid new_pid)) >>= fun () ->
            return (ns,pid,(new_pid, None))
          end            
      else
        match (Potpourri.of_option @@ fun () -> Node_id_hashtbl.find ns.remote_nodes node_id) with
        | Some out_ch ->
          if monitor
          then 
            begin
              log_msg ~pid:(Process_id.get_id pid) ns ~level:Debug "spawning and monitoring remote process" 
                (Format.sprintf "on remote node %s, local process %s" (Node_id.string_of_node node_id) (Process_id.string_of_pid pid)) >>= fun () ->
              sync_send (Process_id.get_id pid) ns ~flags:[Marshal.Closures] out_ch (fun receiver_pid -> (Spawn_monitor (p,pid,receiver_pid))) 
                (fun res ->
                   let Monitor_Ref (_,_,monitored_proc) as mref = match res with Spawn_monitor_result (_,mr,_) -> mr | _ -> assert false in
                   log_msg ~pid:(Process_id.get_id pid) ns ~level:Debug "spawned and monitored remote process" 
                     (Format.sprintf "spawned on remote node %s : result pid %s, result monitor reference : %s" 
                        (Node_id.string_of_node node_id) (Process_id.string_of_pid monitored_proc) (string_of_monitor_ref mref)) >>= fun () ->
                   return (ns,pid, (monitored_proc, Some mref))
                )
            end
          else 
            begin
              log_msg ~pid:(Process_id.get_id pid) ns ~level:Debug "spawning remote process" 
                (Format.sprintf "on remote node %s, local process %s" (Node_id.string_of_node node_id) (Process_id.string_of_pid pid)) >>= fun () ->
              sync_send (Process_id.get_id pid) ns ~flags:[Marshal.Closures] out_ch (fun receiver_pid -> (Proc (p,receiver_pid))) 
                (fun res ->
                   let remote_proc_pid = match res with Proc_result (r,_) -> r | _ -> assert false in 
                   log_msg ~pid:(Process_id.get_id pid) ns ~level:Debug "spawned remote process" 
                     (Format.sprintf "on remote node %s : result pid %s" 
                        (Node_id.string_of_node node_id) (Process_id.string_of_pid remote_proc_pid)) >>= fun () -> 
                   return (ns, pid, (remote_proc_pid, None))
                )                  
            end                                   
        | None -> 
          begin
            log_msg ~pid:(Process_id.get_id pid) ns ~level:Error "failed to spawn process on remote node" 
              (Format.sprintf "remote node %s, is unknown, local process %s " (Node_id.string_of_node node_id) (Process_id.string_of_pid pid)) >>= fun () ->
            fail @@ InvalidNode node_id              
          end 

  let case (match_fn:(message_type -> bool)) (match_handler:(message_type -> 'a t)) : 'a matcher =
    let matcher = function
      | Data (_,_,msg) -> match_fn msg
      | _ -> false in
    let match_handler' = function
      | Data (_,_,msg) -> match_handler msg
      | _ -> assert false in
    (matcher, match_handler')      

  let termination_case (handler_fn:(monitor_reason -> 'a t)) : 'a matcher = 
    let matcher' = function
      | Exit _ -> true
      | _ -> false in
    let match_handler' = function
      | Exit (_,reason) -> handler_fn reason
      | _ -> assert false in
    (matcher', match_handler')     

  let receive ?timeout_duration (matchers : 'a matcher list)  : 'a option t =
    let open I in
    let temp_stream,temp_push_fn = create_stream () in
    let result = ref None in

    let rec iter_fn match_fns candidate_msg =
      match match_fns with
      | [] -> (temp_push_fn (Some candidate_msg)) ; false
      | (matcher,handler)::xs -> 
        if matcher candidate_msg
        then (result := Some (handler candidate_msg) ; true)                       
        else iter_fn xs candidate_msg in

    let rec iter_stream iter_fn stream =
      get stream >>= fun v ->
      if iter_fn (Potpourri.get_option v) then return () else iter_stream iter_fn stream in (* a None is never sent, see send function below. *)

    let do_receive_blocking (ns,pid) =     
      let mailbox,_ = Hashtbl.find ns.mailboxes (Process_id.get_id pid) in 
      iter_stream (iter_fn matchers) mailbox >>= fun () ->
      let mailbox',old_push_fn = Hashtbl.find ns.mailboxes (Process_id.get_id pid) in
      old_push_fn None ; (* mark end of old stream so we can append new and old *)
      Hashtbl.replace ns.mailboxes (Process_id.get_id pid) (stream_append mailbox' temp_stream, temp_push_fn) ; 
      (Potpourri.get_option !result) (ns,pid) >>= fun (ns', pid', result') -> 
      return (ns', pid', Some result') in

    let restore_mailbox ns pid =
      let mailbox',old_push_fn = Hashtbl.find ns.mailboxes (Process_id.get_id pid) in
      old_push_fn None ; (* close old stream so we can append new and old *)
      Hashtbl.replace ns.mailboxes (Process_id.get_id pid) (stream_append mailbox' temp_stream, temp_push_fn) in  

    fun (ns,pid) ->
      if matchers = []
      then 
        begin
          log_msg ~pid:(Process_id.get_id pid) ns ~level:Error "receiving" 
            (Format.sprintf "receiver process %s, called with empty list of matchers" (Process_id.string_of_pid pid)) >>= fun () ->            
          fail Empty_matchers
        end
      else
        match timeout_duration with
        | None -> 
          catch 
            (fun () ->
               log_msg ~pid:(Process_id.get_id pid) ns ~level:Debug "receiving with no time out" 
                 (Format.sprintf "receiver process %s" (Process_id.string_of_pid pid)) >>= fun () ->              
               do_receive_blocking (ns,pid) >>= fun (ns',pid',res) ->
               log_msg ~pid:(Process_id.get_id pid) ns ~level:Debug "successfully received and processed message with no time out" 
                 (Format.sprintf "receiver process %s" (Process_id.string_of_pid pid)) >>= fun () ->
               return (ns',pid',res)
            )
            (fun e ->
               restore_mailbox ns pid ;
               log_msg ~pid:(Process_id.get_id pid) ns ~exn:e ~level:Error "receiving with no time out failed" 
                 (Format.sprintf "receiver process %s, encountred exception" (Process_id.string_of_pid pid)) >>= fun () ->
               fail e
            )          
        | Some timeout_duration' ->
          log_msg ~pid:(Process_id.get_id pid) ns ~level:Debug "receiving with time out" 
            (Format.sprintf "receiver process %s, time out %f" (Process_id.string_of_pid pid) timeout_duration') >>= fun () ->            
          catch 
            (fun () -> 
               pick [do_receive_blocking (ns,pid) ; timeout timeout_duration' ] >>= fun (ns',pid',res) ->
               log_msg ns ~pid:(Process_id.get_id pid) ~level:Debug "successfully received and processed a message with time out" 
                 (Format.sprintf "receiver process %s, time out %f" (Process_id.string_of_pid pid) timeout_duration') >>= fun () ->
               return (ns', pid', res)
            )
            (fun e ->
               restore_mailbox ns pid ;
               match e with 
               | Timeout -> 
                 begin
                   log_msg ~pid:(Process_id.get_id pid) ns ~level:Debug "receive timed out" 
                     (Format.sprintf "receiver process %s, time out %f" (Process_id.string_of_pid pid) timeout_duration') >>= fun () ->
                   return (ns,pid, None)
                 end
               | e ->
                 log_msg ~pid:(Process_id.get_id pid) ns ~exn:e ~level:Error "receiving with time out failed" 
                   (Format.sprintf "receiver process %s, time out %f" (Process_id.string_of_pid pid) timeout_duration') >>= fun () ->
                 fail e
            ) 

  let rec receive_loop ?timeout_duration (matchers : bool matcher list) : unit t =
    let open I in
    fun (ns,pid) ->
      (receive ?timeout_duration matchers) (ns,pid) >>= fun (ns',pid',res) ->
      match res with
      | None | Some false -> return (ns',pid',())
      | Some true -> (receive_loop ?timeout_duration matchers) (ns',pid')                            

  let send_to_remote_node_helper 
      (pid : int) (ns : node_state) (node : Node_id.t) (sending_log_action : string) (sending_log_msg : string) 
      (unknown_node_msg : string) (msg : message) : unit I.t =
    let open I in
    match (Potpourri.of_option @@ fun () -> Node_id_hashtbl.find ns.remote_nodes node) with
    | Some remote_output ->   
      log_msg ns ~pid ~level:Debug sending_log_action sending_log_msg >>= fun () ->        
      write_value ~flags:[Marshal.Closures] remote_output msg (* marshal because the message could be a function *)
    | None -> 
      begin
        log_msg ns ~pid ~level:Error sending_log_action unknown_node_msg >>= fun () ->
        fail @@ InvalidNode node
      end            

  let send (remote_pid : Process_id.t) (msg : message_type) : unit t =
    let open I in
    fun (ns,pid) ->  
      if Process_id.is_local remote_pid ns.local_node
      then
        match (Potpourri.of_option @@ fun () -> Hashtbl.find ns.mailboxes (Process_id.get_id remote_pid)) with
        | None ->
          log_msg ns ~pid:(Process_id.get_id pid) ~level:I.Warning "unable to send message to local process" 
            (Format.sprintf "message : %s, to unknown local process: %s, from local process: %s" 
               (M.string_of_message msg) (Process_id.string_of_pid remote_pid) (Process_id.string_of_pid pid)) >>= fun () ->
          return (ns, pid, ())
        | Some (_,push_fn) ->
          log_msg ns ~pid:(Process_id.get_id pid) ~level:I.Debug "successfully sent message to local process" 
            (Format.sprintf "message : %s, to local process: %s, from local process: %s" 
               (M.string_of_message msg) (Process_id.string_of_pid remote_pid) (Process_id.string_of_pid pid)) >>= fun () ->
          return @@ (ns, pid, push_fn @@ Some (Data (pid,remote_pid,msg)))
      else          
        let sending_msg = Format.sprintf "message : %s, to remote process: %s, from local process: %s" 
            (M.string_of_message msg) (Process_id.string_of_pid remote_pid) (Process_id.string_of_pid pid) in
        let unknown_node_msg = Format.sprintf "message : %s, to unknown remote process: %s, from local process: %s" 
            (M.string_of_message msg) (Process_id.string_of_pid remote_pid) (Process_id.string_of_pid pid) in              
        send_to_remote_node_helper 
          (Process_id.get_id pid) ns (Process_id.get_node remote_pid) 
          "sending message to remote process" sending_msg unknown_node_msg (Data (pid,remote_pid,msg)) >>= fun () ->          
        log_msg ns ~pid:(Process_id.get_id pid) ~level:I.Debug "successfully sent message to remote process" 
          (Format.sprintf "message : %s, to remote process: %s, from local process: %s" 
             (M.string_of_message msg) (Process_id.string_of_pid remote_pid) (Process_id.string_of_pid pid)) >>= fun () ->
        return (ns,pid,()) 

  let (>!) (pid : Process_id.t) (msg : message_type) : unit t =
    send pid msg                       

  let broadcast_local ?pid (ns : node_state) (sending_pid : Process_id.t) (m : message_type) : unit io =
    let open I in      
    Hashtbl.fold 
      (fun recev_pid (_,push_fn) _ ->
         let recev_pid' = Process_id.make ns.local_node recev_pid in
         if recev_pid' = sending_pid
         then return ()
         else
           log_msg ?pid ns ~level:I.Debug "broadcast" 
             (Format.sprintf "sending message %s to local process %s from process %s as result of broadcast request" 
                (M.string_of_message m) (Process_id.string_of_pid recev_pid') (Process_id.string_of_pid sending_pid)) >>= fun () ->             
           return @@ push_fn @@ Some (Data (sending_pid,recev_pid',m))
      ) 
      ns.mailboxes
      (return ())          

  let broadcast (node : Node_id.t) (m : message_type) : unit t =
    let open I in
    fun (ns,pid) ->
      if Node_id.is_local node ns.local_node
      then
        begin
          log_msg ~pid:(Process_id.get_id pid) ns ~level:I.Debug "broadcast" 
            (Format.sprintf "sending broadcast message %s to local processes running on local node %s from local process %s" 
               (M.string_of_message m) (Node_id.string_of_node node) (Process_id.string_of_pid pid)) >>= fun () ->
          broadcast_local ns pid m >>= fun () ->
          return (ns,pid,())
        end        
      else
        let sending_msg = Format.sprintf "Process %s is sending broadcast message %s to remote node %s" 
            (Process_id.string_of_pid pid) (M.string_of_message m) (Node_id.string_of_node node) in
        let unknwon_node_msg = Format.sprintf "Process %s failed to send broadcast message %s to remote node %s, remote node is unknown" 
            (Process_id.string_of_pid pid) (M.string_of_message m) (Node_id.string_of_node node) in
        send_to_remote_node_helper 
          (Process_id.get_id pid) ns node "broadcasting to remote node" sending_msg unknwon_node_msg (Broadcast (pid,node,m)) >>= fun () ->
        log_msg ns ~pid:(Process_id.get_id pid) ~level:I.Debug "successfully sent broadcast message to remote node" 
          (Format.sprintf "message : %s, to remote node: %s" (M.string_of_message m) (Node_id.string_of_node node))  >>= fun () ->
        return (ns,pid,())

  let lookup_node_and_send (pid:int) (ns : node_state) (receiver_process : Process_id.t) (action : string) (unknown_node_msg : string) (node_found_fn : I.output_channel -> 'a I.t) : 'a I.t =
    let open I in
    match (Potpourri.of_option @@ fun () -> Node_id_hashtbl.find ns.remote_nodes (Process_id.get_node @@ receiver_process)) with
    | None -> 
      begin
        log_msg ~pid ns ~level:Error action unknown_node_msg >>= fun () ->
        fail @@ InvalidNode (Process_id.get_node receiver_process)
      end 
    | Some out_ch ->
      node_found_fn out_ch    

  let monitor (pid_to_monitor : Process_id.t) : monitor_ref t =
    fun (ns,pid) ->
      let open I in
      if Process_id.is_local pid_to_monitor ns.local_node
      then
        begin
          log_msg ~pid:(Process_id.get_id pid) ns ~level:Debug "monitored" 
            (Format.sprintf "Creating monitor for local process %s to be monitored by local process %s" 
               (Process_id.string_of_pid pid_to_monitor) (Process_id.string_of_pid pid)) >>= fun () ->
          return (ns,pid, monitor_local ns pid pid_to_monitor)
        end
      else
        begin
          log_msg ~pid:(Process_id.get_id pid) ns ~level:Debug "monitoring" 
            (Format.sprintf "Creating monitor for remote process %s to be monitored by local process %s" 
               (Process_id.string_of_pid pid_to_monitor) (Process_id.string_of_pid pid)) >>= fun () ->
          let unknown_mode_msg = Format.sprintf "Process %s failed to monitor remote process %s on remote node %s, remote node is unknown" 
              (Process_id.string_of_pid pid) (Process_id.string_of_pid pid_to_monitor) (Node_id.string_of_node @@ Process_id.get_node pid_to_monitor) in
          let node_found_fn out_ch = 
            sync_send (Process_id.get_id pid) ns out_ch (fun receiver_pid -> (Monitor (pid, pid_to_monitor,receiver_pid))) 
              (fun res ->
                 let res' = match res with Monitor_result (mon_msg,mon_res,_) -> (mon_msg,mon_res) | _ -> assert false in
                 log_msg ~pid:(Process_id.get_id pid) ns ~level:I.Debug "successfully monitored remote process" 
                   (Format.sprintf "result: %s" (string_of_message res)) >>= fun () -> 
                 return (ns, pid, monitor_response_handler ns res')
              ) in
          lookup_node_and_send (Process_id.get_id pid) ns pid_to_monitor "monitoring" unknown_mode_msg node_found_fn    
        end

  let unmonitor_local (ns : node_state) (Monitor_Ref (_,_, process_to_unmonitor) as mref) : unit =
    match (Potpourri.of_option @@ fun () -> Process_id_hashtbl.find ns.monitor_table process_to_unmonitor) with
    | None -> ()
    | Some curr_set ->
      let curr_set' = Monitor_ref_set.remove mref curr_set in
      if Monitor_ref_set.is_empty curr_set'
      then Process_id_hashtbl.remove ns.monitor_table process_to_unmonitor
      else Process_id_hashtbl.replace ns.monitor_table process_to_unmonitor curr_set'            

  let unmonitor (Monitor_Ref (_,_,process_to_unmonitor) as mref) : unit t =
    let open I in
    fun (ns,pid) ->
      if Process_id.is_local process_to_unmonitor ns.local_node
      then
        begin
          log_msg ns ~pid:(Process_id.get_id pid) ~level:Debug "unmonitored" (Format.sprintf "Unmonitor local : %s" @@ string_of_monitor_ref mref) >>= fun () -> 
          return (ns, pid, unmonitor_local ns mref)
        end          
      else
        begin
          log_msg ns ~pid:(Process_id.get_id pid) ~level:Debug "unmonitoring" (Format.sprintf "Unmonitor remote : %s" @@ string_of_monitor_ref mref) >>= fun () -> 
          let unknown_node_msg = Format.sprintf "Process %s failed to monitor remote process %s on remote node %s, remote node is unknown" 
              (Process_id.string_of_pid pid) (Process_id.string_of_pid process_to_unmonitor) (Node_id.string_of_node @@ Process_id.get_node process_to_unmonitor) in
          let node_found_fn out_ch = 
            sync_send (Process_id.get_id pid) ns out_ch 
              (fun recv_pid -> (Unmonitor (mref,recv_pid))) 
              (fun _ -> 
                 log_msg ~pid:(Process_id.get_id pid) ns ~level:Debug "successfully unmonitored" (Format.sprintf "monitor ref : %s" @@ string_of_monitor_ref mref) >>= fun () ->  
                 return (ns, pid, ())
              ) in
          lookup_node_and_send (Process_id.get_id pid) ns process_to_unmonitor "unmonitoring" unknown_node_msg node_found_fn              
        end

  let get_self_pid : Process_id.t t =
    fun (ns,proc_id) -> I.return(ns,proc_id, proc_id) 

  let get_self_node : Node_id.t t = 
    fun (ns,pid) -> 
      I.return(ns,pid, Process_id.get_node pid)

  let get_remote_node node_name =
    fun (ns,pid) ->
      let res : Node_id.t option ref = ref None in
      let iter_fn node _ =
        if (Node_id.get_name node = node_name) && !res = None
        then res := Some node           
        else () in
      Node_id_hashtbl.iter iter_fn ns.remote_nodes ;  
      I.return(ns,pid, !res)    

  let get_remote_nodes : Node_id.t list t =
    fun (ns,pid) ->
      let res : Node_id.t list = Node_id_hashtbl.fold (fun n _ acc -> n::acc) ns.remote_nodes [] in      
      I.return (ns,pid,res)

  let node_server_fn (ns : node_state) ((in_ch,out_ch) : I.input_channel * I.output_channel) : unit =
    let open I in
    let remote_config = Potpourri.get_option !(ns.config) in
    let node = ref None in    

    let clean_up_fn () =
      begin 
        async (
          fun () ->           
            let out_details = 
              if !node <> None 
              then Format.sprintf "encountered error while closing output channel for remote node %s" (Node_id.string_of_node (Potpourri.get_option !node))  
              else "encountered error while closing output channel" in
            let in_details = 
              if !node <> None 
              then Format.sprintf "encountered error while closing input channel for remote node %s" (Node_id.string_of_node (Potpourri.get_option !node)) 
              else "encountered error while closing input channel" in
            safe_close_channel ns (`In in_ch) "node clean up" out_details >>= fun () ->
            safe_close_channel ns (`Out out_ch) "node clean up" in_details 
        ) ;
        if !node = None then () else Node_id_hashtbl.remove ns.remote_nodes (Potpourri.get_option !node)                
      end in

    let spawn_preamble () =        
      let new_pid = Process_id.make_remote remote_config.Remote_config.node_ip remote_config.Remote_config.local_port 
          remote_config.Remote_config.node_name in
      Hashtbl.replace ns.mailboxes (Process_id.get_id new_pid) (I.create_stream ()) ;        
      new_pid in 

    let put_in_mailbox receiver_pid msg =
      match (Potpourri.of_option @@ fun () -> Hashtbl.find ns.mailboxes (Process_id.get_id receiver_pid)) with
      | None -> 
        begin            
          let receiver_not_found_err_msg = Format.sprintf "remote node %s, processed message %s, recipient unknown local process %s" 
              (Node_id.string_of_node @@ Potpourri.get_option !node) (string_of_message msg) (Process_id.string_of_pid receiver_pid) in            
          log_msg ns ~level:I.Warning "node process message" receiver_not_found_err_msg 
        end           
      | Some (_,push_fn) ->
        return @@ push_fn (Some msg) in       

    let rec handler () = 
      if (Potpourri.of_option @@ fun () -> Node_id_hashtbl.find ns.remote_nodes (Potpourri.get_option !node)) = None
      then
        let node_str = Node_id.string_of_node (Potpourri.get_option !node) in 
        log_msg ns ~level:Error "node process message" 
          (Format.sprintf "previously encountered errors when communicating with remote node %s, stopping handler for remote node %s" node_str node_str)
      else 
        catch
          (fun () ->
             read_value in_ch >>= fun (msg:message) -> 
             log_msg ns ~level:Debug "node process message" 
               (Format.sprintf "remote node %s, message %s" (Node_id.string_of_node @@ Potpourri.get_option !node) (string_of_message msg)) >>= fun () ->
             match msg with
             | Node _ -> 
               handler ()            
             | Heartbeat ->
               Node_id_hashtbl.replace ns.remote_nodes_heart_beats (Potpourri.get_option !node) true ;
               handler ()
             | Proc (p,sender_pid) ->
               begin
                 let result_pid = spawn_preamble () in                   
                 write_value out_ch (Proc_result (result_pid,sender_pid)) >>= fun () ->
                 async (fun () -> run_process' ns result_pid p) ;                                      
                 handler ()
               end  
             | Spawn_monitor (p, monitor_pid,sender) ->
               begin
                 let new_pid = spawn_preamble () in
                 let (monitor_msg,monitor_res) = monitor_helper ns monitor_pid new_pid in
                 write_value out_ch (Spawn_monitor_result (monitor_msg,monitor_res,sender)) >>= fun () ->
                 async (fun () -> run_process' ns new_pid p) ;                                      
                 handler ()
               end                                    
             | Monitor (monitor_pid, to_be_monitored,sender) ->
               begin
                 let (mon_msg,mon_res) = monitor_helper ns monitor_pid to_be_monitored in
                 write_value out_ch (Monitor_result (mon_msg,mon_res,sender)) >>= fun () ->
                 handler ()
               end 
             | Unmonitor (mref,sender) ->
               begin
                 unmonitor_local ns mref ; 
                 write_value out_ch (Unmonitor_result (mref,sender)) >>= fun () ->
                 handler ()
               end   
             | Broadcast (sender_pid,_,msg) ->
               begin
                 broadcast_local ns sender_pid msg >>= fun () ->
                 handler ()
               end
             | Data (_,r,_) as data ->
               begin
                 put_in_mailbox r data >>= fun () ->
                 handler ()
               end
             | Exit (s,m) ->
               begin
                 match (Potpourri.of_option @@ fun () -> Process_id_hashtbl.find ns.monitor_table s) with
                 | None -> 
                   begin
                     log_msg ns ~level:Error "node process message" 
                       (Format.sprintf "no entry for %s in monitor table when processing %s" (Process_id.string_of_pid s) (string_of_message msg)) >>= fun () -> 
                     handler ()
                   end                                                               
                 | Some pids ->
                   begin 
                     Monitor_ref_set.fold (fun (Monitor_Ref (_,pid,_)) _ -> put_in_mailbox pid (Exit (s,m))) pids (return ()) >>= fun () ->
                     handler ()
                   end                   
               end
             | Proc_result (_,receiver_pid) as pres ->
               begin
                 put_in_mailbox receiver_pid pres >>= fun () ->
                 handler ()
               end
             | Spawn_monitor_result (monitor_msg,mref,receiver) as sres ->
               begin
                 ignore (monitor_response_handler ns (monitor_msg,mref)) ;
                 put_in_mailbox receiver sres >>= fun () ->
                 handler ()
               end
             | Monitor_result (mon_msg,mref,receiver) as mres ->
               begin
                 ignore (monitor_response_handler ns (mon_msg,mref)) ;
                 put_in_mailbox receiver mres >>= fun () ->
                 handler ()
               end
             | Unmonitor_result (mref,receiver_pid) as unmonres ->
               begin
                 unmonitor_local ns mref ;
                 put_in_mailbox receiver_pid unmonres >>= fun () ->
                 handler ()
               end) 
          (function e -> 
            clean_up_fn () ; 
            log_msg ns ~exn:e ~level:Error "node process message" @@ 
            Format.sprintf "unexpected exception while processing messages for remote node %s"  
              (Node_id.string_of_node @@ Potpourri.get_option !node)
          ) in

    let rec wait_for_node_msg () =
      catch
        (fun () ->
           read_value in_ch >>= fun (msg:message) -> 
           log_msg ns ~level:Debug "node process message" (Format.sprintf "received message %s" (string_of_message msg)) >>= fun () ->
           match msg with
           | Node node' -> 
             begin
               node := Some node' ;
               Node_id_hashtbl.replace ns.remote_nodes node' out_ch ;
               write_value out_ch (Node ns.local_node) >>= fun () ->                         
               handler ()
             end                  
           | _ ->
             begin
               log_msg ns ~level:Debug "node process message" (Format.sprintf "ignore message %s, waiting for handshake" (string_of_message msg)) >>= fun () ->
               wait_for_node_msg ()                 
             end
        )
        (function e -> clean_up_fn () ; log_msg ns ~exn:e ~level:Error "node process message" "unexpected exception") in

    async wait_for_node_msg 

  let connect_to_remote_nodes_unsafe ?pid (ns : node_state) (remote_node : Node_id.t) (ip : string) (port : int) (name : string) (remote_sock_addr : Unix.sockaddr) : unit I.t =  
    let open I in
    log_msg ns ?pid ~level:Notice "connecting to remote node" (Format.sprintf "remote node %s:%d, name %s" ip port name) >>= fun () -> 
    open_connection remote_sock_addr >>= fun (in_ch,out_ch) ->
    write_value out_ch @@ Node (ns.local_node) >>= fun () ->               
    Node_id_hashtbl.replace ns.remote_nodes remote_node out_ch ;
    Node_id_hashtbl.replace ns.remote_nodes_heart_beats remote_node false ;
    node_server_fn ns (in_ch,out_ch) ;
    log_msg ns ~level:Notice "connected to remote node" (Format.sprintf "remote node %s:%d, name %s" ip port name) 

  let add_remote_node (ip : string) (port : int) (name : string) : Node_id.t t =
    let open I in
    fun (ns,pid) ->
      if !(ns.config) = None
      then
        log_msg ~pid:(Process_id.get_id pid) ns ~level:Error "add remote node" 
          "called add remote node when node is running with local only configuration" >>= fun () ->  
        fail Local_only_mode        
      else
        log_msg ~pid:(Process_id.get_id pid) ns ~level:Debug "adding remote node" (Format.sprintf "%s:%d, name %s" ip port name) >>= fun () ->
        let remote_sock_addr = Unix.ADDR_INET (Unix.inet_addr_of_string ip,port) in
        let remote_node = Node_id.make_remote_node ip port name in
        if Node_id_hashtbl.mem ns.remote_nodes remote_node
        then  
          log_msg ~pid:(Process_id.get_id pid) ns ~level:Warning "remote node already exists" (Format.sprintf "%s:%d, name %s" ip port name) >>= fun () ->
          return (ns, pid, remote_node)
        else 
          connect_to_remote_nodes_unsafe ns remote_node ip port name remote_sock_addr >>= fun () ->
          return (ns, pid, remote_node)

  let remove_remote_node  (node : Node_id.t) : unit t =
    let open I in
    fun (ns,pid) ->
      if !(ns.config) = None
      then
        log_msg ~pid:(Process_id.get_id pid) ns ~level:Error "remote remote node" 
          "called remove remote node when node is running with local only configuration" >>= fun () ->  
        fail Local_only_mode
      else
        log_msg ~pid:(Process_id.get_id pid) ns ~level:Debug "removing remote node" (Format.sprintf "remote node : %s" @@ Node_id.string_of_node node) >>= fun () ->
        Node_id_hashtbl.remove ns.remote_nodes node ;
        Node_id_hashtbl.remove ns.remote_nodes_heart_beats node ;
        return (ns,pid, ())                

  let rec connect_to_remote_nodes (ns : node_state) (nodes : (string * int * string) list) : unit io =
    let open I in      
    match nodes with
    | [] -> return ()
    | (ip,port,name)::rest ->
      let remote_sock_addr = Unix.ADDR_INET (Unix.inet_addr_of_string ip,port) in
      let remote_node = Node_id.make_remote_node ip port name in
      catch 
        (fun () ->
           connect_to_remote_nodes_unsafe ns remote_node ip port name remote_sock_addr >>= fun () ->
           connect_to_remote_nodes ns rest
        )
        (fun e ->
           log_msg ns ~exn:e ~level:Error "connecting to remote nodes" (Format.sprintf "unable to connect to remote node %s:%d" ip port) >>= fun () -> 
           connect_to_remote_nodes ns rest
        )

  let rec send_heart_beats_fn (ns : node_state) (heart_beat_freq : float) : unit I.t =
    let open I in
    let safe_send node out_ch () =
      catch 
        (fun () ->
           log_msg ns ~level:Debug "sending heartbeat" (Format.sprintf "to node %s" @@ Node_id.string_of_node node) >>= fun () ->
           write_value out_ch Heartbeat
        )
        (fun e -> 
           log_msg ns ~exn:e ~level:Error "sending heartbeat" (Format.sprintf "failed for node %s" @@ Node_id.string_of_node node) >>= fun () ->
           safe_close_channel ns (`Out out_ch) "sending heartbeat" 
             (Format.sprintf "encountered error while closing output channel for remote node %s after sending heart beat failed" 
              @@ Node_id.string_of_node node) >>= fun () ->
           return @@ Node_id_hashtbl.remove ns.remote_nodes node                
        ) 
    in
    sleep heart_beat_freq >>= fun () ->
    Node_id_hashtbl.iter (fun node out_ch -> async @@ safe_send node out_ch) ns.remote_nodes ;
    send_heart_beats_fn ns heart_beat_freq

  let rec process_remote_heart_beats_timeout_fn (ns : node_state) (heat_beat_timeout : float) : unit I.t =
    let open I in
    sleep heat_beat_timeout >>= fun () ->
    Node_id_hashtbl.iter 
      (fun node recvd_hear_beat ->
         if (not recvd_hear_beat) 
         then 
           match (Potpourri.of_option @@ fun () -> Node_id_hashtbl.find ns.remote_nodes node) with
           | None -> async @@ fun () -> return ()
           | Some out_ch -> 
             begin 
               Node_id_hashtbl.remove ns.remote_nodes node ; 
               async 
                 (fun () ->
                    safe_close_channel ns (`Out out_ch) "node heartbeat process" 
                      (Format.sprintf "encountered error while closing output channel for remote node %s after heat beat not received in time" 
                       @@ Node_id.string_of_node node)                    
                 ) ;
               async 
                 (fun () ->
                    match ns.node_mon_fn with
                    | None -> return ()
                    | Some f ->
                      catch 
                        (fun () -> (f node) (ns,make_new_pid ns.local_node ns) >>= fun _ -> return ())
                        (fun e -> log_msg ns ~exn:e ~level:Error "node heartbeat process" 
                            (Format.sprintf "encountered error while running node monitor function for remote node %s after heat beat not received in time" 
                             @@ Node_id.string_of_node node))                    
                 ) 
             end 
         else ()
      ) 
      ns.remote_nodes_heart_beats ;
    Node_id_hashtbl.clear ns.remote_nodes_heart_beats ;    
    process_remote_heart_beats_timeout_fn ns heat_beat_timeout  

  let run_node ?process ?node_monitor_fn (node_config : node_config) : unit io =
    let open I in
    if !initalised
    then fail Init_more_than_once
    else
      begin
        initalised := true ;
        match node_config with
        | Local local_config ->
          let ns = { mailboxes                = Hashtbl.create 1000 ; 
                     remote_nodes             = Node_id_hashtbl.create ~random:true 10 ;
                     remote_nodes_heart_beats = Node_id_hashtbl.create ~random:true 10 ;
                     monitor_table            = Process_id_hashtbl.create ~random:true 1000 ;
                     local_node               = Node_id.make_local_node local_config.Local_config.node_name ; 
                     logger                   = local_config.Local_config.logger ;
                     monitor_ref_id           = ref 0 ;
                     config                   = ref None ;
                     node_mon_fn              = node_monitor_fn ;
                   } in
          log_msg ns ~level:Info "node start up" 
            (Format.sprintf "{Distributed library version : %s ; Threading implementation : [name : %s ; version : %s ; description : %s]}" 
               dist_lib_version lib_name lib_version lib_description) >>= fun () ->
          log_msg ns ~level:Info "node start up" (Format.sprintf "local only mode with configuration of %s" @@ string_of_config node_config) >>= fun () ->
          if process = None
          then return ()
          else 
            begin
              let new_pid = Process_id.make_local local_config.Local_config.node_name in
              Hashtbl.replace ns.mailboxes (Process_id.get_id new_pid) (I.create_stream ()) ; 
              run_process' ns new_pid (Potpourri.get_option process)
            end 
        | Remote remote_config ->
          let ns = { mailboxes                = Hashtbl.create 1000 ; 
                     remote_nodes             = Node_id_hashtbl.create ~random:true 10 ;
                     remote_nodes_heart_beats = Node_id_hashtbl.create ~random:true 10 ;
                     monitor_table            = Process_id_hashtbl.create ~random:true 1000 ;
                     local_node               = Node_id.make_remote_node remote_config.Remote_config.node_ip remote_config.Remote_config.local_port remote_config.Remote_config.node_name ; 
                     logger                   = remote_config.Remote_config.logger ;
                     monitor_ref_id           = ref 0 ;
                     config                   = ref (Some remote_config) ;
                     node_mon_fn              = node_monitor_fn ;
                   } in
          log_msg ns ~level:Info "node start up" 
            (Format.sprintf "{Distributed library version : %s ; Threading implementation : [name : %s ; version : %s ; description : %s]}" 
               dist_lib_version lib_name lib_version lib_description) >>= fun () ->
          log_msg ns ~level:Info "node start up" (Format.sprintf "remote mode with configuration of %s" @@ string_of_config node_config) >>= fun () ->
          connect_to_remote_nodes ns remote_config.Remote_config.remote_nodes >>= fun () ->
          let local_sock_addr =  Unix.ADDR_INET (Unix.inet6_addr_any , remote_config.Remote_config.local_port) in
          let command_process_server = I.establish_server ~backlog:remote_config.Remote_config.connection_backlog local_sock_addr (node_server_fn ns) in
          async (fun () -> send_heart_beats_fn ns remote_config.Remote_config.heart_beat_frequency) ;
          async (fun () -> process_remote_heart_beats_timeout_fn ns remote_config.Remote_config.heart_beat_timeout) ; 
          at_exit (
            fun () ->
              log_msg ns ~level:Info "node shutting down" (Format.sprintf "start clean up actions for remote mode with configuration of %s" @@ string_of_config node_config) >>= fun () ->
              shutdown_server command_process_server ;
              Node_id_hashtbl.fold (fun _ out_ch _ -> I.close_output out_ch) ns.remote_nodes (return ()) >>= fun () ->
              log_msg ns ~level:Info "node shutting down" (Format.sprintf "finished clean up actions for remote mode with configuration of %s" @@ string_of_config node_config)                                                                                  
          );   
          if process = None
          then return ()
          else 
            begin
              let new_pid = Process_id.make_remote remote_config.Remote_config.node_ip remote_config.Remote_config.local_port remote_config.Remote_config.node_name in
              Hashtbl.replace ns.mailboxes (Process_id.get_id new_pid) (I.create_stream ()) ; 
              run_process' ns new_pid (Potpourri.get_option process)
            end             
      end                              
end    


