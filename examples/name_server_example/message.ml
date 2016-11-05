type t = Register of string * Distributed.Process_id.t
       | Register_ok
       | Whois of string * Distributed.Process_id.t
       | Whois_result of Distributed.Process_id.t
       | Add of int * int * Distributed.Process_id.t
       | Add_result of int

let string_of_message = function
  | Register (s,_) -> Format.sprintf "Register %s" s
  | Register_ok -> "Register O.K."  
  | Whois (s,_) -> Format.sprintf "Who is %s" s
  | Whois_result _ -> "Who is result <pid>"
  | Add (i,j,_) -> Format.sprintf "Add %d %d" i j
  | Add_result r -> Format.sprintf "Add result %d" r       