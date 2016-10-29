let get_option (v : 'a option) : 'a = 
  match v with
  | None -> assert false
  | Some v' -> v'

let map_default_option (f : ('a -> 'b)) (default_value : 'b) (opt : 'a option) : 'b =
  match opt with
  | Some v -> f v
  | None -> default_value  

let post_incr (v : int ref) : int =
  let res = !v in
  v := !v + 1 ;
  res  

let of_option (f : unit -> 'a) : 'a option =
  try
    Some (f ())
  with _ -> None

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