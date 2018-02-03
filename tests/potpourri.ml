(*BISECT-IGNORE-BEGIN*)
let get_option (v : 'a option) : 'a = 
  match v with
  | None -> assert false (*BISECT-IGNORE*)
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

let pp_list ?first ?last ?sep (items : 'a list) (string_of_item : 'a -> string) (formatter) : unit =
  if first <> None then Format.fprintf formatter (get_option first) else () ;        
  List.iter 
    (fun i ->
       let s = string_of_item i in 
       Format.fprintf formatter "%s" s ;
       if sep <> None then Format.fprintf formatter (get_option sep) else ()
    ) 
    items ;    
  if last <> None then Format.fprintf formatter (get_option last) else () ;  

(*BISECT-IGNORE-END*)