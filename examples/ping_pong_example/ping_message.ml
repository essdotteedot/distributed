type t = Ping of string
       | Pong of string

let string_of_message = function
  | Ping s -> Format.sprintf "Ping %s" s
  | Pong s -> Format.sprintf "Pong %s" s