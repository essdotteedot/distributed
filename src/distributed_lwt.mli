 (** A lwt based implementation of {!module:Distributed}.

    @author essdotteedot [<essdotteedot[at]gmail[dot]com>]
    @version 0.4.0
 *)

 module Make (M : Distributed.Message_type) : (Distributed.Process with type 'a io = 'a Lwt.t and type message_type = M.t and type logger = Lwt_log.logger)
 (** Functor to create a module of type {!module:Distributed.Process} given a message module [M] of type {!module:Distributed.Message_type}. *)