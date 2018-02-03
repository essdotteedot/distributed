 (** A lwt based implementation of {!module:Distributed}. Note, that this lwt based implmentation depends on the [Logs_lwt] library.
     In keeping with the usage conventions of the [Logs] library, this implementation does not define a source, it does not set the
     log level, and it does not define a reporter. The application is expected to define a source, set the log level, and define
     a reporter (see the examples).

    @author essdotteedot <essdotteedot_at_gmail_dot_com>
    @version %%VERSION_NUM%%
 *)

(** This module provides a Log_lwt based logger to use. *)
module type CustomerLogger = sig

  val msg : Logs.level -> 'a Logs_lwt.log
  (** [msg level logger] returns a logger that can be use to log levels at the provided level. The returned thread only proceeds once the log operation is over. *)
end

 module Make (M : Distributed.Message_type) (L : CustomerLogger) : (Distributed.Process with type 'a io = 'a Lwt.t and type message_type = M.t)
 (** Functor to create a module of type {!module:Distributed.Process} given a message module [M] of type {!module:Distributed.Message_type}
     and a custom logger module [L] of type {!module:CustomerLogger}. *)