.PHONY : build clean lwt_test uwt_test doc test_all_ocaml_lwt test_all_ocaml_uwt examples

build :	
	dune build --workspace dune-workspace.dev

lwt_test : clean
	dune runtest
	bisect-ppx-report -I _build/default/ -html _coverage/ `find . -name 'bisect*.out'`	

uwt_test : clean
	dune build @runtest-uwt	

clean :
	dune clean
	rm -f `find . -name 'bisect*.out'`
	rm -rf _coverage/		

doc :
	dune build @doc

test_all_ocaml_lwt : clean
	dune runtest --workspace dune-workspace.dev -j 1

test_all_ocaml_uwt : clean
	dune build @runtest-uwt --workspace dune-workspace.dev -j 1

examples:
	dune build examples/basic_example/basic.exe
	dune build examples/name_server_example/add_client.exe
	dune build examples/name_server_example/add_server.exe
	dune build examples/name_server_example/name_server.exe
	dune build examples/ping_pong_example/ping.exe
	dune build examples/ping_pong_example/pong.exe
