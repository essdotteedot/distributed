.PHONY : build clean test doc test_all_ocaml examples

build :	
	jbuilder build --dev

lwt_test : clean
	jbuilder runtest
	bisect-ppx-report -I _build/default/ -html _coverage/ `find . -name 'bisect*.out'`	

uwt_test : clean
	jbuilder build @runtest-uwt	

clean :
	jbuilder clean
	rm -f `find . -name 'bisect*.out'`
	rm -rf _coverage/		

doc :
	jbuilder build @doc

test_all_ocaml_lwt : clean
	jbuilder runtest --workspace jbuild-workspace.dev -j 1

test_all_ocaml_uwt : clean
	jbuilder build @runtest-uwt --workspace jbuild-workspace.dev -j 1

examples:
	jbuilder build examples/basic_example/basic.exe
	jbuilder build examples/name_server_example/add_client.exe
	jbuilder build examples/name_server_example/add_server.exe
	jbuilder build examples/name_server_example/name_server.exe
	jbuilder build examples/ping_pong_example/ping.exe
	jbuilder build examples/ping_pong_example/pong.exe
