.PHONY : build clean test doc

build :	
	jbuilder build --dev

test : clean
	jbuilder runtest

coverage : test
	bisect-ppx-report -I _build/default/ -html _coverage/ `find . -name 'bisect*.out'`	

clean :
	jbuilder clean
	rm -f `find . -name 'bisect*.out'`
	rm -rf _coverage/		

doc :
	jbuilder build @doc

test_all_ocaml : clean
	jbuilder runtest --workspace jbuild-workspace.dev 
