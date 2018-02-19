set -ex
echo Installing opam
cd /cygdrive/c/projects
tar -xf 'opam64.tar.xz'
echo Username is `whoami`
bash ./opam64/install.sh  --prefix /usr/`whoami`
/usr/`whoami`/bin/opam.exe init mingw 'https://github.com/fdopen/opam-repository-mingw.git' --comp "$OCAML_BRANCH"+mingw64c --switch "$OCAML_BRANCH"+mingw64c -y -a
eval $(/usr/`whoami`/bin/ocaml-env.exe cygwin)    
/usr/`whoami`/bin/opam.exe install depext depext-cygwinports -y
echo Installing distributed
echo distributed checkout dir is $APPVEYOR_BUILD_FOLDER
/usr/`whoami`/bin/opam.exe pin add distributed $APPVEYOR_BUILD_FOLDER -n -y
/usr/`whoami`/bin/opam.exe pin add distributed-lwt $APPVEYOR_BUILD_FOLDER -n -y
/usr/`whoami`/bin/opam.exe pin add lwt https://github.com/ocsigen/lwt.git\#safer-semantics -n -y
/usr/`whoami`/bin/opam.exe install distributed-lwt -y
cd distributed
make test