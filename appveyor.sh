set -ex
echo Installing distributed
echo distributed checkout dir is $APPVEYOR_BUILD_FOLDER
/usr/`whoami`/bin/opam.exe pin add distributed $APPVEYOR_BUILD_FOLDER -n -y
/usr/`whoami`/bin/opam.exe pin add distributed-uwt $APPVEYOR_BUILD_FOLDER -n -y 
/usr/`whoami`/bin/opam.exe install distributed-uwt -y -v -t
cd distributed
dune build
dune build @runtest-uwt