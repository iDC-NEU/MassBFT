echo "go version" "`go version | { read _ _ v _; echo ${v#go}; }`"

go build -buildmode=c-shared -o libgrc.so main.go
