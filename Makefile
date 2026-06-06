build:
	go build -o "bin/$(APP)" "./cmd/$(APP)"

mtest:
	cd maelstrom && ./maelstrom test -w echo --bin ../bin/$(APP) --node-count 1 --time-limit 10
