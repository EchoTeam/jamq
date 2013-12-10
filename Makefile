.PHONY: rel stagedevrel deps test

all: deps compile

compile: deps
	./rebar compile

deps:
	test -d deps || ./rebar get-deps

clean:
	./rebar clean

distclean: clean
	./rebar delete-deps
	rmdir deps

test: compile
	./rebar eunit skip_deps=true
