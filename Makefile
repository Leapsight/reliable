.PHONY: test docs check compile eqwalizer dialyzer xref

test:
	rebar3 ct --readable=false -v 	

docs:
	rebar3 ex_doc skip_deps=true

compile:
	rebar3 compile

xref: compile
	rebar3 xref skip_deps=true

dialyzer: compile
	rebar3 dialyzer

eqwalizer:
	elp eqwalize-all

check: xref dialyzer eqwalizer
