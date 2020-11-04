
.PHONY: all test typecheck lint

all: test typecheck lint

test:
	@echo "== python test =="
	@pytest --quiet --doctest-modules spikes/bjarke/dataset/

typecheck:
	@echo "== python typecheck =="
	@pyright

lint:
	@echo "== python lint =="
	@black --quiet --check .
	@echo "== js lint =="
	@prettier --check .
