
.PHONY: all test typecheck lint

all: test typecheck lint

# todo: fix test errors
test:
	@echo "== python test =="
	@pytest --quiet --doctest-modules lib/dataset/

# todo: fix type check errors
typecheck:
	@echo "== python typecheck =="
	@pyright

lint:
	@echo "== python lint =="
	@black --quiet --check .
	@echo "== js lint =="
	@prettier --check .
