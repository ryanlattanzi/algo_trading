
# Starts environment from scratch.
start:
	./bin/start_env.sh new

# Kills environment.
kill:
	./bin/kill_env.sh

# Integration test.
test-integration:
	./bin/test_integration.sh

# Pytest Unit Tests.
test-unit:
	./bin/test_unit.sh

# Builds new algo_trading package
.PHONY: dist
dist:
	./bin/make_dist.sh
