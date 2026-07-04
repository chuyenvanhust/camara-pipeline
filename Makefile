# Makefile
.PHONY: up sim pipeline load-test report reset-db

up:
	docker compose up -d

sim:
	@chmod +x scripts/run_simulator.sh
	./scripts/run_simulator.sh

pipeline:
	@chmod +x scripts/run_pipeline.sh
	./scripts/run_pipeline.sh data/radius_log.csv

load-test:
	@chmod +x scripts/run_load_test.sh
	./scripts/run_load_test.sh

report:
	@chmod +x scripts/generate_report.sh
	./scripts/generate_report.sh

reset-db:
	@chmod +x scripts/reset_db.sh && ./scripts/reset_db.sh