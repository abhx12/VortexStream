.PHONY: doctor build up down init reset-state lint \
        data data-fake profile preprocess sim-data \
        replay replay-demo q0 q1 q2 q3 q4 streams-stop \
        calibrate eval-alerts batch-demo train eval-anomaly \
        dashboard verify demo demo-stop e2e \
        test-phase0 test-phase1 test-phase2 test-phase3 test-phase4 \
        test-phase5 test-phase6 test-phase7 test-phase8 test-phase9 \
        test-phase10 test-phase11 test-phase12 test-phase13 test-phase14 \
        test-all

# Phase 0 & 1: Environment, infra, health
doctor:
	python scripts/doctor.py

build:
	docker compose build

up:
	docker compose up -d

down:
	docker compose down

init:
	docker compose exec app bash -c "bash scripts/create_topics.sh && python scripts/hbase_init.py"

reset-state:
	docker compose exec app bash scripts/reset_state.sh

lint:
	python -m py_compile scripts/doctor.py

# Phase 2: Puffer data
data:
	python -m qoe.data.puffer_download

data-fake:
	python -m qoe.data.make_fake_puffer

profile:
	python -m qoe.data.profile

# Phase 3: Preprocess
preprocess:
	@echo "not implemented until phase 3" && exit 2

# Phase 4: Sim data
sim-data:
	@echo "not implemented until phase 4" && exit 2

# Phase 5: Replay
replay:
	@echo "not implemented until phase 5" && exit 2

replay-demo:
	@echo "not implemented until phase 5" && exit 2

# Phase 6-8, 11: Streaming queries
q0:
	@echo "not implemented until phase 6" && exit 2

q1:
	@echo "not implemented until phase 7" && exit 2

q2:
	@echo "not implemented until phase 8" && exit 2

q3:
	@echo "not implemented until phase 8" && exit 2

q4:
	@echo "not implemented until phase 11" && exit 2

streams-stop:
	@echo "not implemented until phase 6" && exit 2

# Phase 9: Alerts & calibration
calibrate:
	@echo "not implemented until phase 9" && exit 2

eval-alerts:
	@echo "not implemented until phase 9" && exit 2

# Phase 10: Batch demo
batch-demo:
	@echo "not implemented until phase 10" && exit 2

# Phase 11: ML training & eval
train:
	@echo "not implemented until phase 11" && exit 2

eval-anomaly:
	@echo "not implemented until phase 11" && exit 2

# Phase 12: Dashboard
dashboard:
	@echo "not implemented until phase 12" && exit 2

# Phase 13: Verification
verify:
	@echo "not implemented until phase 13" && exit 2

# Phase 14: Demo & acceptance
demo:
	@echo "not implemented until phase 14" && exit 2

demo-stop:
	@echo "not implemented until phase 14" && exit 2

e2e:
	@echo "not implemented until phase 14" && exit 2

# Phase tests
test-phase0:
	pytest tests/phase_0 -v

test-phase1:
	pytest tests/phase_1 -v

test-phase2:
	pytest tests/phase_2 -v

test-phase3:
	@echo "not implemented until phase 3" && exit 2

test-phase4:
	@echo "not implemented until phase 4" && exit 2

test-phase5:
	@echo "not implemented until phase 5" && exit 2

test-phase6:
	@echo "not implemented until phase 6" && exit 2

test-phase7:
	@echo "not implemented until phase 7" && exit 2

test-phase8:
	@echo "not implemented until phase 8" && exit 2

test-phase9:
	@echo "not implemented until phase 9" && exit 2

test-phase10:
	@echo "not implemented until phase 10" && exit 2

test-phase11:
	@echo "not implemented until phase 11" && exit 2

test-phase12:
	@echo "not implemented until phase 12" && exit 2

test-phase13:
	@echo "not implemented until phase 13" && exit 2

test-phase14:
	@echo "not implemented until phase 14" && exit 2

test-all:
	pytest tests/phase_0 -v
