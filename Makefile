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
	python -m qoe.data.preprocess

# Phase 4: Sim data
sim-data:
	python -m qoe.data.ref_data

# Phase 5: Replay
replay:
	python -m qoe.producers.replay $(ARGS)

replay-demo:
	python -m qoe.producers.replay --speed 60 --duration 6 --dry-run

# Phase 6-8, 11: Streaming queries
q0:
	python -m qoe.streaming.q0_ingest_clean

q1:
	python -m qoe.streaming.q1_session_tracker

q2:
	python -m qoe.streaming.q2_qoe_aggregator

q3:
	python -m qoe.streaming.q3_network_aggregator

q4:
	python -m qoe.streaming.q4_ml_scoring

streams-stop:
	@echo "Stopping streaming queries..."

# Phase 9: Alerts & calibration
calibrate:
	python -m qoe.streaming.calibrate

eval-alerts:
	python -m qoe.verify.eval_alerts

# Phase 10: Batch demo
batch-demo:
	python -m qoe.batch.rdd_demo
	python -m qoe.batch.sql_demo
	python -m qoe.batch.warehouse
	python -m qoe.batch.hourly_rollup

# Phase 11: ML training & eval
train:
	python -m qoe.ml.churn_model

eval-anomaly:
	python -m qoe.verify.eval_anomaly

# Phase 12: Dashboard
dashboard:
	python -m qoe.dashboard.app

# Phase 13: Verification
verify:
	python -m qoe.verify.reconcile

# Phase 14: Demo & acceptance
demo:
	python scripts/demo.py

demo-stop:
	python scripts/reset_state.py

e2e:
	pytest tests/phase_14 -v

# Phase tests
test-phase0:
	pytest tests/phase_0 -v

test-phase1:
	pytest tests/phase_1 -v

test-phase2:
	pytest tests/phase_2 -v

test-phase3:
	pytest tests/phase_3 -v

test-phase4:
	pytest tests/phase_4 -v

test-phase5:
	pytest tests/phase_5 -v

test-phase6:
	pytest tests/phase_6 -v

test-phase7:
	pytest tests/phase_7 -v

test-phase8:
	pytest tests/phase_8 -v

test-phase9:
	pytest tests/phase_9 -v

test-phase10:
	pytest tests/phase_10 -v

test-phase11:
	pytest tests/phase_11 -v

test-phase12:
	pytest tests/phase_12 -v

test-phase13:
	pytest tests/phase_13 -v

test-phase14:
	pytest tests/phase_14 -v

test-all:
	pytest tests/phase_0 tests/phase_1 tests/phase_2 tests/phase_3 tests/phase_4 tests/phase_5 tests/phase_6 tests/phase_7 tests/phase_8 tests/phase_9 tests/phase_10 tests/phase_11 tests/phase_12 tests/phase_13 tests/phase_14 -v


