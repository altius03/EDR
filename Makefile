COMPOSE := docker compose -f compose.yaml -f compose.observability.local.yaml
EVENTS ?= 1000
VUS ?= 10
DUP_EVERY ?= 0

.DEFAULT_GOAL := help
.PHONY: help start status logs test-alert test-load stop

help:
	@printf '%s\n' \
	  'make start                                      로컬 서비스와 Grafana 시작' \
	  'make status                                     서비스 상태 확인' \
	  'make logs                                       주요 서비스의 최근 로그 확인' \
	  'make test-alert                                 이벤트 1건의 저장·Alert 생성 확인' \
	  'make test-load EVENTS=1000 VUS=10 DUP_EVERY=0   Collector 부하테스트와 결과 검증' \
	  'make stop                                       서비스 종료(데이터 볼륨 보존)'

start:
	@docker info >/dev/null
	@test -f .env || { printf '%s\n' 'Missing .env; run uv run python -m tools.local_demo setup, then set GRAFANA_ADMIN_PASSWORD.' >&2; exit 1; }
	@$(COMPOSE) config --quiet
	@$(COMPOSE) up -d --build --wait
	@$(COMPOSE) ps

status:
	@$(COMPOSE) ps

logs:
	@$(COMPOSE) logs --tail=100 nginx backend event-storage-worker detection-worker grafana prometheus loki alloy

test-alert:
	@$(MAKE) test-load EVENTS=1 VUS=1 DUP_EVERY=0

test-load:
	@set -eu; \
	  for value in '$(EVENTS)' '$(VUS)' '$(DUP_EVERY)'; do \
	    case "$$value" in ''|*[!0-9]*) printf 'EVENTS, VUS, and DUP_EVERY must be integers.\n' >&2; exit 1;; esac; \
	  done; \
	  if [ '$(EVENTS)' -lt 1 ] || [ '$(EVENTS)' -gt 10000 ]; then \
	    printf 'EVENTS must be between 1 and 10000.\n' >&2; exit 1; \
	  fi; \
	  if [ '$(VUS)' -lt 1 ] || [ '$(VUS)' -gt 50 ] || [ '$(VUS)' -gt '$(EVENTS)' ]; then \
	    printf 'VUS must be between 1 and min(50, EVENTS).\n' >&2; exit 1; \
	  fi; \
	  if [ '$(DUP_EVERY)' -gt '$(EVENTS)' ]; then \
	    printf 'DUP_EVERY must be between 0 and EVENTS.\n' >&2; exit 1; \
	  fi
	@test -f .env || { printf '%s\n' 'Missing .env.' >&2; exit 1; }
	@command -v k6 >/dev/null || { printf '%s\n' 'k6 is not installed.' >&2; exit 1; }
	@command -v jq >/dev/null || { printf '%s\n' 'jq is not installed.' >&2; exit 1; }
	@test -f runtime/compose/cert-authority/ca/ca.crt
	@test -f runtime/compose/cert-authority/agents/edr-load-agent/agent.crt
	@test -f runtime/compose/cert-authority/agents/edr-load-agent/agent.key
	@set -eu; \
	  tls_code=$$(curl --cacert runtime/compose/cert-authority/ca/ca.crt \
	    --cert runtime/compose/cert-authority/agents/edr-load-agent/agent.crt \
	    --key runtime/compose/cert-authority/agents/edr-load-agent/agent.key \
	    --silent --show-error --output /dev/null --write-out '%{http_code}' \
	    https://127.0.0.1:8443/api/v1/collector/agents/register); \
	  case "$$tls_code" in 404|405) ;; *) printf 'mTLS check failed: HTTP %s\n' "$$tls_code" >&2; exit 1;; esac; \
	  prepared=$$(uv run python -m tools.collector_load prepare --iterations "$(EVENTS)"); \
	  manifest=$$(printf '%s\n' "$$prepared" | jq -er '.manifest'); \
	  run_id=$${manifest##*/}; run_id=$${run_id%.json}; \
	  printf 'Run %s: %s events, %s VUs, duplicate every %s\n' "$$run_id" '$(EVENTS)' '$(VUS)' '$(DUP_EVERY)'; \
	  k6_result=0; \
	  k6 run --summary-export "runtime/load/$$run_id-k6.json" \
	    -e RUN_MANIFEST="$$manifest" -e VUS="$(VUS)" -e DUPLICATE_EVERY="$(DUP_EVERY)" \
	    -e LOCAL_SKIP_TLS_VERIFY=1 tests/load/collector_alert.js || k6_result=$$?; \
	  verify_result=0; \
	  uv run python -m tools.collector_load verify --manifest "$$manifest" --wait-seconds 300 || verify_result=$$?; \
	  printf 'Manifest: %s\nk6 summary: runtime/load/%s-k6.json\n' "$$manifest" "$$run_id"; \
	  test "$$k6_result" -eq 0 && test "$$verify_result" -eq 0

stop:
	@$(COMPOSE) down
