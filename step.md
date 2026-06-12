phase 1

1.
pytest tests/unit/mock_services/shared/ -v
2.
pytest tests/unit/api/schemas/ tests/unit/simulator/ -v
vấn đề : chỉ định dạng +xy chuẩn ,không chấp nhận 0 ... hay xy -> xử lí ở pipeline
3.
tests\unit\storage\migrations\run_test.bat

phase 2

1.
pytest tests\unit\mock_services\gsma_tac -v
2.
pytest tests\unit\mock_services\itu_e164 -v
3.
pytest tests\unit\mock_services\hlr_hss -v

dành cho các bước sau:
# Build cho GSMA TAC
docker build -f mock_services/gsma_tac/Dockerfile -t camara/gsma-tac-mock:latest .

# Build cho ITU E.164
docker build -f mock_services/itu_e164/Dockerfile -t camara/itu-e164-mock:latest .

phase 3

pytest tests/unit/simulator -v