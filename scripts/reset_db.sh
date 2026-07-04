#scripts\reset_db.sh
#!/bin/bash
set -a; [ -f .env ] && . .env; set +a
echo ">>> CẢNH BÁO: Đang reset database..."
read -p "Bạn có chắc chắn muốn xóa sạch DB? (y/n) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    docker compose exec postgres psql -U postgres -d camara_db -c "DROP SCHEMA public CASCADE; CREATE SCHEMA public;"
    echo ">>> DB đã được reset."
fi