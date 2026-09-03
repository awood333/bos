# sql_db_related/check_sync_status.sh

cd ~/git_repos/bos_project/bos_backend/sql_db_related

echo "=== Starting local Postgres container (if not already running) ==="
docker start bos_local_pg 2>/dev/null || echo "Container already running."

echo "=== Waiting for Postgres to accept connections ==="
until docker exec bos_local_pg pg_isready -U al -d neondb > /dev/null 2>&1; do
    sleep 1
done

echo "=== Local sync status ==="
docker exec bos_local_pg psql -U al -d neondb -c "SELECT * FROM _sync_metadata"

echo ""
read -p "Press Enter to close..."