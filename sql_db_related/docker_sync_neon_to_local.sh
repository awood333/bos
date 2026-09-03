#sql_db_related/docker_sync_neon_to_local.sh
echo "=== Starting local Postgres container (if not already running) ==="
docker start bos_local_pg 2>/dev/null || echo "Container already running."

echo "=== Waiting for Postgres to accept connections ==="
until docker exec bos_local_pg pg_isready -U al -d neondb > /dev/null 2>&1; do
  sleep 1
done

echo "=== Syncing Neon -> local ==="
cd /home/alanw/git_repos/bos_project/bos_backend
.venv/bin/python sql_db_related/sync_from_neon.py
SYNC_STATUS=$?

echo ""
if [ $SYNC_STATUS -eq 0 ]; then
    echo "=== Sync script finished OK. Verifying local DB ==="
    TABLE_COUNT=$(docker exec bos_local_pg psql -U al -d neondb -t -c "SELECT count(*) FROM information_schema.tables WHERE table_schema='public';" | tr -d '[:space:]')
    echo "Local DB now has $TABLE_COUNT tables."
    echo ""
    echo "=== SUCCESS ==="
else
    echo "=== SYNC FAILED (exit code $SYNC_STATUS) — see error above ==="
fi

echo ""
read -p "Press Enter to close..."
