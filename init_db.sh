psql -p 5433 -h localhost -U postgres -c 'CREATE DATABASE uma_processed'
psql -p 5433 -h localhost -U postgres -d uma_processed -f create_uma_rating_02.sql
