# Use -O3 instead of default for PostgreSQL -O2 to enable loop and vectorization optiomizations in GCC.
PATH=/usr/local/pgsql/bin/:$PATH make USE_PGXS=1 CFLAGS="-O3 -Wall"  LDFLAGS="-pthread" install