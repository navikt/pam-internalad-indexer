#!/bin/sh
TIMEOUT=60
COMMAND=$@
run_integration() {
  for i in `seq $TIMEOUT` ; do
    result=$(curl -s http://localhost:8080/internal/isAlive)
    if [ "$result" = "OK" ] ; then
      if [ -n "$COMMAND" ] ; then
        exec $COMMAND
      fi
      exit 0
    fi
    sleep 1
  done
  echo "Integration script timed out" >&2
  docker-compose logs internalad-indexer
  exit 1
}
run_integration
