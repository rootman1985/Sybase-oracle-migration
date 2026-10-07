#!/usr/bin/env bash
set -e
if [ -z "$1" ]; then
  echo "Usage: ./run.sh CHECK [TABLE]"
  echo "       ./run.sh MIGRATE [TABLE]"
  exit 1
fi
java -cp "target/classes:target/dependency/*:lib/*" fr.migration.MigrationApplication "$@"
