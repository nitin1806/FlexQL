#!/usr/bin/env bash
set -euo pipefail

CXX="${CXX:-g++}"
CXXFLAGS=(-std=c++20 -Wall -Wextra -Wpedantic -O2 -Iinclude -pthread)

COMMON_SRCS=(
  src/storage.cpp
  src/sql.cpp
  src/protocol.cpp
)

echo "Building flexql_server..."
"$CXX" "${CXXFLAGS[@]}" \
  "${COMMON_SRCS[@]}" \
  src/server_main.cpp \
  -o flexql_server

echo "Building flexql_repl..."
"$CXX" "${CXXFLAGS[@]}" \
  "${COMMON_SRCS[@]}" \
  src/flexql_client.cpp \
  src/repl_main.cpp \
  -o flexql_repl

echo "Building benchmark_flexql..."
"$CXX" "${CXXFLAGS[@]}" \
  "${COMMON_SRCS[@]}" \
  src/flexql_client.cpp \
  benchmark_flexql.cpp \
  -o benchmark_flexql

echo "Build complete."
