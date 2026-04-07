CXX := g++
CXXFLAGS := -std=c++20 -Wall -Wextra -Wpedantic -O2 -Iinclude -pthread
LDFLAGS := -pthread

SRC_COMMON := src/storage.cpp src/sql.cpp src/protocol.cpp
SRC_CLIENT := src/flexql_client.cpp
SRC_SERVER := src/server_main.cpp
SRC_REPL := src/repl_main.cpp

OBJ_COMMON := $(SRC_COMMON:.cpp=.o)
OBJ_CLIENT := $(SRC_CLIENT:.cpp=.o)
OBJ_SERVER := $(SRC_SERVER:.cpp=.o)
OBJ_REPL := $(SRC_REPL:.cpp=.o)

.PHONY: all clean

all: flexql_server flexql_repl

flexql_server: $(OBJ_COMMON) $(OBJ_SERVER)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)

flexql_repl: $(OBJ_COMMON) $(OBJ_CLIENT) $(OBJ_REPL)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)

clean:
	rm -f $(OBJ_COMMON) $(OBJ_CLIENT) $(OBJ_SERVER) $(OBJ_REPL) flexql_server flexql_repl
