
project(prod_funds_controller)

include(${ROOT}/cmake/proto/proto.cmake)
include(${ROOT}/cmake/conan/conan.cmake)

add_library(${PROJECT_NAME}
clickhouse_client.cpp
main_commands.cpp
block_trading.cpp
loans_manager.cpp
hedge_manager.cpp
transaction_manager.cpp
)

target_link_libraries(${PROJECT_NAME}
  util
  common
  connector
  prod_transfer
  CONAN_PKG::simdjson
  CONAN_PKG::clickhouse-cpp
)

target_include_directories(${PROJECT_NAME} PUBLIC ${PROJECT_SOURCE_DIR}/include)

add_executable(${PROJECT_NAME} main.cpp)
target_link_libraries(${PROJECT_NAME} common connector)