cmake_minimum_required (VERSION 2.6)

include(InstallRequiredSystemLibraries)
project (consumer)
set(LIB_SEARCH_DIRS /usr/lib /usr/lib64 /usr/local/lib /usr/local/lib64)
set(HDR_SEARCH_DIRS /usr/include /usr/local/include /usr/include/mysql /usr/local/include/mysql)

find_path(MYSQL_INCLUDE_DIRS mysql.h ${HDR_SEARCH_DIRS})
find_library(MYSQL_LIBRARIES NAMES mysqld PATHS ${LIB_SEARCH_DIRS})
find_library(RABBITMQ_C_LIBRARIES NAMES rabbitmq PATHS ${LIB_SEARCH_DIRS})

include_directories(${MYSQL_INCLUDE_DIRS})
include_directories(${RABBITMQ_C_INCLUDE_DIRS})
include_directories(${CMAKE_SOURCE_DIR}/inih)

add_subdirectory (inih)
link_directories(${CMAKE_SOURCE_DIR}/inih)

if(RABBITMQ_C_LIBRARIES AND MYSQL_LIBRARIES AND MYSQL_INCLUDE_DIRS)

add_executable (consumer consumer.c ${MYSQL_LIBRARIES} ${RABBITMQ_C_LIBRARIES})
target_link_libraries(consumer mysqld)
target_link_libraries(consumer rabbitmq)
target_link_libraries(consumer inih)
install(TARGETS consumer DESTINATION bin)
else(RABBITMQ_C_LIBRARIES AND MYSQL_LIBRARIES AND MYSQL_INCLUDE_DIRS)
message(FATAL_ERROR "Error: Can not find requred libraries: libmysqld, librabbitmq.")
endif(RABBITMQ_C_LIBRARIES AND MYSQL_LIBRARIES AND MYSQL_INCLUDE_DIRS)

set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "RabbitMQ Consumer Client")
set(CPACK_PACKAGE_NAME "RabbitMQ Consumer")
set(CPACK_GENERATOR "RPM")
set(CPACK_PACKAGE_VERSION_MAJOR "1")
set(CPACK_PACKAGE_VERSION_MINOR "0")
set(CPACK_RPM_PACKAGE_NAME "rabbitmq-consumer")
set(CPACK_RPM_PACKAGE_VENDOR "SkySQL Ab")
set(CPACK_RPM_PACKAGE_AUTOREQPROV " no")
include(CPack)