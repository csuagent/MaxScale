cmake_minimum_required (VERSION 2.6)

set(CMAKE_LIBRARY_PATH ${CMAKE_LIBRARY_PATH} /usr /usr/local)
set(CMAKE_INCLUDE_PATH ${CMAKE_INCLUDE_PATH} /usr /usr/local)

include(InstallRequiredSystemLibraries)

project (consumer)

find_path(MYSQL_INCLUDE_DIRS mysql.h PATH_SUFFIXES include include/mysql include/mariadb)
find_path(RABBITMQ_C_INCLUDE_DIRS amqp.h PATH_SUFFIXES include)
find_library(MYSQL_LIBRARIES NAMES mysqlclient PATH_SUFFIXES lib lib/mysql lib/mariadb lib64 lib64/mariadb lib64/mysql)
find_library(RABBITMQ_C_LIBRARIES NAMES rabbitmq PATH_SUFFIXES lib lib64)

include_directories(${MYSQL_INCLUDE_DIRS})
include_directories(${RABBITMQ_C_INCLUDE_DIRS})
include_directories(${CMAKE_SOURCE_DIR}/inih)

add_subdirectory (inih)
link_directories(${CMAKE_SOURCE_DIR}/inih)

if(RABBITMQ_C_LIBRARIES AND MYSQL_LIBRARIES AND MYSQL_INCLUDE_DIRS)

add_executable (consumer consumer.c ${MYSQL_LIBRARIES} ${RABBITMQ_C_LIBRARIES})
target_link_libraries(consumer ${MYSQL_LIBRARIES})
target_link_libraries(consumer ${RABBITMQ_C_LIBRARIES})
target_link_libraries(consumer inih)
install(TARGETS consumer DESTINATION bin)
install(FILES consumer.cnf DESTINATION etc)


else(RABBITMQ_C_LIBRARIES AND MYSQL_LIBRARIES AND MYSQL_INCLUDE_DIRS)

if(NOT RABBITMQ_C_LIBRARIES)
message(FATAL_ERROR "Error: Can not find requred libraries: librabbitmq.")
endif(NOT RABBITMQ_C_LIBRARIES)

if(NOT MYSQL_LIBRARIES)
message(FATAL_ERROR "Error: Can not find requred libraries: libmysqlclient.")
endif(NOT MYSQL_LIBRARIES)

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