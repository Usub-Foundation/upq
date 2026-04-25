cmake_minimum_required(VERSION 3.27)
cmake_policy(SET CMP0144 NEW)
project(upq VERSION 0.1.0 LANGUAGES CXX)

include(GNUInstallDirs)
include(CMakePackageConfigHelpers)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_CXX_FLAGS_DEBUG "-g -O0" CACHE STRING "Debug flags" FORCE)
set(CMAKE_C_FLAGS_DEBUG "-g -O0" CACHE STRING "Debug flags" FORCE)

option(UPQ_POOL_DEBUG OFF)

add_compile_definitions(DEV_STAGE=${DEV_STAGE})

message(STATUS "DEV_STAGE is set to ${DEV_STAGE}")

option(UPQ_HARDENING "Enable compile-time hardening flags" ON)

find_package(OpenSSL REQUIRED)
find_package(PostgreSQL REQUIRED)

include(FetchContent)

FetchContent_Declare(
        uvent
        GIT_REPOSITORY https://github.com/Usub-Foundation/uvent.git
        GIT_TAG main
        OVERRIDE_FIND_PACKAGE
)

FetchContent_Declare(
        ureflect
        GIT_REPOSITORY https://github.com/Usub-Foundation/ureflect.git
        GIT_TAG main
        OVERRIDE_FIND_PACKAGE
)

FetchContent_Declare(
        ujson
        GIT_REPOSITORY https://github.com/Usub-Foundation/ujson.git
        GIT_TAG main
        OVERRIDE_FIND_PACKAGE
)

FetchContent_MakeAvailable(uvent ureflect ujson)

file(GLOB_RECURSE UPQ_HEADERS CONFIGURE_DEPENDS
        ${CMAKE_CURRENT_SOURCE_DIR}/include/upq/*.h
        ${CMAKE_CURRENT_SOURCE_DIR}/include/upq/*.hpp
)
file(GLOB_RECURSE UPQ_SOURCES CONFIGURE_DEPENDS
        ${CMAKE_CURRENT_SOURCE_DIR}/src/upq/*.cpp
)

add_library(upq ${UPQ_SOURCES} ${UPQ_HEADERS})
add_library(usub::upq ALIAS upq)

target_include_directories(upq
        PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/upq>
)

target_link_libraries(upq
        PUBLIC
        usub::uvent
        usub::ureflect
        usub::ujson
        OpenSSL::Crypto
        PostgreSQL::PostgreSQL
)

if (TARGET PostgreSQL::pq)
        target_link_libraries(upq PUBLIC PostgreSQL::pq)
endif()

if (UPQ_HARDENING)
        if (CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
                target_compile_options(upq PRIVATE
                        -Wall -Wextra -Wpedantic
                        -Wformat=2 -Wformat-security
                        -Wcast-qual -Wcast-align
                        -Wnull-dereference
                        -fstack-protector-strong
                        $<$<NOT:$<CONFIG:Debug>>:-D_FORTIFY_SOURCE=2>
                )
                target_link_options(upq PRIVATE
                        -Wl,-z,relro -Wl,-z,now -Wl,-z,noexecstack
                )
        elseif (MSVC)
                target_compile_options(upq PRIVATE /W4 /guard:cf /sdl)
        endif()
endif()

set_target_properties(upq PROPERTIES
        EXPORT_NAME upq
        VERSION ${PROJECT_VERSION}
        SOVERSION ${PROJECT_VERSION_MAJOR}
)

option(TRADER_LOCAL_DEV "Disable Pulsar producers for local dev" OFF)

if(UPQ_REFLECT_DEBUG)
        target_compile_definitions(${PROJECT_NAME} PRIVATE UPQ_REFLECT_DEBUG=1)
else()
        target_compile_definitions(${PROJECT_NAME} PRIVATE UPQ_REFLECT_DEBUG=0)
endif()

option(UPQ_BUILD_EXAMPLES "Build UPQ example executables" OFF)
if (UPQ_BUILD_EXAMPLES)
        add_executable(upq_example examples/main.cpp)
        target_link_libraries(upq_example PRIVATE upq)
        target_compile_definitions(upq_example PRIVATE DEV_STAGE=${DEV_STAGE})

        add_executable(upq_example_timeout examples/main_timeout.cpp)
        target_link_libraries(upq_example_timeout PRIVATE upq)
        target_compile_definitions(upq_example_timeout PRIVATE DEV_STAGE=${DEV_STAGE})

        add_executable(upq_example_large_exports examples/large_exports.cpp)
        target_link_libraries(upq_example_large_exports PRIVATE upq)
        target_compile_definitions(upq_example_large_exports PRIVATE DEV_STAGE=${DEV_STAGE})

        FetchContent_Declare(
                ulog
                GIT_REPOSITORY https://github.com/Usub-Foundation/ulog.git
                GIT_TAG main
                OVERRIDE_FIND_PACKAGE
        )
        FetchContent_Declare(
                unet
                GIT_REPOSITORY https://github.com/Usub-Foundation/unet.git
                GIT_TAG v0.2.1
                OVERRIDE_FIND_PACKAGE
        )
        FetchContent_MakeAvailable(unet ulog)

        configure_file(
                ${CMAKE_CURRENT_SOURCE_DIR}/examples/db_timeout/source_dir.h.in
                ${CMAKE_CURRENT_BINARY_DIR}/examples/db_timeout/source_dir.h
                @ONLY
        )

        add_executable(upq_example_db_timeout
                examples/db_timeout/main_db_timeout.cpp
        )
        target_include_directories(upq_example_db_timeout PRIVATE
                ${CMAKE_CURRENT_BINARY_DIR}/examples/db_timeout
        )
        target_link_libraries(upq_example_db_timeout PRIVATE upq usub::server usub::ulog)
        target_compile_definitions(upq_example_db_timeout PRIVATE DEV_STAGE=${DEV_STAGE})
endif()

install(TARGETS upq
        EXPORT upqTargets
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
        RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
        INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/upq
)

install(DIRECTORY include/
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/upq
        FILES_MATCHING
        PATTERN "*.h"
        PATTERN "*.hpp"
)

configure_package_config_file(
        cmake/upqConfig.cmake.in
        ${CMAKE_CURRENT_BINARY_DIR}/upqConfig.cmake
        INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/upq
        NO_CHECK_REQUIRED_COMPONENTS_MACRO
)

write_basic_package_version_file(
        ${CMAKE_CURRENT_BINARY_DIR}/upqConfigVersion.cmake
        VERSION ${PROJECT_VERSION}
        COMPATIBILITY SameMajorVersion
)

install(FILES
        ${CMAKE_CURRENT_BINARY_DIR}/upqConfig.cmake
        ${CMAKE_CURRENT_BINARY_DIR}/upqConfigVersion.cmake
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/upq
)

install(EXPORT upqTargets
        NAMESPACE upq::
        FILE upqTargets.cmake
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/upq
)
