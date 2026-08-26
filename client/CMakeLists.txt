# If not stated otherwise in this file or this component's LICENSE file the
# following copyright and licenses apply:
#
# Copyright 2024 RDK Management
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Standalone on-device benchmark client - plain POSIX sockets and hand-built
# JSON-RPC text, deliberately with no dependency on WPEFramework's Core::
# classes, so it builds independently of the plugin target above it.

add_executable(es1client es1client.cpp)

set_target_properties(es1client PROPERTIES
    CXX_STANDARD 14
    CXX_STANDARD_REQUIRED YES
)

find_package(Threads REQUIRED)
target_link_libraries(es1client PRIVATE Threads::Threads)

install(TARGETS es1client
    DESTINATION ${CMAKE_INSTALL_PREFIX}/bin
)

# es1.config.default is installed to /opt/es1.config by the recipe's own
# do_install() (see es1test-jsonrpc-benchmark.bb) rather than here, since
# /opt sits outside CMAKE_INSTALL_PREFIX's normal staging tree.
