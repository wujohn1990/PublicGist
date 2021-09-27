cmake_minimum_required(VERSION 3.0.0)
cmake_policy(SET CMP0048 NEW)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)

project(PublicGist VERSION 0.1.0)

find_package(Boost COMPONENTS program_options REQUIRED)
if(Boost_FOUND)
    include_directories( ${Boost_INCLUDE_DIRS} )
endif()


add_subdirectory(MergeSortedFiles)
