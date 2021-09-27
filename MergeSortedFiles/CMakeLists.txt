# MergeCsv
add_executable(MergeSortedFiles MergeSortedFiles.cpp)

target_link_libraries( MergeSortedFiles
    pthread
    ${Boost_LIBRARIES}
)
