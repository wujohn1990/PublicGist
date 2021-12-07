## Luigi

1. if both task_manager and task_manager_new have same task int_range(), task_manager is still writing to the int_range().output() file, 
    1. if int_range() are the same (no parameters, same task id), Luigi will detect the same task is run in another work, and current worker will stop
    2. if int_range() has it's own unique parameters, and are different two task instances, task_manager_new will run init_range(param) and cube_int_range(). This could cause problems if the two task managers write to the same output file. If task_manager's init_range() is already done, task_manager_new will start cube_int_range() task directly.

    - so 
    - a) write to a small output file, or just use a small output file for complete status
    - b) we use an ID parameter in the filename (like a global YYYYMMDDHHMM id, but this also does duplicate jobs)
    - c) orchestra all the dependentent tasks in one manager  

2. can pass in parameters, and on web ui, tasks can be filtered by paramters
