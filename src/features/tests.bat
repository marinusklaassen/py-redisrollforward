@echo off
:tests.bat
cls
SET PYTHONPATH=%AmberEclipseWorkspace%py-redisrollforward/src;%AmberEclipseWorkspace%py-redisrollforward/features;%PYTHONPATH%
cd /d "%AmberEclipseWorkspace%py-redisrollforward\features"

behave --no-capture %*

:
: Example run on windows:
: cd /d "%AmberEclipseWorkspace%py-redisrollforward\features"
: tests.bat --include aof_reconstruct_and_recreate.feature
: (don't specify --include if you want to run all tests)
:
:EOF
