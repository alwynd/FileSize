cd ..
call dotnet "FileSize\bin\Debug\netcoreapp2.1\FileSize.dll" %* > scripts\output.log
if %errorlevel% neq 0 pause
