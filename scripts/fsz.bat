call dotnet "C:\alwyn\dev\DotnetCore\FileSize\FileSize\bin\Debug\netcoreapp2.1\FileSize.dll" %* > C:\alwyn\dev\DotnetCore\FileSize\scripts\output.log
if %errorlevel% neq 0 pause
