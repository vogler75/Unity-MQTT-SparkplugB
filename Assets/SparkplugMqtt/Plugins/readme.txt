set SRC_DIR=D:\Workspace\tahu\sparkplug_b
set DST_DIR=D:\Workspace\tahu\sparkplug_b
protoc -I=%SRC_DIR% --csharp_out=%DST_DIR% %SRC_DIR%\sparkplug_b.proto