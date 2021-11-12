big:
	dotnet run localhost 3003 0 </dev/null >/dev/null &
	dotnet run localhost 3003 1 </dev/null >/dev/null &
	dotnet run localhost 3003 2 </dev/null >/dev/null &
	dotnet run localhost 3003 3 </dev/null >/dev/null &
	dotnet run localhost 3003 4 </dev/null >/dev/null &
	dotnet run localhost 3003 5 </dev/null >/dev/null &
	dotnet run localhost 3003 6 </dev/null >/dev/null &
	dotnet run localhost 3003 7 </dev/null >/dev/null &

debug:
	/bin/rm -f client.log
	touch client.log
	dotnet run localhost 3003 0 -debug </dev/null >>client.log &
	dotnet run localhost 3003 1 -debug </dev/null >>client.log &
	dotnet run localhost 3003 2 -debug </dev/null >>client.log &
	dotnet run localhost 3003 3 -debug </dev/null >>client.log &

clean:
	dotnet run localhost 3003 0 </dev/null >/dev/null &
	dotnet run localhost 3003 1 </dev/null >/dev/null &
	dotnet run localhost 3003 2 </dev/null >/dev/null &
	dotnet run localhost 3003 3 </dev/null >/dev/null &
