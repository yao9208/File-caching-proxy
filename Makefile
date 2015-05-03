all: Proxy.class Server.class FileService.class LRUCache.class

%.class: %.java
	javac $<

clean:
	rm -f *.class
