TARGET := dupfiles.native

.PHONY: all build clean run

all:
	@$(MAKE) -s clean
	@$(MAKE) -s build

build:
	@ocamlbuild -cflags '-w A' $(TARGET)

clean:
	@ocamlbuild -clean

run:
	@find ~ -type f | egrep -v '/.git/' | ./$(TARGET)
