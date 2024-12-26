# 查找 go 命令的路径
GO := $(shell which go)
BINARY_NAME = oula-shares-monitor
SRC = main.go

# 默认目标
all: build

# 构建二进制文件
build:
	$(GO) build -o $(BINARY_NAME) $(SRC)

# 清理生成的文件
clean:
	rm -f $(BINARY_NAME)

.PHONY: all build clean.root