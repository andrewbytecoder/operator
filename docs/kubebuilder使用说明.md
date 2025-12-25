## 初始化工程



```bash
mkdir $GOPATH/memcached-operator
cd $GOPATH/memcached-operator
kubebuilder init --domain=example.com
```
> 如果您的项目在 GOPATH 内初始化，则隐式调用的 go mod init 将为您插入模块路径。否则，必须设置 --repo=<module path>。

