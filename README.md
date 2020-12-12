# gohangout-input-kafkago
此包为 https://github.com/childe/gohangout 项目的 kafka outputs 插件。

# 特点
使用[kafka-go](https://github.com/segmentio/kafka-go) 这个仓库来作为output
### TODO
- TLS配置项的支持
- 写入状态的统计数据

### DONE
SASL已经支持

# 使用方法

将 `gokafka_output.go` 复制到 `gohangout` 主目录下面, 运行

```bash
go build -buildmode=plugin -o gokafka_output.so gokafka_output.go
```

将 `gokafka_output.so` 路径作为 outputs

## gohangout 配置示例
所有参数字段名字都使用kafka-go原生的，所以和gohangout的kafka插件的配置名字有些不一样。主要是为了偷懒.

```yaml
inputs:
    - Stdin:
        codec: plain
outputs:
    - Stdout:
        if:
            - '{{if .error}}y{{end}}'
    - '/Users/fiendhuang/program/my/gohangout/gokafka_output.so':
        Brokers:
            - '127.0.0.1:9092'
        Topic: 'test'
        Compression: 'Gzip'
```
