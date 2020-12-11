package main

import (
	"context"
	"crypto/tls"
	"github.com/childe/gohangout/value_render"
	"github.com/segmentio/kafka-go/compress"
	"time"

	"github.com/childe/gohangout/codec"
	"github.com/golang/glog"
	kafka_go "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"
)

// GoKafkaOutput 使用的Kafka-go的output插件
type GoKafkaOutput struct {
	config   map[interface{}]interface{}
	encoder  codec.Encoder
	producer *kafka_go.Writer
	key      value_render.ValueRender
}

/**
格式转换
*/
func (p *GoKafkaOutput) getProducerConfig(config map[interface{}]interface{}) (*kafka_go.WriterConfig, error) {
	c := &kafka_go.WriterConfig{
		Brokers: make([]string, 1),
	}
	if v, ok := config["Brokers"]; ok {
		for _, vv := range v.([]interface{}) {
			c.Brokers = append(c.Brokers, vv.(string))
		}
	} else {
		glog.Fatal("Brokers must be set")
	}
	if v, ok := config["BatchSize"]; ok {
		c.BatchSize = v.(int)
	}
	if v, ok := config["BatchBytes"]; ok {
		c.BatchBytes = v.(int)
	}
	if v, ok := config["BatchTimeout"]; ok {
		c.BatchTimeout = time.Duration(v.(int)) * time.Second
	}
	if v, ok := config["ReadTimeout"]; ok {
		c.ReadTimeout = time.Duration(v.(int)) * time.Second
	}
	if v, ok := config["WriteTimeout"]; ok {
		c.WriteTimeout = time.Duration(v.(int)) * time.Second
	}
	if v, ok := config["Acks"]; ok {
		c.RequiredAcks = v.(int)
	}
	if v, ok := config["Balancer"]; ok {
		vc := v.(map[string]interface{})
		if vv, ok1 := vc["Type"]; ok1 {
			switch vv.(string) {
			case "Hash":
				c.Balancer = &kafka_go.Hash{}
			case "CRC32":
				c.Balancer = &kafka_go.CRC32Balancer{}
			case "Murmur2":
				c.Balancer = &kafka_go.Murmur2Balancer{}
			default:
				c.Balancer = &kafka_go.RoundRobin{}
			}
		} else {
			glog.Fatal("Missing Balancer Type")
		}
	}
	if v, ok := config["Compression"]; ok {
		switch v.(string) {
		case "Gzip":
			c.CompressionCodec = compress.Compression(1).Codec()
		case "Snappy":
			c.CompressionCodec = compress.Compression(2).Codec()
		case "Lz4":
			c.CompressionCodec = compress.Compression(3).Codec()
		case "Zstd":
			c.CompressionCodec = compress.Compression(4).Codec()
		default:
			c.CompressionCodec = nil
		}
	}
	if v, ok := config["Topic"]; ok {
		c.Topic = v.(string)
	} else {
		glog.Fatal("Topic must be set")
	}
	var dialer *kafka_go.Dialer
	if v, ok := config["Timeout"]; ok {
		i := v.(int)
		dialer = &kafka_go.Dialer{
			Timeout:   time.Duration(i) * time.Second,
			DualStack: true,
		}
	} else {
		dialer = &kafka_go.Dialer{
			Timeout:   10 * time.Second,
			DualStack: true,
		}
	}
	if v, ok := config["SASL"]; ok {
		vh := v.(map[string]interface{})
		v1, ok1 := vh["Type"]
		v2, ok2 := vh["Username"]
		v3, ok3 := vh["Password"]
		if ok1 && ok2 && ok3 {
			var (
				mechanism sasl.Mechanism
				err       error
			)
			switch v1.(string) {
			case "Plain":
				mechanism = plain.Mechanism{
					Username: v2.(string),
					Password: v3.(string),
				}
			case "SCRAM":
				mechanism, err = scram.Mechanism(
					scram.SHA512,
					v2.(string),
					v3.(string),
				)
				if err != nil {
					glog.Fatal("ERROR FOR SCRAM: ", err)
				}
			default:
				glog.Fatalf("ERROR SASL type: %s", v1.(string))
			}
			dialer.SASLMechanism = mechanism
		} else {
			glog.Fatal("NO CONFIG FOR SASL")
		}
	}
	//TODO 后面再看是否需要完成了，理论上内网的KAFKA不会开启TLS，毕竟这个耗性能
	if v, ok := config["TLS"]; ok {
		vh := v.(map[string]interface{})
		var tls *tls.Config
		if _, ok1 := vh["PrivateKey"]; ok1 {
			glog.Info("TODO")
		}
		dialer.TLS = tls
	}
	c.Dialer = dialer
	if err := c.Validate(); err != nil {
		glog.Fatal("ReadConfig Validate error: ", err)
		return nil, err
	}
	return c, nil
}

/*
New 插件模式的初始化
*/
func New(config map[interface{}]interface{}) interface{} {
	p := &GoKafkaOutput{
		config: config,
	}
	if v, ok := config["codec"]; ok {
		p.encoder = codec.NewEncoder(v.(string))
	} else {
		p.encoder = codec.NewEncoder("json")
	}
	pConf, err := p.getProducerConfig(config)
	if err != nil {
		glog.Fatal("Error Config: ", err)
	}
	p.producer = kafka_go.NewWriter(*pConf)
	return p
}

//Emit 单次事件的处理函数
func (p *GoKafkaOutput) Emit(event map[string]interface{}) {
	buf, err := p.encoder.Encode(event)
	if err != nil {
		glog.Errorf("marshal %v error: %s", event, err)
		return
	}
	if p.key == nil {
		err = p.producer.WriteMessages(
			context.Background(),
			kafka_go.Message{
				Key:   nil,
				Value: buf,
			})
	} else {
		key := []byte(p.key.Render(event).(string))
		err = p.producer.WriteMessages(
			context.Background(),
			kafka_go.Message{
				Key:   key,
				Value: buf,
			})
	}
	if err != nil {
		glog.Error("Write Message: ", err)
	}
}

//Shutdown 关闭需要做的事情
func (p *GoKafkaOutput) Shutdown() {
	if err := p.producer.Close(); err != nil {
		glog.Fatal("failed to close writer:", err)
	}
}
