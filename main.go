package main

import (
	"fmt"
	"github.com/ardanlabs/conf"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/qubic/go-events-consumer/consume"
	"github.com/qubic/go-events-consumer/status"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kprom"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
)

const envPrefix = "QUBIC_EVENTS_CONSUMER"

func main() {
	if err := run(); err != nil {
		log.Fatalf("main: exited with error: %s", err.Error())
	}
}

func run() error {
	log.SetOutput(os.Stdout) // default is stderr

	var cfg struct {
		Elastic struct {
			Addresses              []string `conf:"default:https://localhost:9200"`
			Username               string   `conf:"default:qubic-ingestion"`
			Password               string   `conf:"default:none"`
			IndexName              string   `conf:"default:qubic-events-alias"`
			CertificateFingerprint string   `conf:"default:E4:D9:0B:F5:83:3E:86:B5:F1:25:FF:37:18:81:4B:42:62:7C:7F:45:34:B6:B9:87:DB:64:F6:40:BC:D3:1E:27"`
		}
		Broker struct {
			BootstrapServers string `conf:"default:localhost:9092"`
			MetricsPort      int    `conf:"default:9999"`
			MetricsNamespace string `conf:"default:qubic-kafka"`
			ConsumeTopic     string `conf:"default:qubic-events"`
			ConsumerGroup    string `conf:"default:qubic-elastic"`
		}
		Sync struct {
			InternalStoreFolder string `conf:"default:store"`
			Enabled             bool   `conf:"default:true"`
		}
	}

	// load config
	if err := conf.Parse(os.Args[1:], envPrefix, &cfg); err != nil {
		switch {
		case errors.Is(err, conf.ErrHelpWanted):
			usage, err := conf.Usage(envPrefix, &cfg)
			if err != nil {
				return errors.Wrap(err, "generating config usage")
			}
			fmt.Println(usage)
			return nil
		case errors.Is(err, conf.ErrVersionWanted):
			version, err := conf.VersionString(envPrefix, &cfg)
			if err != nil {
				return errors.Wrap(err, "generating config version")
			}
			fmt.Println(version)
			return nil
		}
		return errors.Wrap(err, "parsing config")
	}

	out, err := conf.String(&cfg)
	if err != nil {
		return errors.Wrap(err, "generating config for output")
	}
	log.Printf("main: Config :\n%v\n", out)

	m := kprom.NewMetrics(cfg.Broker.MetricsNamespace,
		kprom.Registerer(prometheus.DefaultRegisterer),
		kprom.Gatherer(prometheus.DefaultGatherer))
	kcl, err := kgo.NewClient(
		kgo.WithHooks(m),
		kgo.ConsumeTopics(cfg.Broker.ConsumeTopic),
		kgo.ConsumerGroup(cfg.Broker.ConsumerGroup),
		kgo.SeedBrokers(cfg.Broker.BootstrapServers),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer kcl.Close()

	esClient, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses:              cfg.Elastic.Addresses,
		Username:               cfg.Elastic.Username,
		Password:               cfg.Elastic.Password,
		CertificateFingerprint: cfg.Elastic.CertificateFingerprint,
	})
	elasticClient := consume.NewElasticClient(esClient, cfg.Elastic.IndexName)
	metrics := consume.NewMetrics(cfg.Broker.MetricsNamespace)
	consumer := consume.NewEventConsumer(kcl, elasticClient, metrics)
	if cfg.Sync.Enabled {
		go consumer.Consume()
	} else {
		log.Println("main: Event processing disabled")
	}

	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, os.Interrupt, syscall.SIGTERM)

	// metrics endpoint
	go func() {
		log.Printf("main: Starting status and metrics endpoint on port [%d].", cfg.Broker.MetricsPort)
		http.Handle("/status", &status.Handler{})
		http.Handle("/metrics", promhttp.Handler())
		log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", cfg.Broker.MetricsPort), nil))
	}()

	log.Println("main: Service started.")

	for {
		select {
		case <-shutdown:
			log.Println("main: Received shutdown signal, shutting down...")
			return nil
		}
	}
}
