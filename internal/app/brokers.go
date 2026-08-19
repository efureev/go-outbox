package app

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/efureev/appmod/v4"

	"github.com/efureev/go-outbox/internal/broker"
	"github.com/efureev/go-outbox/internal/broker/kafka"
	"github.com/efureev/go-outbox/internal/broker/postgres"
	"github.com/efureev/go-outbox/internal/broker/rabbitmq"
	"github.com/efureev/go-outbox/internal/config"
	"github.com/efureev/go-outbox/internal/logging"
)

// brokersModule builds one publisher per configured driver and publishes the
// router that maps streams onto them.
type brokersModule struct {
	*appmod.BaseAppModule

	cfg    config.Config
	log    *slog.Logger
	router *broker.Router
}

func newBrokersModule(cfg config.Config, log *slog.Logger) *brokersModule {
	m := &brokersModule{
		BaseAppModule: appmod.New(appmod.WithConfig(appmod.NewConfig(ModuleBrokers, "v1"))),
		cfg:           cfg,
		log:           log.With(slog.String(logging.ModuleKey, ModuleBrokers)),
	}

	m.BeforeStart(m.connect)
	m.AfterStart(m.publish)

	return m
}

func (m *brokersModule) connect(ctx context.Context, _ appmod.HookModule) error {
	publishers := make(map[string]broker.Publisher, len(m.cfg.Brokers.Drivers))

	for name, driver := range m.cfg.Brokers.Drivers {
		p, err := m.build(ctx, name, driver)
		if err != nil {
			return err
		}

		// Registered next to the connection it closes, so a later driver
		// failing to connect unwinds the ones already open.
		m.AddCleanup(p.Close)
		publishers[name] = p

		m.log.Info("broker connected",
			slog.String("driver", name),
			slog.String("type", string(driver.Type())),
		)
	}

	router, err := broker.NewRouter(m.cfg.Brokers, publishers)
	if err != nil {
		return err
	}
	m.router = router

	return nil
}

func (m *brokersModule) build(ctx context.Context, name string, driver config.DriverConfig) (broker.Publisher, error) {
	log := m.log.With(slog.String("driver", name))

	switch d := driver.(type) {
	case *config.RabbitMQDriver:
		return rabbitmq.New(ctx, d, log)
	case *config.KafkaDriver:
		return kafka.New(ctx, d, log)
	case *config.PostgresDriver:
		return postgres.New(ctx, d, log)
	default:
		return nil, fmt.Errorf("driver %q has unsupported type %q", name, driver.Type())
	}
}

func (m *brokersModule) publish(_ context.Context, _ appmod.HookModule) error {
	registry := m.AppContext().Registry

	if err := appmod.Provide[*broker.Router](registry, m.router); err != nil {
		return err
	}
	m.AddCleanup(func(context.Context) error {
		_, err := appmod.Revoke[*broker.Router](registry)

		return err
	})

	return nil
}

// HealthCheck reports the first unreachable broker.
func (m *brokersModule) HealthCheck(ctx context.Context) error {
	if m.router == nil {
		return errNotStarted
	}

	return m.router.HealthCheck(ctx)
}

// Router exposes the routing table for tests.
func (m *brokersModule) Router() *broker.Router { return m.router }
