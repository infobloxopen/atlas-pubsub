package main

// An example pubsub grpc server. This uses the aws message broker as its
// underlying messaging system.

import (
	"context"
	"fmt"
	"log"
	"net"
	"strings"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"google.golang.org/grpc"

	"github.com/infobloxopen/atlas-app-toolkit/health"
	"github.com/infobloxopen/atlas-app-toolkit/server"
	pubsub "github.com/infobloxopen/atlas-pubsub"
	pubsubaws "github.com/infobloxopen/atlas-pubsub/aws"
	pubsubgrpc "github.com/infobloxopen/atlas-pubsub/grpc"
)

func main() {
	doneC := make(chan error)
	logger := NewLogger()
	if viper.GetBool("internal.enable") {
		go func() { doneC <- ServeInternal(logger) }()
	}

	log.Printf("starting aws pubsub server on port %s", viper.GetString("server.port"))
	lis, err := net.Listen("tcp", fmt.Sprintf("%s:%s", viper.GetString("server.address"), viper.GetString("server.port")))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	grpcServer := grpc.NewServer()

	pubsubServer, err := newAWSPubSubServer()
	if err != nil {
		log.Fatalf("failed to create aws pubsub server: %v", err)
	}
	pubsubgrpc.RegisterPubSubServer(grpcServer, pubsubServer)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

func NewLogger() *logrus.Logger {
	logger := logrus.StandardLogger()

	// Set the log level on the default logger based on command line flag
	logLevels := map[string]logrus.Level{
		"debug":   logrus.DebugLevel,
		"info":    logrus.InfoLevel,
		"warning": logrus.WarnLevel,
		"error":   logrus.ErrorLevel,
		"fatal":   logrus.FatalLevel,
		"panic":   logrus.PanicLevel,
	}
	if level, ok := logLevels[viper.GetString("logging.level")]; !ok {
		logger.Errorf("Invalid %q provided for log level", viper.GetString("logging.level"))
		logger.SetLevel(logrus.InfoLevel)
	} else {
		logger.SetLevel(level)
	}

	return logger
}

// newAWSPubSubServer creates a new grpc PubSub server using the broker
// implementation for AWS
func newAWSPubSubServer() (pubsubgrpc.PubSubServer, error) {
	sess, err := session.NewSession()
	if err != nil {
		return nil, err
	}
	// Checks to see if aws config credentials are valid
	log.Print("checking server for AWS permissions")
	if err := pubsubaws.VerifyPermissions(sess); err != nil {
		log.Fatalf("AWS permissions check failed: %v", err)
	}
	log.Print("server has proper AWS permissions")

	pubFactory := func(ctx context.Context, topic string) (pubsub.Publisher, error) {
		return pubsubaws.NewPublisher(sess, topic)
	}
	subFactory := func(ctx context.Context, topic, subscriptionID string) (pubsub.Subscriber, error) {
		return pubsubaws.NewSubscriber(sess, topic, subscriptionID)
	}
	return pubsubgrpc.NewPubSubServer(pubFactory, subFactory), nil
}

// ServeInternal builds and runs the server that listens on InternalAddress
func ServeInternal(logger *logrus.Logger) error {
	healthChecker := health.NewChecksHandler(
		viper.GetString("internal.health"),
		viper.GetString("internal.readiness"),
	)

	s, err := server.NewServer(
		// register our health checks
		server.WithHealthChecks(healthChecker),
	)
	if err != nil {
		return err
	}
	l, err := net.Listen("tcp", fmt.Sprintf("%s:%s", viper.GetString("internal.address"), viper.GetString("internal.port")))
	if err != nil {
		return err
	}

	logger.Debugf("serving internal http at %q", fmt.Sprintf("%s:%s", viper.GetString("internal.address"), viper.GetString("internal.port")))
	return s.Serve(nil, l)
}

func init() {
	pflag.Parse()
	viper.BindPFlags(pflag.CommandLine)
	viper.AutomaticEnv()
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AddConfigPath(viper.GetString("config.source"))
	if viper.GetString("config.file") != "" {
		log.Printf("Serving from configuration file: %s", viper.GetString("config.file"))
		viper.SetConfigName(viper.GetString("config.file"))
		if err := viper.ReadInConfig(); err != nil {
			log.Fatalf("cannot load configuration: %v", err)
		}
	} else {
		log.Printf("Serving from default values, environment variables, and/or flags")
	}
}
