package main

// An example pubsub grpc server. This uses the aws message broker as its
// underlying messaging system.

import (
	"context"
	"fmt"
	"log"
	"net"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
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

var registeredPubSubServer = false

const pubSubNotReadyError = "PubSub Server is not ready!"

func main() {
	logger := NewLogger()
	if viper.GetBool("internal.enable") {
		go func() {
			if err := ServeInternal(logger); err != nil {
				logger.Fatal(err)
			}
		}()
	}

	logger.Printf("starting aws pubsub server on port %s", viper.GetString("server.port"))
	lis, err := net.Listen("tcp", fmt.Sprintf("%s:%s", viper.GetString("server.address"), viper.GetString("server.port")))
	if err != nil {
		logger.Fatalf("failed to listen: %v", err)
	}
	grpcServer := grpc.NewServer()

	pubsubServer, err := getPubSubServer(logger)
	if err != nil {
		logger.Fatalf("failed to create aws pubsub server: %v", err)
	}
	pubsubgrpc.RegisterPubSubServer(grpcServer, pubsubServer)

	// We just setting global variable, because no race condition consequence is unacceptable
	registeredPubSubServer = true

	if err := grpcServer.Serve(lis); err != nil {
		logger.Fatalf("failed to serve: %v", err)
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

func getPubSubServer(logger *logrus.Logger) (pubsubgrpc.PubSubServer, error) {

	snsEndpoint := viper.GetString("sns.endpoint")
	if snsEndpoint != "" { // Assuming sqsEndpoint also set
		return newLocalStackPubSubServer(logger)
	}
	return newAWSPubSubServer(logger)
}

// newAWSPubSubServer creates a new grpc PubSub server using the broker
// implementation for AWS
func newAWSPubSubServer(logger *logrus.Logger) (pubsubgrpc.PubSubServer, error) {
	sess, err := session.NewSession()
	if err != nil {
		return nil, err
	}
	// Checks to see if aws config credentials are valid
	logger.Print("checking server for AWS permissions")
	if err := pubsubaws.VerifyPermissions(sess); err != nil {
		logger.Fatalf("AWS permissions check failed: %v", err)
	}
	logger.Print("server has proper AWS permissions")

	pubFactory := func(ctx context.Context, topic string) (pubsub.Publisher, error) {
		return pubsubaws.NewPublisher(sess, topic)
	}
	subFactory := func(ctx context.Context, topic, subscriptionID string) (pubsub.Subscriber, error) {
		return pubsubaws.NewSubscriber(sess, sess, topic, subscriptionID)
	}
	return pubsubgrpc.NewPubSubServer(pubFactory, subFactory), nil
}

// newLocalStackPubSubServer creates a new grpc PubSub server using the broker
// implementation for LocalStack
func newLocalStackPubSubServer(logger *logrus.Logger) (pubsubgrpc.PubSubServer, error) {
	cfg := aws.Config{}
	// Use below line for debugging purpose
	// cfg := aws.Config{LogLevel: aws.LogLevel(aws.LogDebugWithHTTPBody)}
	snsEndpoint := viper.GetString("sns.endpoint")
	sqsEndpoint := viper.GetString("sqs.endpoint")

	snsSession, err := session.NewSession(cfg.WithEndpoint(snsEndpoint))
	if err != nil {
		logger.Errorf("Could not create session with SNS Endpoint: %s", snsEndpoint)
		return nil, err
	}
	sqsSession, err := session.NewSession(cfg.WithEndpoint(sqsEndpoint))
	if err != nil {
		logger.Errorf("Could not create session with SQS Endpoint: %s", sqsEndpoint)
		return nil, err
	}
	logger.Printf("SNS Endpoint: %s & SQS Endpoint: %s used to create pubsub server", snsEndpoint, sqsEndpoint)

	pubFactory := func(ctx context.Context, topic string) (pubsub.Publisher, error) {
		return pubsubaws.NewPublisher(snsSession, topic)
	}
	subFactory := func(ctx context.Context, topic, subscriptionID string) (pubsub.Subscriber, error) {
		return pubsubaws.NewSubscriber(snsSession, sqsSession, topic, subscriptionID)
	}
	return pubsubgrpc.NewPubSubServer(pubFactory, subFactory), nil
}

// ServeInternal builds and runs the server that listens on InternalAddress
func ServeInternal(logger *logrus.Logger) error {
	healthChecker := health.NewChecksHandler(
		viper.GetString("internal.health"),
		viper.GetString("internal.readiness"),
	)

	healthChecker.AddReadiness("PubSub Server ready check", pubSubServerReady)

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

func pubSubServerReady() error {
	if !registeredPubSubServer {
		return fmt.Errorf(pubSubNotReadyError)
	}
	return nil
}
