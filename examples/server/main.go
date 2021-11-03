package main

// An example pubsub grpc server. This uses the aws message broker as its
// underlying messaging system.

import (
	"context"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/session"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	"github.com/infobloxopen/atlas-app-toolkit/health"
	"github.com/infobloxopen/atlas-app-toolkit/logging"
	"github.com/infobloxopen/atlas-app-toolkit/server"
	pubsub "github.com/infobloxopen/atlas-pubsub"
	pubsubaws "github.com/infobloxopen/atlas-pubsub/aws"
	pubsubgrpc "github.com/infobloxopen/atlas-pubsub/grpc"
)

var registeredPubSubServer = false

const pubSubNotReadyError = "PubSub Server is not ready!"

func main() {
	logger := logging.New(viper.GetString("logging.level"))
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

	// attempt to fix ENHANCE_YOUR_CALM error when using linkerd
	grpcServer := grpc.NewServer(
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{MinTime: time.Millisecond, PermitWithoutStream: true}),
		grpc.UnaryInterceptor(
			grpc_middleware.ChainUnaryServer(
				grpc_prometheus.UnaryServerInterceptor,
			),
		),
	)

	pubsubServer, err := newAWSPubSubServer(logger)
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

// newAWSPubSubServer creates a new grpc PubSub server using the broker
// implementation for AWS
func newAWSPubSubServer(logger *logrus.Logger) (pubsubgrpc.PubSubServer, error) {
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		AssumeRoleTokenProvider: stscreds.StdinTokenProvider,
		SharedConfigState:       session.SharedConfigEnable,
	}))
	// Checks to see if aws config credentials are valid
	logger.Print("checking server for AWS permissions")
	if err := pubsubaws.VerifyPermissions(sess, logger); err != nil {
		logger.Fatalf("AWS permissions check failed: %v", err)
	}
	logger.Print("server has proper AWS permissions")

	pubFactory := func(ctx context.Context, topic string) (pubsub.Publisher, error) {
		return pubsubaws.NewPublisher(sess, topic, pubsubaws.PublishWithLogger(logger))
	}
	subFactory := func(ctx context.Context, topic, subscriptionID string) (pubsub.Subscriber, error) {
		return pubsubaws.NewSubscriber(sess, topic, subscriptionID, pubsubaws.SubscribeWithLogger(logger))
	}
	return pubsubgrpc.NewPubSubServer(pubFactory, subFactory, pubsubgrpc.WithLogger(logger)), nil
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
		// register metrics
		server.WithHandler("/metrics", promhttp.Handler()),
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
		logrus.Infof("Serving from configuration file: %s", viper.GetString("config.file"))
		viper.SetConfigName(viper.GetString("config.file"))
		if err := viper.ReadInConfig(); err != nil {
			logrus.Fatalf("cannot load configuration: %v", err)
		}
	} else {
		logrus.Infof("Serving from default values, environment variables, and/or flags")
	}
}

func pubSubServerReady() error {
	if !registeredPubSubServer {
		return fmt.Errorf(pubSubNotReadyError)
	}
	return nil
}
