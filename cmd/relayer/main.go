package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/cosmos/ibc-relayer/db/gen/db"
	"github.com/cosmos/ibc-relayer/db/tx"
	"github.com/cosmos/ibc-relayer/gasmonitor"
	"github.com/cosmos/ibc-relayer/proto/gen/proofapi"
	"github.com/cosmos/ibc-relayer/relayer/ibcv2"
	"github.com/cosmos/ibc-relayer/relayerapi/server"
	"github.com/cosmos/ibc-relayer/shared/clients/coingecko"
	"github.com/cosmos/ibc-relayer/shared/config"
	"github.com/cosmos/ibc-relayer/shared/database"
	"github.com/cosmos/ibc-relayer/shared/lmt"
	"github.com/cosmos/ibc-relayer/shared/metrics"
	"github.com/cosmos/ibc-relayer/transferstats"
)

var (
	configPath          = flag.String("config", "./config/local/config.yml", "path to relayer config file")
	enableIBCV2Relaying = flag.Bool("ibcv2-relaying", true, "if ibcv2 relaying should be enabled")
	dbMigrate           = flag.Bool("db-migrate", false, "run database migrations before starting the relayer")
)

func main() {
	// `relayer migrate [flags]` is sugar for `relayer --db-migrate [flags]`
	// that exits after migrations. Strip the subcommand token so flag.Parse
	// sees the remaining flags uniformly.
	migrateMode := isMigrateSubcommand()
	if migrateMode {
		os.Args = append(os.Args[:1], os.Args[2:]...)
	}
	flag.Parse()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	lmt.ConfigureLogger()
	ctx = lmt.LoggerContext(ctx)

	promMetrics := metrics.NewPromMetrics()
	ctx = metrics.ContextWithMetrics(ctx, promMetrics)

	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		lmt.Logger(ctx).Fatal("Unable to load config", zap.Error(err))
	}
	ctx = config.ConfigReaderContext(ctx, config.NewConfigReader(cfg))

	dsn := config.GetConfigReader(ctx).GetPostgresConnString()
	pool, err := database.NewDatabase(ctx, dsn, config.GetConfigReader(ctx).PostgresIAMAuthEnabled(), config.GetConfigReader(ctx).PostgresIAMAuthRegion())
	if err != nil {
		lmt.Logger(ctx).Fatal("Unable to connect to database: %v", zap.Error(err))
	}

	if migrateMode || *dbMigrate {
		if err := runMigrations(ctx, pool); err != nil {
			lmt.Logger(ctx).Fatal("Failed to run database migrations", zap.Error(err))
		}
		lmt.Logger(ctx).Info("Database migrations applied")
		if migrateMode {
			pool.Close()
			return
		}
	}

	var ibcv2ClientManager ibcv2.BridgeClientManager
	var ibcv2ChainIDToPrivateKey map[string]string
	var signerConn *grpc.ClientConn

	signing := cfg.Signing
	switch {
	case signing.GRPCAddress != "":
		lmt.Logger(ctx).Info("Remote signer configured for Relayer",
			zap.String("grpc_address", signing.GRPCAddress),
			zap.String("cosmos_wallet_id", signing.CosmosWalletKey),
			zap.String("evm_wallet_id", signing.EVMWalletKey))

		var signerOpts []grpc.DialOption
		if !signing.GRPCTLSEnabled {
			signerOpts = append(signerOpts, grpc.WithTransportCredentials(insecure.NewCredentials())) // nosemgrep: go.grpc.tls.grpc-client-new-insecure-connection.grpc-client-new-insecure-connection
		} else {
			signerOpts = append(signerOpts, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{InsecureSkipVerify: true, MinVersion: tls.VersionTLS13}))) //nolint:gosec // signer is an internal service
		}

		conn, err := grpc.NewClient(signing.GRPCAddress, signerOpts...)
		if err != nil {
			lmt.Logger(ctx).Fatal("failed to connect to remote signer",
				zap.String("address", signing.GRPCAddress),
				zap.Error(err))
		}
		signerConn = conn
		defer signerConn.Close()
	case signing.KeysPath != "":
		keys, err := LoadChainIDToPrivateKeyMap(signing.KeysPath)
		if err != nil {
			lmt.Logger(ctx).Fatal("Failed to load chain id -> private key map for ibcv2", zap.Error(err))
		}
		ibcv2ChainIDToPrivateKey = keys
		lmt.Logger(ctx).Info("Using local keys for signing", zap.String("keys_path", signing.KeysPath))
	default:
		lmt.Logger(ctx).Fatal("No signing configuration: set either signing.grpc_address or signing.keys_path")
	}

	ibcv2ClientManager, err = ibcv2.NewClientManagerFromConfig(
		ctx,
		ibcv2ChainIDToPrivateKey,
		signerConn,
		signing.CosmosWalletKey,
		signing.EVMWalletKey,
		config.GetConfigReader(ctx).GetIBCV2Chains()...,
	)
	if err != nil {
		lmt.Logger(ctx).Fatal("error creating ibcv2 client manager from config", zap.Error(err))
	}

	var coingeckoClient ibcv2.PriceClient
	coingeckoConfig := config.GetConfigReader(ctx).GetCoingeckoConfig()
	if coingeckoConfig.APIKey != "" {
		coingeckoClient = coingecko.NewCachedPriceClient(coingecko.DefaultCoingeckoClient(coingeckoConfig), coingeckoConfig.CacheRefreshInterval)
	}

	eg, ctx := errgroup.WithContext(ctx)

	eg.Go(func() error {
		lmt.Logger(ctx).Info("Starting Prometheus")
		return metrics.StartPrometheus(ctx, cfg.Metrics.PrometheusAddress)
	})

	eg.Go(func() error {
		apiConfig := config.GetConfigReader(ctx).GetRelayerAPIConfig()
		grpcServer, err := server.NewRelayerGRPCServer(ctx, pool, ibcv2ClientManager, apiConfig.Address)
		if err != nil {
			return err
		}
		grpcServer.Start(ctx)
		return nil
	})

	eg.Go(func() error {
		gasMonitor := gasmonitor.NewGasMonitor(ibcv2ClientManager)
		err := gasMonitor.Start(ctx)
		if err != nil {
			return fmt.Errorf("creating gas monitor: %w", err)
		}
		return nil
	})

	// create connection to proof relayer
	proofRelayerConfig := config.GetConfigReader(ctx).GetIBCV2ProofRelayerConfig()

	var opts []grpc.DialOption
	if !proofRelayerConfig.GRPCTLSEnabled {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials())) // nosemgrep: go.grpc.tls.grpc-client-new-insecure-connection.grpc-client-new-insecure-connection
	} else {
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{InsecureSkipVerify: true, MinVersion: tls.VersionTLS13}))) //nolint:gosec // proof relayer is an internal service
	}
	opts = append(opts, grpc.WithChainUnaryInterceptor(metrics.UnaryClientInterceptor, proofAPITimeoutInterceptor(proofRelayerConfig.Timeout)))

	opts = append(opts, grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(1024*1024*10)))

	conn, err := grpc.NewClient(proofRelayerConfig.GRPCAddress, opts...)
	if err != nil {
		lmt.Logger(ctx).Fatal(
			"error creating grpc connection to proof relayer",
			zap.String("address", proofRelayerConfig.GRPCAddress),
			zap.Error(err),
		)
	}

	relayer := proofapi.NewProofApiServiceClient(conn)
	defer conn.Close()

	// create storage for ibcv2 transactions
	storage := tx.New(db.New(pool), pool)

	// create a pipeline manager to create new pipelines for new packet
	// transfer paths
	manager := ibcv2.NewIBCV2PipelineManager(storage, ibcv2ClientManager, relayer, coingeckoClient)

	// create relay dispatcher to submit relays to the pipeline from storage
	dispatcher := ibcv2.NewRelayDispatcher(storage, 5*time.Second, manager, *enableIBCV2Relaying)

	eg.Go(func() error {
		if err := dispatcher.Run(ctx); err != nil {
			return fmt.Errorf("running ibcv2 relayer: %w", err)
		}
		return nil
	})

	eg.Go(func() error {
		if err := transferstats.New(storage).Start(ctx); err != nil {
			return fmt.Errorf("running transfer stats monitor: %w", err)
		}
		return nil
	})

	eg.Go(func() error {
		tracker := ibcv2.NewSubmittedTxCostTracker(storage, ibcv2ClientManager, coingeckoClient, 5*time.Second, 100, 6*time.Hour)
		if err := tracker.Run(ctx); err != nil {
			return fmt.Errorf("running submitted tx cost tracker: %w", err)
		}
		return nil
	})

	if err := eg.Wait(); err != nil {
		lmt.Logger(ctx).Fatal("Error running Relayer", zap.Error(err))
	}
}

func LoadChainIDToPrivateKeyMap(keysPath string) (map[string]string, error) {
	keysBytes, err := os.ReadFile(keysPath)
	if err != nil {
		return nil, err
	}

	rawKeysMap := make(map[string]map[string]string)
	if err := json.Unmarshal(keysBytes, &rawKeysMap); err != nil {
		return nil, err
	}

	keysMap := make(map[string]string)
	for key, value := range rawKeysMap {
		keysMap[key] = value["private_key"]
	}

	return keysMap, nil
}

func proofAPITimeoutInterceptor(timeout *time.Duration) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, request, reply any, conn *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if timeout == nil {
			return invoker(ctx, method, request, reply, conn, opts...)
		}

		deadlineCtx, cancel := context.WithTimeout(ctx, *timeout)
		defer cancel()

		return invoker(deadlineCtx, method, request, reply, conn, opts...)
	}
}
