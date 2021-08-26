package main

import (
	"context"
	"os"
	"os/signal"
	agonescluster "quilkin.dev/xds-management-server/pkg/cluster/agones"
	k8sfilterchain "quilkin.dev/xds-management-server/pkg/filterchain/k8s"
	"quilkin.dev/xds-management-server/pkg/k8s"
	"time"

	"github.com/alecthomas/kong"
	log "github.com/sirupsen/logrus"

	"quilkin.dev/xds-management-server/pkg/server"
	"quilkin.dev/xds-management-server/pkg/snapshot"
)

type flags struct {
	KubeHost                string        `name:"kube-host" env:"KUBE_HOST" help:"The host the kubernetes API server runs on or 'cluster' if running in cluster or 'local' to use local kube config" default:"cluster"`
	Port                    int16         `name:"int16" help:"Server listening port." default:"18000"`
	ProxyNamespace          string        `name:"proxy-namespace" help:"Namespace under which the proxies run." default:"quilkin"`
	GameServersNamespace    string        `name:"game-server-namespace" help:"Namespace under which the game-servers run." default:"gameservers"`
	GameServersPollInterval time.Duration `name:"game-server-poll-interval" help:"How long to wait in-between checking for game-server updates." default:"1s"`
	ProxyPollInterval       time.Duration `name:"proxy-interval" help:"How long to wait in-between checking for proxy updates." default:"1s"`
}

func main() {
	var flags flags
	kong.Parse(&flags)

	logger := &log.Logger{}
	logger.SetOutput(os.Stdout)
	logger.SetLevel(log.DebugLevel)
	logger.SetFormatter(&log.JSONFormatter{})

	ctx, shutdown := context.WithCancel(context.Background())
	defer shutdown()

	k8sConfig, err := k8s.GetK8sConfig(logger, flags.KubeHost)
	if err != nil {
		log.WithError(err).Fatal("failed to get kube config")
	}

	k8sClient, err := k8s.CreateClient(k8sConfig)
	if err != nil {
		log.WithError(err).Fatal("failed to create k8s client")
	}

	clusterProvider, err := agonescluster.NewProvider(logger, agonescluster.Config{
		K8sConfig:               k8sConfig,
		GameServersNamespace:    flags.GameServersNamespace,
		GameServersPollInterval: flags.GameServersPollInterval,
	})
	if err != nil {
		log.WithError(err).Fatal("failed to create Agones cluster provider")
	}

	filterChainProvider, err := k8sfilterchain.NewProvider(ctx, logger, k8sClient, flags.ProxyNamespace)
	if err != nil {
		log.WithError(err).Fatal("failed to create k8s filter-chain provider")
	}

	clusterCh, err := clusterProvider.Run(ctx)
	if err != nil {
		log.WithError(err).Fatal("failed to create start cluster provider")
	}

	filterChainCh := filterChainProvider.Run(ctx, flags.ProxyPollInterval)

	snapshotCache := snapshot.RunSnapshotUpdater(
		ctx,
		logger,
		clusterCh,
		filterChainCh,
		100*time.Millisecond)

	if err := server.Run(ctx, logger, flags.Port, snapshotCache, nil); err != nil {
		logger.WithError(err).Fatal("failed to start server")
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	select {
	case <-c:
		logger.Info("Received shutdown signal. Shutting down.")
		shutdown()
	case <-ctx.Done():
		logger.Info("Shutdown.")
	}
}