package replica

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"time"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/relab/gorums"
	backend "github.com/relab/hotstuff/backend/gorums"
	"github.com/relab/hotstuff/blockchain"
	"github.com/relab/hotstuff/config"
	"github.com/relab/hotstuff/consensus"
	"github.com/relab/hotstuff/consensus/chainedhotstuff"
	"github.com/relab/hotstuff/consensus/fasthotstuff"
	"github.com/relab/hotstuff/crypto"
	"github.com/relab/hotstuff/crypto/bls12"
	"github.com/relab/hotstuff/crypto/ecdsa"
	"github.com/relab/hotstuff/crypto/keygen"
	"github.com/relab/hotstuff/internal/logging"
	"github.com/relab/hotstuff/internal/proto/orchestrationpb"
	"github.com/relab/hotstuff/leaderrotation"
	"github.com/relab/hotstuff/synchronizer"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// cmdID is a unique identifier for a command
type cmdID struct {
	clientID    uint32
	sequenceNum uint64
}

type Replica struct {
	output io.Writer
	*clientSrv
	cfg      *backend.Config
	hsSrv    *backend.Server
	hs       *consensus.Modules
	cmdCache *cmdCache

	mut          sync.Mutex
	execHandlers map[cmdID]func(*empty.Empty, error)

	lastExecTime int64
}

func getCertificate(conf *orchestrationpb.ReplicaRunConfig) (*tls.Certificate, error) {
	if conf.GetUseTLS() && conf.GetCertificate() != nil {
		var key []byte
		if conf.GetCertificateKey() != nil {
			key = conf.GetCertificateKey()
		} else {
			key = conf.GetPrivateKey()
		}
		cert, err := tls.X509KeyPair(conf.GetCertificate(), key)
		if err != nil {
			return nil, err
		}
		return &cert, nil
	}
	return nil, nil
}

func getConfiguration(conf *orchestrationpb.ReplicaRunConfig) (*config.ReplicaConfig, error) {
	pk, err := keygen.ParsePrivateKey(conf.GetPrivateKey())
	if err != nil {
		return nil, err
	}

	var creds credentials.TransportCredentials
	if conf.GetUseTLS() {
		cert, err := getCertificate(conf)
		if err != nil {
			return nil, err
		}
		cp := x509.NewCertPool()
		cp.AppendCertsFromPEM(conf.GetCertificateAuthority())
		creds = credentials.NewTLS(&tls.Config{
			Certificates: []tls.Certificate{*cert},
			RootCAs:      cp,
			ClientAuth:   tls.RequireAndVerifyClientCert,
		})
	}

	cfg := &config.ReplicaConfig{
		ID:         consensus.ID(conf.GetID()),
		PrivateKey: pk,
		Creds:      creds,
	}

	for _, replica := range conf.GetReplicas() {
		pubKey, err := keygen.ReadPublicKeyFile(string(replica.GetPublicKey()))
		if err != nil {
			return nil, err
		}
		cfg.Replicas[consensus.ID(replica.GetID())] = &config.ReplicaInfo{
			ID:      consensus.ID(replica.GetID()),
			Address: replica.GetAddress(),
			PubKey:  pubKey,
		}
	}
	return cfg, nil
}

// New returns a new replica.
func New(conf *orchestrationpb.ReplicaRunConfig) (replica *Replica, err error) {
	serverOpts := []gorums.ServerOption{}
	grpcServerOpts := []grpc.ServerOption{}

	cert, err := getCertificate(conf)
	if err != nil {
		return nil, err
	}
	if cert != nil {
		grpcServerOpts = append(grpcServerOpts, grpc.Creds(credentials.NewServerTLSFromCert(cert)))
	}

	serverOpts = append(serverOpts, gorums.WithGRPCServerOptions(grpcServerOpts...))
	clientSrv, err := newClientServer(conf, serverOpts)
	if err != nil {
		return nil, err
	}
	srv := &Replica{
		clientSrv:    clientSrv,
		cmdCache:     newCmdCache(int(conf.GetBatchSize())),
		execHandlers: make(map[cmdID]func(*empty.Empty, error)),
		lastExecTime: time.Now().UnixNano(),
	}

	replicaConfig, err := getConfiguration(conf)
	if err != nil {
		return nil, err
	}

	var consensusImpl consensus.Consensus
	switch conf.GetConsensus() {
	case "chainedhotstuff":
		consensusImpl = chainedhotstuff.New()
	case "fasthotstuff":
		consensusImpl = fasthotstuff.New()
	default:
		fmt.Fprintf(os.Stderr, "Invalid consensus type: '%s'\n", conf.GetConsensus())
		os.Exit(1)
	}

	var cryptoImpl consensus.CryptoImpl
	switch conf.Crypto {
	case "ecdsa":
		cryptoImpl = ecdsa.New()
	case "bls12":
		cryptoImpl = bls12.New()
	default:
		fmt.Fprintf(os.Stderr, "Invalid crypto type: '%s'\n", conf.Crypto)
		os.Exit(1)
	}

	srv.cfg = backend.NewConfig(*replicaConfig)
	srv.hsSrv = backend.NewServer(*replicaConfig)

	builder := consensus.NewBuilder(replicaConfig.ID, replicaConfig.PrivateKey)

	builder.Register(
		srv.cfg,
		srv.hsSrv,
		consensusImpl,
		crypto.NewCache(cryptoImpl, 2*srv.cfg.Len()),
		leaderrotation.NewRoundRobin(),
		srv,          // executor
		srv.cmdCache, // acceptor and command queue
		synchronizer.New(synchronizer.NewViewDuration(
			uint64(conf.TimeoutSamples), float64(conf.InitialTimeout), float64(conf.TimeoutMultiplier)),
		),
		blockchain.New(int(conf.BlockCacheSize)),
		logging.New(fmt.Sprintf("hs%d", conf.GetID())),
	)
	srv.hs = builder.Build()

	return srv, nil
}

// Run runs the replica until the context is cancelled.
func (srv *Replica) Run(ctx context.Context, address string) (err error) {
	err = srv.hsSrv.Start()
	if err != nil {
		return err
	}

	err = srv.cfg.Connect(10 * time.Second)
	if err != nil {
		return err
	}

	c := make(chan struct{})
	go func() {
		srv.hs.Synchronizer().Start(ctx)
		srv.hs.EventLoop().Run(ctx)
		close(c)
	}()

	err = srv.clientSrv.Start(address)
	if err != nil {
		log.Println(err)
	}

	// wait for the event loop to exit
	<-c

	srv.stop()
	return nil
}

func (srv *Replica) stop() {
	srv.clientSrv.Stop()
	srv.cfg.Close()
	srv.hsSrv.Stop()
	if closer, ok := srv.output.(io.Closer); ok {
		err := closer.Close()
		if err != nil {
			log.Println("error closing output: ", err)
		}
	}
}