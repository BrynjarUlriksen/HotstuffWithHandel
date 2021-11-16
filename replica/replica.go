// Package replica provides the required code for starting and running a replica and handling client requests.
package replica

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net"
	"strconv"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/relab/gorums"
	"github.com/relab/hotstuff"
	backend "github.com/relab/hotstuff/backend/gorums"
	"github.com/relab/hotstuff/config"
	"github.com/relab/hotstuff/consensus"
	"github.com/relab/hotstuff/internal/logging"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// cmdID is a unique identifier for a command
type cmdID struct {
	clientID    uint32
	sequenceNum uint64
}

// Config configures a replica.
type Config struct {

	//Ids of other replicas
	IDList []uint32

	// The id of the replica.
	ID hotstuff.ID
	// The private key of the replica.
	PrivateKey consensus.PrivateKey
	// Controls whether TLS is used.
	TLS bool
	// The TLS certificate.
	Certificate *tls.Certificate
	// The root certificates trusted by the replica.
	RootCAs *x509.CertPool
	// The number of client commands that should be batched together in a block.
	BatchSize uint32
	// Options for the client server.
	ClientServerOptions []gorums.ServerOption
	// Options for the replica server.
	ReplicaServerOptions []gorums.ServerOption
	// Options for the replica manager.
	ManagerOptions []gorums.ManagerOption
}

// Replica is a participant in the consensus protocol.
type Replica struct {
	clientSrv  *clientSrv
	cfg        *backend.Config
	hsSrv      *backend.Server
	hs         *consensus.Modules
	BinaryTree *[][]uint32

	execHandlers map[cmdID]func(*empty.Empty, error)
	cancel       context.CancelFunc
	done         chan struct{}
}

// New returns a new replica.
func New(conf Config, builder consensus.Builder) (replica *Replica) {
	IdList := conf.IDList
	binaryTreegroups := MakeBinaryTree(IdList, uint32(conf.ID))

	clientSrvOpts := conf.ClientServerOptions

	if conf.TLS {
		clientSrvOpts = append(clientSrvOpts, gorums.WithGRPCServerOptions(
			grpc.Creds(credentials.NewServerTLSFromCert(conf.Certificate)),
		))
	}

	clientSrv := newClientServer(conf, clientSrvOpts)

	srv := &Replica{
		clientSrv:    clientSrv,
		execHandlers: make(map[cmdID]func(*empty.Empty, error)),
		cancel:       func() {},
		done:         make(chan struct{}),
	}

	replicaSrvOpts := conf.ReplicaServerOptions
	if conf.TLS {
		replicaSrvOpts = append(replicaSrvOpts, gorums.WithGRPCServerOptions(
			grpc.Creds(credentials.NewTLS(&tls.Config{
				Certificates: []tls.Certificate{*conf.Certificate},
				ClientCAs:    conf.RootCAs,
				ClientAuth:   tls.RequireAndVerifyClientCert,
			})),
		))
	}

	srv.hsSrv = backend.NewServer(replicaSrvOpts...)

	var creds credentials.TransportCredentials
	managerOpts := conf.ManagerOptions
	if conf.TLS {
		creds = credentials.NewTLS(&tls.Config{
			RootCAs:      conf.RootCAs,
			Certificates: []tls.Certificate{*conf.Certificate},
		})
	}
	srv.cfg = backend.NewConfig(conf.ID, creds, managerOpts...)

	builder.Register(
		srv.cfg,                // configuration
		srv.hsSrv,              // event handling
		srv.clientSrv,          // executor
		srv.clientSrv.cmdCache, // acceptor and command queue
		logging.New("hs"+strconv.Itoa(int(conf.ID))),
	)
	srv.hs = builder.Build()
	srv.BinaryTree = &binaryTreegroups
	return srv
}

// creates binary tree
func MakeBinaryTree(nodes []uint32, yourID uint32) [][]uint32 {
	groupSize := 2
	levelTree := make(map[int][][]uint32)

	levelCounter := 0
	for groupSize <= len(nodes) {
		currentLevel := SplitToSubarrays(nodes, groupSize)
		levelTree[levelCounter] = currentLevel
		groupSize = groupSize * 2
		levelCounter++

	}
	// leveltree created. now we create an array node
	//can follow while communicating
	var myTree [][]uint32
	poppedElements := []uint32{yourID}
	for level := 0; level <= levelCounter; level++ {
		currentLevel := levelTree[level]

		for _, group := range currentLevel {
			var groupCopy = make([]uint32, len(group))
			copy(groupCopy, group)
			if numberInSlice(yourID, groupCopy) {
				groupWithoutElements := popElement(poppedElements,
					groupCopy)
				myTree = append(myTree, groupWithoutElements)
				poppedElements = append(poppedElements, groupCopy...)
			}
		}
	}
	return myTree
}

// splits nodes in group based on groupsize
func SplitToSubarrays(nodes []uint32, size int) [][]uint32 {
	var subarrays [][]uint32
	var j int
	for i := 0; i < len(nodes); i += size {
		j += size
		if j > len(nodes) {
			j = len(nodes)
		}
		subarrays = append(subarrays, nodes[i:j])

	}
	return subarrays
}

// pops element from slice
func popElement(poppedElements []uint32, group []uint32) []uint32 {
	for _, element := range poppedElements {
		for i, b := range group {
			if b == element {
				group[i] = group[len(group)-1]
				group = group[:len(group)-1]
			}
		}
	}
	return group
}

// checks if a number exist in a slice
func numberInSlice(yourID uint32, list []uint32) bool {
	for _, b := range list {
		if b == yourID {
			return true
		}
	}
	return false
}

// StartServers starts the client and replica servers.
func (srv *Replica) StartServers(replicaListen, clientListen net.Listener) {
	srv.hsSrv.StartOnListener(replicaListen)
	srv.clientSrv.StartOnListener(clientListen)
}

// Connect connects to the other replicas.
func (srv *Replica) Connect(replicas *config.ReplicaConfig) error {
	return srv.cfg.Connect(replicas)
}

// Start runs the replica in a goroutine.
func (srv *Replica) Start() {
	var ctx context.Context
	ctx, srv.cancel = context.WithCancel(context.Background())
	go func() {
		srv.Run(ctx)
		close(srv.done)
	}()
}

// Stop stops the replica and closes connections.
func (srv *Replica) Stop() {
	srv.cancel()
	<-srv.done
	srv.Close()
}

// Run runs the replica until the context is cancelled.
func (srv *Replica) Run(ctx context.Context) {
	srv.hs.Synchronizer().Start(ctx)
	srv.hs.Run(ctx)
}

// Close closes the connections and stops the servers used by the replica.
func (srv *Replica) Close() {
	srv.clientSrv.Stop()
	srv.cfg.Close()
	srv.hsSrv.Stop()
}

// GetHash returns the hash of all executed commands.
func (srv *Replica) GetHash() (b []byte) {
	return srv.clientSrv.hash.Sum(b)
}
