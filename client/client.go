// Package client implements a simple client for testing HotStuff.
// The client reads data from an input stream and sends the data in commands to a HotStuff replica.
// The client waits for replies from f+1 replicas before it considers a command to be executed.
package client

import (
	"context"
	"crypto/x509"
	"errors"
	"io"
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/relab/gorums"
	"github.com/relab/hotstuff"
	"github.com/relab/hotstuff/config"
	"github.com/relab/hotstuff/internal/logging"
	"github.com/relab/hotstuff/internal/proto/clientpb"
	"github.com/relab/hotstuff/modules"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type qspec struct {
	faulty int
}

func (q *qspec) ExecCommandQF(_ *clientpb.Command, signatures map[uint32]*empty.Empty) (*empty.Empty, bool) {
	if len(signatures) < q.faulty+1 {
		return nil, false
	}
	return &empty.Empty{}, true
}

type pendingCmd struct {
	sequenceNumber uint64
	sendTime       time.Time
	promise        *clientpb.AsyncEmpty
}

// Config contains config options for a client.
type Config struct {
	ID               hotstuff.ID
	TLS              bool
	RootCAs          *x509.CertPool
	MaxConcurrent    uint32
	PayloadSize      uint32
	Input            io.ReadCloser
	ManagerOptions   []gorums.ManagerOption
	RateLimit        float64       // initial rate limit
	RateStep         float64       // rate limit step up
	RateStepInterval time.Duration // step up interval
}

// Client is a hotstuff client.
type Client struct {
	mut              sync.Mutex
	id               hotstuff.ID
	mods             *modules.Modules
	mgr              *clientpb.Manager
	gorumsConfig     *clientpb.Configuration
	payloadSize      uint32
	highestCommitted uint64 // highest sequence number acknowledged by the replicas
	pendingCmds      chan pendingCmd
	cancel           context.CancelFunc
	done             chan struct{}
	reader           io.ReadCloser
	limiter          *rate.Limiter
	stepUp           float64
	stepUpInterval   time.Duration
}

// New returns a new Client.
func New(conf Config, builder modules.Builder) (client *Client) {
	builder.Register(logging.New("cli" + strconv.Itoa(int(conf.ID))))
	mods := builder.Build()

	client = &Client{
		id:               conf.ID,
		mods:             mods,
		pendingCmds:      make(chan pendingCmd, conf.MaxConcurrent),
		highestCommitted: 1,
		done:             make(chan struct{}),
		reader:           conf.Input,
		limiter:          rate.NewLimiter(rate.Limit(conf.RateLimit), 1),
		stepUp:           conf.RateStep,
		stepUpInterval:   conf.RateStepInterval,
	}

	grpcOpts := []grpc.DialOption{grpc.WithBlock()}

	if conf.TLS {
		grpcOpts = append(grpcOpts, grpc.WithTransportCredentials(credentials.NewClientTLSFromCert(conf.RootCAs, "")))
	} else {
		grpcOpts = append(grpcOpts, grpc.WithInsecure())
	}

	opts := conf.ManagerOptions
	opts = append(opts, gorums.WithGrpcDialOptions(grpcOpts...))

	client.mgr = clientpb.NewManager(opts...)

	return client
}

// Connect connects the client to the replicas.
func (c *Client) Connect(replicaConfig *config.ReplicaConfig) (err error) {
	nodes := make(map[string]uint32, len(replicaConfig.Replicas))
	for _, r := range replicaConfig.Replicas {
		nodes[r.Address] = uint32(r.ID)
	}
	c.gorumsConfig, err = c.mgr.NewConfiguration(&qspec{faulty: hotstuff.NumFaulty(len(replicaConfig.Replicas))}, gorums.WithNodeMap(nodes))
	if err != nil {
		c.mgr.Close()
		return err
	}
	return nil
}

// Run runs the client until the context is closed.
func (c *Client) Run(ctx context.Context) {
	eventLoopDone := make(chan struct{})
	go func() {
		c.mods.MetricsEventLoop().Run(ctx)
		close(eventLoopDone)
	}()
	c.mods.Logger().Info("Starting to send commands")

	commandStatsChan := make(chan struct{ executed, failed int })
	// start the command handler
	go func() {
		executed, failed := c.handleCommands(ctx)
		commandStatsChan <- struct {
			executed int
			failed   int
		}{executed, failed}
	}()

	err := c.sendCommands(ctx)
	if err != nil && !errors.Is(err, io.EOF) {
		c.mods.Logger().Panicf("Failed to send commands: %v", err)
	}
	c.close()

	stats := <-commandStatsChan
	c.mods.Logger().Infof("Done sending commands (executed: %d, failed: %d)", stats.executed, stats.failed)
	<-eventLoopDone
	close(c.done)
}

// Start starts the client.
func (c *Client) Start() {
	var ctx context.Context
	ctx, c.cancel = context.WithCancel(context.Background())
	go c.Run(ctx)
}

// Stop stops the client.
func (c *Client) Stop() {
	c.cancel()
	<-c.done
}

func (c *Client) close() {
	c.mgr.Close()
	err := c.reader.Close()
	if err != nil {
		c.mods.Logger().Warn("Failed to close reader: ", err)
	}
}

func (c *Client) sendCommands(ctx context.Context) error {
	var (
		num         uint64 = 1
		lastCommand uint64 = math.MaxUint64
		lastStep           = time.Now()
	)

loop:
	for {
		if ctx.Err() != nil {
			break
		}

		// step up the rate limiter
		now := time.Now()
		if now.Sub(lastStep) > c.stepUpInterval {
			c.limiter.SetLimit(c.limiter.Limit() + rate.Limit(c.stepUp))
			lastStep = now
		}

		err := c.limiter.Wait(ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			return err
		}

		// annoyingly, we need a mutex here to prevent the data race detector from complaining.
		c.mut.Lock()
		shouldStop := lastCommand <= c.highestCommitted
		c.mut.Unlock()

		if shouldStop {
			break
		}

		data := make([]byte, c.payloadSize)
		n, err := c.reader.Read(data)
		if err != nil && err != io.EOF {
			// if we get an error other than EOF
			return err
		} else if err == io.EOF && n == 0 && lastCommand > num {
			lastCommand = num
			c.mods.Logger().Info("Reached end of file. Sending empty commands until last command is executed...")
		}

		cmd := &clientpb.Command{
			ClientID:       uint32(c.id),
			SequenceNumber: num,
			Data:           data[:n],
		}

		promise := c.gorumsConfig.ExecCommand(ctx, cmd)

		num++
		select {
		case c.pendingCmds <- pendingCmd{sequenceNumber: num, sendTime: time.Now(), promise: promise}:
		case <-ctx.Done():
			break loop
		}

		if num%100 == 0 {
			c.mods.Logger().Infof("%d commands sent", num)
		}

	}
	return nil
}

// handleCommands will get pending commands from the pendingCmds channel and then
// handle them as they become acknowledged by the replicas. We expect the commands to be
// acknowledged in the order that they were sent.
func (c *Client) handleCommands(ctx context.Context) (executed, failed int) {
	for {
		var (
			cmd pendingCmd
			ok  bool
		)
		select {
		case cmd, ok = <-c.pendingCmds:
			if !ok {
				return
			}
		case <-ctx.Done():
			return
		}
		_, err := cmd.promise.Get()
		if err != nil {
			qcError, ok := err.(gorums.QuorumCallError)
			if !ok || qcError.Reason != context.Canceled.Error() {
				c.mods.Logger().Debugf("Did not get enough replies for command: %v\n", err)
				failed++
			}
		} else {
			executed++
		}
		c.mut.Lock()
		if cmd.sequenceNumber > c.highestCommitted {
			c.highestCommitted = cmd.sequenceNumber
		}
		c.mut.Unlock()

		duration := time.Since(cmd.sendTime)
		c.mods.MetricsEventLoop().AddEvent(LatencyMeasurementEvent{Latency: duration})
	}
}

// LatencyMeasurementEvent represents a single latency measurement.
type LatencyMeasurementEvent struct {
	Latency time.Duration
}
