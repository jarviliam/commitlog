package server_test

import (
	"context"
	"io/ioutil"
	"net"
	"testing"

	api "github.com/jarviliam/commitlog/api/v1"
	"github.com/jarviliam/commitlog/internal/auth"
	"github.com/jarviliam/commitlog/internal/config"
	"github.com/jarviliam/commitlog/internal/log"
	"github.com/jarviliam/commitlog/internal/server"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
)

func TestServer(t *testing.T) {
	for scenario, fn := range map[string]func(t *testing.T, client api.LogClient, nobodyClient api.LogClient, config *server.Config){
		"produce/consume a message via stream": testProduceConsumeStream,
		"produce/consume a message":            testProduceConsume,
		"consume past fails":                   testConsumePastBoundary,
		"unauthorized fails":                   testUnauthorized,
	} {
		t.Run(scenario, func(t *testing.T) {
			client, nobodyClient, config, teardown := setupTest(t, nil)
			defer teardown()
			fn(t, client, nobodyClient, config)
		})
	}
}

func setupTest(t *testing.T, fn func(*server.Config)) (client api.LogClient, nobodyClient api.LogClient, cfg *server.Config, teardown func()) {
	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	newClient := func(crtPath, keyPath string) (*grpc.ClientConn, api.LogClient, []grpc.DialOption) {

		tlsConfig, err := config.SetupTLSConfig(config.TLSConfig{CAFile: config.CAFile,
			CertFile: crtPath, KeyFile: keyPath})
		require.NoError(t, err)

		clientCreds := credentials.NewTLS(tlsConfig)
		opt := []grpc.DialOption{grpc.WithTransportCredentials(clientCreds)}
		conn, err := grpc.Dial(l.Addr().String(), opt...)
		require.NoError(t, err)
		client := api.NewLogClient(conn)
		return conn, client, opt
	}
	var rootConn *grpc.ClientConn
	rootConn, client, _ = newClient(config.ClientCert, config.ClientKey)
	var nobodyConn *grpc.ClientConn
	nobodyConn, nobodyClient, _ = newClient(config.NobodyCert, config.NobodyKey)

	serverTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		ServerAddress: l.Addr().String(),
		CAFile:        config.CAFile,
		KeyFile:       config.ServerKey,
		CertFile:      config.ServerCert,
		Server:        true,
	})
	require.NoError(t, err)
	serverCreds := credentials.NewTLS(serverTLSConfig)

	dir, err := ioutil.TempDir("", "server-test")
	require.NoError(t, err)

	clog, err := log.NewLog(dir, log.Config{})
	require.NoError(t, err)

	authorizer := auth.New(config.ACLModelFile, config.ACLPolicyFile)
	cfg = &server.Config{
		CommitLog:  clog,
		Authorizer: authorizer,
	}
	if fn != nil {
		fn(cfg)
	}
	server, err := server.NewGRPCServer(cfg, grpc.Creds(serverCreds))
	require.NoError(t, err)

	go func() {
		server.Serve(l)
	}()

	return client, nobodyClient, cfg, func() {
		server.Stop()
		rootConn.Close()
		nobodyConn.Close()
		l.Close()
	}
}

func testProduceConsume(t *testing.T, client api.LogClient, _ api.LogClient, config *server.Config) {
	ctx := context.Background()

	want := &api.Record{
		Value: []byte("hello world"),
	}

	produce, err := client.Produce(ctx, &api.ProduceRequest{Record: want})
	require.NoError(t, err)

	consume, err := client.Consume(ctx, &api.ConsumeRequest{Offset: produce.Offset})
	require.NoError(t, err)
	require.Equal(t, want.Value, consume.Record.Value)
	require.Equal(t, want.Offset, consume.Record.Offset)
}

func testConsumePastBoundary(t *testing.T, client api.LogClient, _ api.LogClient, config *server.Config) {
	ctx := context.Background()

	produce, err := client.Produce(ctx, &api.ProduceRequest{Record: &api.Record{Value: []byte("hello world!")}})
	require.NoError(t, err)

	consume, err := client.Consume(ctx, &api.ConsumeRequest{Offset: produce.Offset + 1})
	require.Error(t, err)
	require.Nil(t, consume)
	got := grpc.Code(err)
	want := grpc.Code(api.ErrOffsetOutOfRange{}.GRPCStatus().Err())

	if got != want {
		t.Fatalf("got err: %v, want: %v", err, want)
	}
}

func testProduceConsumeStream(t *testing.T, client api.LogClient, _ api.LogClient, config *server.Config) {
	ctx := context.Background()

	records := []*api.Record{{
		Value:  []byte("first message"),
		Offset: 0,
	}, {
		Value:  []byte("second message"),
		Offset: 1,
	}}

	{
		stream, err := client.ProduceStream(ctx)
		require.NoError(t, err)

		for offset, record := range records {
			err = stream.Send(&api.ProduceRequest{Record: record})
			require.NoError(t, err)

			res, err := stream.Recv()
			require.NoError(t, err)
			require.Equal(t, uint64(offset), res.Offset)
		}
	}

	{
		stream, err := client.ConsumeStream(ctx, &api.ConsumeRequest{Offset: 0})
		require.NoError(t, err)

		for i, record := range records {
			res, err := stream.Recv()

			require.NoError(t, err)
			require.Equal(t, res.Record, &api.Record{
				Value:  record.Value,
				Offset: uint64(i),
			})
		}

	}
}

func testUnauthorized(t *testing.T, _, client api.LogClient, config *server.Config) {
	ctx := context.Background()
	produce, err := client.Produce(ctx, &api.ProduceRequest{
		Record: &api.Record{
			Value: []byte("hello world"),
		},
	})
	if produce != nil {
		t.Fatalf("Produce Response should be nil")
	}
	got, want := grpc.Code(err), codes.PermissionDenied

	if got != want {
		t.Fatalf("got : %d, want: %d", got, want)
	}
	consume, err := client.Consume(ctx, &api.ConsumeRequest{Offset: 0})
	if consume != nil {
		t.Fatalf("Consume should be nil")
	}
	got, want = grpc.Code(err), codes.PermissionDenied
	if got != want {
		t.Fatalf("got : %d, want: %d", got, want)
	}
}
