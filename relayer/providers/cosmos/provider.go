package cosmos

import (
	"time"

	"github.com/cosmos/cosmos-sdk/simapp/params"
	rpcclient "github.com/tendermint/tendermint/rpc/client"
	rpchttp "github.com/tendermint/tendermint/rpc/client/http"
	libclient "github.com/tendermint/tendermint/rpc/jsonrpc/client"
)

type CosmosProvider struct {
	ChainID  string
	Encoding params.EncodingConfig
	Client   rpcclient.Client
	debug    bool
}

func NewCosmosProvider(chainid, rpcaddr, prefix string, timeout time.Duration, debug bool) *CosmosProvider {
	_, err := newRPCClient(rpcaddr, timeout)
	if err != nil {
		panic(err)
	}
	_ = MakeEncodingConfig(prefix)
	return &CosmosProvider{}
}

func newRPCClient(addr string, timeout time.Duration) (*rpchttp.HTTP, error) {
	httpClient, err := libclient.DefaultHTTPClient(addr)
	if err != nil {
		return nil, err
	}

	httpClient.Timeout = timeout
	rpcClient, err := rpchttp.NewWithClient(addr, "/websocket", httpClient)
	if err != nil {
		return nil, err
	}

	return rpcClient, nil
}
