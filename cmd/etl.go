/*
Copyright © 2021 NAME HERE <EMAIL ADDRESS>

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package cmd

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	sdk "github.com/cosmos/cosmos-sdk/types"
	transfertypes "github.com/cosmos/ibc-go/modules/apps/transfer/types"
	channeltypes "github.com/cosmos/ibc-go/modules/core/04-channel/types"
	"github.com/cosmos/relayer/relayer"
	"github.com/spf13/cobra"
)

// queryCmd represents the chain command
func etlCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "etl",
		Short: "extract transform load tooling for doing bulk IBC queries",
	}

	cmd.AddCommand(
		qosCmd(),
	)

	return cmd
}

// query latest heights
// make []int64 containing all heights between start and current for src
// make []int64 containing all heights between start and current for dst
// iterate over all src heights
// // query the block at the height
// // decode all txs in the block and iterate
// // // iterate over all msgs in the tx
// // // // write a row to a postgres table for each transfertypes.MsgTransfer, channeltypes.MsgRecvPacket,channeltypes.MsgTimeout, channeltypes.MsgAcknowledgement
// iterate over all dst heights
// // query the block at the heigth
// // decode all txs in the block and iterate
// // // iterate over all msgs in the tx
// // // // write a row to a postgres table for each transfertypes.MsgTransfer, channeltypes.MsgRecvPacket,channeltypes.MsgTimeout, channeltypes.MsgAcknowledgement

// rly etl qos hubosmo {start_height_cosmoshub-4} {start_height_osmosis-1} {sql_connection_string}
func qosCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "quality-of-servce [path] [src_start] [dst_start]",
		Aliases: []string{"qos"},
		Short:   "query denomination traces for a given network by chain ID",
		Args:    cobra.ExactArgs(1),
		Example: strings.TrimSpace(fmt.Sprintf(`
$ %s query ibc-denoms ibc-0
$ %s q ibc-denoms ibc-0`,
			appName, appName,
		)),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, src, dst, err := config.ChainsFromPath(args[0])
			if err != nil {
				return err
			}

			srcStart, err := strconv.ParseInt(args[0], 10, 64)
			if err != nil {
				return err
			}
			dstStart, err := strconv.ParseInt(args[1], 10, 64)
			if err != nil {
				return err
			}
			srcBlocks := []int64{srcStart}
			dstBlocks := []int64{dstStart}

			srcCurrent, dstCurrent, err := relayer.QueryLatestHeights(c[src], c[dst])
			if err != nil {
				return err
			}

			for i := srcStart; i < srcCurrent; i++ {
				srcBlocks = append(srcBlocks, i)
			}

			for i := dstStart; i < dstCurrent; i++ {
				dstBlocks = append(dstBlocks, i)
			}

			for _, h := range srcBlocks {
				block, err := c[src].Client.Block(context.Background(), &h)
				if err != nil {
					return err
				}
				for _, tx := range block.Block.Data.Txs {
					sdkTx, err := c[src].Encoding.TxConfig.TxDecoder()(tx)
					if err != nil {
						return err
					}
					for _, msg := range sdkTx.GetMsgs() {
						handleMsg(msg, block.Block.Height, block.Block.Time, src)
					}
				}
			}

			for _, h := range dstBlocks {
				block, err := c[dst].Client.Block(context.Background(), &h)
				if err != nil {
					return err
				}
				for _, tx := range block.Block.Data.Txs {
					sdkTx, err := c[dst].Encoding.TxConfig.TxDecoder()(tx)
					if err != nil {
						return err
					}
					for _, msg := range sdkTx.GetMsgs() {
						handleMsg(msg, block.Block.Height, block.Block.Time, dst)
					}
				}
			}

			return nil
		},
	}

	return cmd
}

func handleMsg(msg sdk.Msg, height int64, timestamp time.Time, chainid string) {
	switch m := msg.(type) {
	case *transfertypes.MsgTransfer:
		fmt.Printf("%s => [%s]@{%d} *transfertypes.MsgTransfer [%x]", timestamp.String(), chainid, height, m.GetSigners()[0].Bytes())
	case *channeltypes.MsgRecvPacket:
		fmt.Printf("%s => [%s]@{%d} *channeltypes.MsgRecvPacket [%x]", timestamp.String(), chainid, height, m.GetSigners()[0].Bytes())
	case *channeltypes.MsgTimeout:
		fmt.Printf("%s => [%s]@{%d} *channeltypes.MsgTimeout [%x]", timestamp.String(), chainid, height, m.GetSigners()[0].Bytes())
	case *channeltypes.MsgAcknowledgement:
		fmt.Printf("%s => [%s]@{%d} *channeltypes.MsgAcknowledgement [%x]", timestamp.String(), chainid, height, m.GetSigners()[0].Bytes())
	}
}
