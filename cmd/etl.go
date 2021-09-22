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
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/avast/retry-go"

	sdk "github.com/cosmos/cosmos-sdk/types"
	transfertypes "github.com/cosmos/ibc-go/modules/apps/transfer/types"
	channeltypes "github.com/cosmos/ibc-go/modules/core/04-channel/types"
	"github.com/cosmos/relayer/relayer"
	_ "github.com/lib/pq"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"
)

const driverName = "postgres"

var db *sql.DB

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
		Use:     "quality-of-servce [chain-id] [src_start]",
		Aliases: []string{"qos"},
		Short:   "query denomination traces for a given network by chain ID",
		Args:    cobra.ExactArgs(2),
		Example: strings.TrimSpace(fmt.Sprintf(`
$ %s query ibc-denoms ibc-0
$ %s q ibc-denoms ibc-0`,
			appName, appName,
		)),
		RunE: func(cmd *cobra.Command, args []string) error {
			chain, err := config.Chains.Get(args[0])
			if err != nil {
				return err
			}

			connString, _ := cmd.Flags().GetString("conn")
			fmt.Printf("Connecting to database with conn string: %s \n", connString)
			db, err = sql.Open(driverName, connString)
			if err != nil {
				return fmt.Errorf("Failed to connect to db, ensure db server is running & check conn string. Err: %s \n", err.Error())
			}
			defer db.Close()

			alive := db.Ping()
			if alive != nil {
				return fmt.Errorf("Failed to connect to db, ensure db server is running & check conn string. Err: %s \n", err.Error())
			}
			fmt.Println("Successfully connected to db instance.")

			srcStart, err := strconv.ParseInt(args[1], 10, 64)
			if err != nil {
				return err
			}

			srcBlocks, err := makeBlockArray(chain, srcStart)
			if err != nil {
				return nil
			}
			fmt.Printf("chain-id[%s] startBlock(%d) endBlock(%d)\n", chain.ChainID, srcBlocks[0], srcBlocks[len(srcBlocks)-1])

			return QueryBlocks(chain, srcBlocks)
		},
	}

	//TODO add proper default value
	cmd.Flags().StringP("conn", "c", "host=127.0.0.1 port=5432 user=anon dbname=relayer sslmode=disable", "database connection string")
	return cmd
}

func QueryBlocks(chain *relayer.Chain, blocks []int64) error {
	fmt.Println("starting block queries for", chain.ChainID)
	var eg errgroup.Group
	failedBlocks := make([]int64, 0)
	sem := make(chan struct{}, 100)

	for _, h := range blocks {
		h := h
		//fmt.Println(h)
		sem <- struct{}{}
		//fmt.Printf("Queue has this many elements: %d \n", len(sem))

		eg.Go(func() error {
			block, err := chain.Client.Block(context.Background(), &h)
			if err != nil {
				if err = retry.Do(func() error {
					block, err = chain.Client.Block(context.Background(), &h)
					if err != nil {
						return err
					}

					return nil
				}, relayer.RtyAtt, relayer.RtyDel, relayer.RtyErr, retry.DelayType(retry.BackOffDelay), retry.OnRetry(func(n uint, err error) {
					chain.LogRetryGetBlock(n, err, h)
				})); err != nil {
					if strings.Contains(err.Error(), "wrong ID: no ID") {
						failedBlocks = append(failedBlocks, h)
					}
				}
			}

			for _, tx := range block.Block.Data.Txs {
				sdkTx, err := chain.Encoding.TxConfig.TxDecoder()(tx)
				if err != nil {
					fmt.Printf("Failed to decode tx at height %d from %s \n", h, chain.ChainID)
					return err
				}

				err = insertTxRow(tx.Hash(), chain.ChainID, h, block.Block.Time)
				if err != nil {
					fmt.Printf("Failed to insert tx at Height: %d on chain %s. Err: %s", block.Block.Height, chain.ChainID, err.Error())
				} else {
					fmt.Printf("Wrote to database for height %d with %d txs \n", h, len(sdkTx.GetMsgs()))
				}

				for msgIndex, msg := range sdkTx.GetMsgs() {
					handleMsg(chain, msg, msgIndex, block.Block.Height, block.Block.Time, chain.ChainID, block.Block.Hash())
				}
			}

			<-sem
			fmt.Printf("Queue has this many elements: %d \n", len(sem))
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return err
	}
	if len(failedBlocks) > 0 {
		return QueryBlocks(chain, failedBlocks)
	}
	return nil
}

func makeBlockArray(src *relayer.Chain, srcStart int64) ([]int64, error) {
	srcBlocks := []int64{}
	srcCurrent, err := src.QueryLatestHeight()
	if err != nil {
		return srcBlocks, err
	}
	for i := srcStart; i < srcCurrent; i++ {
		srcBlocks = append(srcBlocks, i)
	}
	return srcBlocks, nil
}

func handleMsg(c *relayer.Chain, msg sdk.Msg, msgIndex int, height int64, timestamp time.Time, chainid string, hash []byte) {
	switch m := msg.(type) {
	case *transfertypes.MsgTransfer:
		done := c.UseSDKContext()

		err := insertMsgTransferRow(hash, m.Token.Denom, m.SourceChannel, m.Route(), m.Token.Amount.String(), msgIndex)
		if err != nil {
			fmt.Printf("Failed to insert MsgTransfer. Height: %d Err: %s", height, err.Error())
		} else {
			fmt.Printf("Wrote to database for height %d  \n", height)
		}

		fmt.Printf("[%s] => [%s]@{%d} *transfertypes.MsgTransfer [%x]\n", timestamp.String(), chainid, height, m.GetSigners()[0].Bytes())
		done()
	case *channeltypes.MsgRecvPacket:
		done := c.UseSDKContext()

		err := insertMsgRecvPacketRow(m.Packet.Sequence, hash, m.Signer, m.Packet.SourceChannel,
			m.Packet.DestinationChannel, m.Packet.SourcePort, m.Packet.DestinationPort, msgIndex)
		if err != nil {
			fmt.Printf("Failed to insert MsgRecvPacket. Sequence: %d Err: %s", m.Packet.Sequence, err.Error())
		} else {
			fmt.Printf("Wrote to database for height %d \n", height)
		}

		fmt.Printf("[%s] => [%s]@{%d} *channeltypes.MsgRecvPacket [%x]\n", timestamp.String(), chainid, height, m.GetSigners()[0].Bytes())
		done()
	case *channeltypes.MsgTimeout:
		done := c.UseSDKContext()

		err := insertMsgTimeoutRow(m.Packet.Sequence, hash, m.Signer, m.Packet.SourceChannel,
			m.Packet.DestinationChannel, m.Packet.SourcePort, m.Packet.DestinationPort, msgIndex)
		if err != nil {
			fmt.Printf("Failed to insert MsgTimeout. Sequence: %d Err: %s", m.Packet.Sequence, err.Error())
		} else {
			fmt.Printf("Wrote to database for height %d\n", height)
		}

		fmt.Printf("[%s] => [%s]@{%d} *channeltypes.MsgTimeout [%x]\n", timestamp.String(), chainid, height, m.GetSigners()[0].Bytes())
		done()
	case *channeltypes.MsgAcknowledgement:
		done := c.UseSDKContext()

		err := insertMsgAckRow(m.Packet.Sequence, hash, m.Signer, m.Packet.SourceChannel,
			m.Packet.DestinationChannel, m.Packet.SourcePort, m.Packet.DestinationPort, msgIndex)
		if err != nil {
			fmt.Printf("Failed to insert MsgAck. Sequence: %d Err: %s", m.Packet.Sequence, err.Error())
		} else {
			fmt.Printf("Wrote to database for height %d  \n", height)
		}

		fmt.Printf("[%s] => [%s]@{%d} *channeltypes.MsgAcknowledgement [%x]\n", timestamp.String(), chainid, height, m.GetSigners()[0].Bytes())
		done()
	default:
	}
}

func insertTxRow(hash []byte, cID string, height int64, timestamp time.Time) error {
	stmt, err := db.Prepare("INSERT INTO txs(hash, block_time, chainid, block_height) VALUES($1, $2, $3, $4)")
	if err != nil {
		fmt.Println("Fail to create query")
		return err
	}

	_, err = stmt.Exec(hash, timestamp, cID, height)
	if err != nil {
		fmt.Println("Fail to execute query")
		return err
	}

	return nil
}

func insertMsgTransferRow(hash []byte, denom, srcChan, route, amount string, msgIndex int) error {
	stmt, err := db.Prepare("INSERT INTO msg_transfer(tx_hash, msg_index, amount, denom, src_chan, route) VALUES($1, $2, $3, $4, $5, $6)")
	if err != nil {
		fmt.Println("Fail to create query")
		return err
	}

	_, err = stmt.Exec(hash, msgIndex, amount, denom, srcChan, route)
	if err != nil {
		fmt.Println("Fail to execute query")
		return err
	}

	return nil
}

func insertMsgTimeoutRow(sequence uint64, hash []byte, signer, srcChan, dstChan, srcPort, dstPort string, msgIndex int) error {
	stmt, err := db.Prepare("INSERT INTO msg_timeout(tx_hash, msg_index, signer, src_chan, dst_chan, src_port, dst_port) VALUES($1, $2, $3, $4, $5, $6, $7)")
	if err != nil {
		fmt.Println("Fail to create query")
		return err
	}

	_, err = stmt.Exec(hash, msgIndex, signer, srcChan, dstChan, srcPort, dstPort)
	if err != nil {
		fmt.Println("Fail to execute query")
		return err
	}

	return nil
}

func insertMsgRecvPacketRow(sequence uint64, hash []byte, signer, srcChan, dstChan, srcPort, dstPort string, msgIndex int) error {
	stmt, err := db.Prepare("INSERT INTO msg_recvpacket(tx_hash, msg_index, signer, src_chan, dst_chan, src_port, dst_port) VALUES($1, $2, $3, $4, $5, $6, $7)")
	if err != nil {
		fmt.Println("Fail to create query")
		return err
	}

	_, err = stmt.Exec(hash, msgIndex, signer, srcChan, dstChan, srcPort, dstPort)
	if err != nil {
		fmt.Println("Fail to execute query")
		return err
	}

	return nil
}

func insertMsgAckRow(sequence uint64, hash []byte, signer, srcChan, dstChan, srcPort, dstPort string, msgIndex int) error {
	stmt, err := db.Prepare("INSERT INTO msg_ack(tx_hash, msg_index, signer, src_chan, dst_chan, src_port, dst_port) VALUES($1, $2, $3, $4, $5, $6, $7)")
	if err != nil {
		fmt.Println("Fail to create query")
		return err
	}

	_, err = stmt.Exec(hash, msgIndex, signer, srcChan, dstChan, srcPort, dstPort)
	if err != nil {
		fmt.Println("Fail to execute query")
		return err
	}

	return nil
}
