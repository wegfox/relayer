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
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/avast/retry-go"
	"github.com/cosmos/cosmos-sdk/client"
	sdk "github.com/cosmos/cosmos-sdk/types"
	transfertypes "github.com/cosmos/ibc-go/v2/modules/apps/transfer/types"
	channeltypes "github.com/cosmos/ibc-go/v2/modules/core/04-channel/types"
	"github.com/cosmos/relayer/relayer"
	_ "github.com/lib/pq"
	"github.com/spf13/cobra"
	coretypes "github.com/tendermint/tendermint/rpc/core/types"
	"golang.org/x/sync/errgroup"
	"gopkg.in/yaml.v3"
)

func etlCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "etl",
		Short: "extract transform load tooling for doing bulk IBC queries",
	}

	cmd.AddCommand(
		extractCmd(),
		qos(),
		transferAmounts(),
	)

	return cmd
}

func transferAmounts() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "transfer-amounts [path]",
		Aliases: []string{"t"},
		Short:   "retrieve token transfer amounts on a given chain for a specified date-time period",
		Args:    cobra.ExactArgs(1),
		Example: strings.TrimSpace(fmt.Sprintf(`
$ %s etl transfer-amounts hubosmo --start YYYY-MM-DD HH:MM:SS --end YYYY-MM-DD HH:MM:SS
$ %s e t osmoterra --start YYYY-MM-DD HH:MM:SS`,
			appName, appName,
		)),
		RunE: func(cmd *cobra.Command, args []string) error {
			const driverName = "postgres"
			p, err := config.Paths.Get(args[0])
			if err != nil {
				return err
			}

			connString, _ := cmd.Flags().GetString("conn")
			fmt.Printf("Connecting to database with conn string: %s \n", connString)
			db, err := connectToDatabase(driverName, connString)
			if err != nil {
				return err
			}
			defer db.Close()
			fmt.Println("Successfully connected to db instance.")

			start, _ := cmd.Flags().GetString("start")
			strtTime, err := time.Parse("2006-01-02 15:04:05", start)
			if err != nil {
				return err
			}

			end, _ := cmd.Flags().GetString("end")
			endTime, err := time.Parse("2006-01-02 15:04:05", end)
			if err != nil {
				return err
			}

			srcChain, err := config.Chains.Get(p.Src.ChainID)
			if err != nil {
				return err
			}
			srcChan := p.Src.ChannelID
			dstChain, err := config.Chains.Get(p.Dst.ChainID)
			if err != nil {
				return err
			}
			dstChan := p.Dst.ChannelID

			fmt.Printf("[%s:%s <-> %s:%s] Fetching transfers for %s - %s\n", srcChain.ChainID, srcChan,
				dstChain.ChainID, dstChan, strtTime.Format("2006-01-02 15:04:05"), endTime.Format("2006-01-02 15:04:05"))

			srcAmounts, err := getTransferedAmounts(srcChain, srcChan, strtTime, endTime, db)
			if err != nil {
				return err
			}
			dstAmounts, err := getTransferedAmounts(dstChain, dstChan, strtTime, endTime, db)
			if err != nil {
				return err
			}

			// Combine src & dst transfer amounts into one map
			totalAmounts := srcAmounts
			for denom, amount := range dstAmounts {
				if _, exists := totalAmounts[denom]; exists {
					totalAmounts[denom] = totalAmounts[denom] + amount
				} else {
					totalAmounts[denom] = amount
				}
			}

			for denom, amount := range totalAmounts {
				// make the output pretty
				if len(denom) >= 6 {
					fmt.Printf("[%s:%s <-> %s:%s] Denom: %s \t Amount: %d \n", srcChain.ChainID, srcChan, dstChain.ChainID, dstChan, denom, amount)
				} else {
					fmt.Printf("[%s:%s <-> %s:%s] Denom: %s \t\t Amount: %d \n", srcChain.ChainID, srcChan, dstChain.ChainID, dstChan, denom, amount)
				}
			}

			return nil
		},
	}

	cmd.Flags().StringP("conn", "c", "host=127.0.0.1 port=5432 user=anon dbname=relayer sslmode=disable", "database connection string")
	cmd.Flags().StringP("start", "s", time.Now().AddDate(0, -1, 0).Format("2006-01-02 15:04:05"), "start date-time for QoS query")
	cmd.Flags().StringP("end", "e", time.Now().Format("2006-01-02 15:04:05"), "end date-time for QoS query")
	return cmd
}

func qos() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "quality-of-servce [path]",
		Aliases: []string{"qos"},
		Short:   "retrieve QoS metrics on a given path for a specified date-time period",
		Args:    cobra.ExactArgs(1),
		Example: strings.TrimSpace(fmt.Sprintf(`
$ %s etl quality-of-service hubosmo --start YYYY-MM-DD HH:MM:SS --end YYYY-MM-DD HH:MM:SS
$ %s e qos osmoterra
$ %s etl qos hubjuno --start YYYY-MM-DD HH:MM:SS`,
			appName, appName, appName,
		)),
		RunE: func(cmd *cobra.Command, args []string) error {
			const driverName = "postgres"
			p, err := config.Paths.Get(args[0])
			if err != nil {
				return err
			}

			connString, _ := cmd.Flags().GetString("conn")
			fmt.Printf("Connecting to database with conn string: %s \n", connString)
			db, err := connectToDatabase(driverName, connString)
			if err != nil {
				return err
			}
			defer db.Close()
			fmt.Println("Successfully connected to db instance.")

			start, _ := cmd.Flags().GetString("start")
			strtTime, err := time.Parse("2006-01-02 15:04:05", start)
			if err != nil {
				return err
			}

			end, _ := cmd.Flags().GetString("end")
			endTime, err := time.Parse("2006-01-02 15:04:05", end)
			if err != nil {
				return err
			}

			srcChain := p.Src.ChainID
			srcChan := p.Src.ChannelID
			dstChain := p.Dst.ChainID
			dstChan := p.Dst.ChannelID

			fmt.Printf("[%s:%s <-> %s:%s] Calculating IBC QoS over %s - %s\n", srcChain, srcChan,
				dstChain, dstChan, strtTime.Format("2006-01-02 15:04:05"), endTime.Format("2006-01-02 15:04:05"))

			srcTransfers, err := getTransfersForPeriod(srcChain, srcChan, db, strtTime, endTime)
			if err != nil {
				return err
			}
			dstTransfers, err := getTransfersForPeriod(dstChain, dstChan, db, strtTime, endTime)
			if err != nil {
				return err
			}

			srcTimeouts, err := getTimeoutsForPeriod(srcChain, srcChan, dstChan, db, strtTime, endTime)
			if err != nil {
				return err
			}
			dstTimeouts, err := getTimeoutsForPeriod(dstChain, dstChan, srcChan, db, strtTime, endTime)
			if err != nil {
				return err
			}

			srcRecvPackets, err := getRecvPacketsForPeriod(srcChain, dstChan, srcChan, db, strtTime, endTime)
			if err != nil {
				return err
			}
			dstRecvPackets, err := getRecvPacketsForPeriod(dstChain, srcChan, dstChan, db, strtTime, endTime)
			if err != nil {
				return err
			}

			// calculate successes and failures for both chains + combined avg
			var srcAvgTimeouts, srcAvgRecvd, dstAvgTimeouts, dstAvgRecvd, avgTimeouts, avgRecvd float64
			srcAvgTimeouts = float64(srcTimeouts) / float64(srcTransfers)
			srcAvgRecvd = float64(dstRecvPackets) / float64(srcTransfers)
			dstAvgTimeouts = float64(dstTimeouts) / float64(dstTransfers)
			dstAvgRecvd = float64(srcRecvPackets) / float64(dstTransfers)
			avgTimeouts = float64(srcTimeouts+dstTimeouts) / float64(srcTransfers+dstTransfers)
			avgRecvd = float64(srcRecvPackets+dstRecvPackets) / float64(srcTransfers+dstTransfers)

			fmt.Printf("[%s:%s -> %s:%s] - Timedout Packets: %f%% \n", srcChain, srcChan, dstChain, dstChan, srcAvgTimeouts*100)
			fmt.Printf("[%s:%s -> %s:%s] - Received Packets: %f%% \n", srcChain, srcChan, dstChain, dstChan, srcAvgRecvd*100)
			fmt.Printf("[%s:%s -> %s:%s] - Timedout Packets: %f%% \n", dstChain, dstChan, srcChain, srcChan, dstAvgTimeouts*100)
			fmt.Printf("[%s:%s -> %s:%s] - Received Packets: %f%% \n", dstChain, dstChan, srcChain, srcChan, dstAvgRecvd*100)
			fmt.Printf("[%s:%s <-> %s:%s] - Average Timedout Packets: %f%% \n", srcChain, srcChan, dstChain, dstChan, avgTimeouts*100)
			fmt.Printf("[%s:%s <-> %s:%s] - Average Received Packets: %f%% \n", srcChain, srcChan, dstChain, dstChan, avgRecvd*100)

			if debug {
				fmt.Println("---------------------------------------------------")
				fmt.Printf("[%s:%s -> %s:%s] - There were %d MsgTransfers. \n", srcChain, srcChan, dstChain, dstChan, srcTransfers)
				fmt.Printf("[%s:%s -> %s:%s] - There were %d MsgTransfers. \n", dstChain, dstChan, srcChain, srcChan, dstTransfers)
				fmt.Printf("[%s:%s -> %s:%s] - There were %d MsgTimeouts. \n", srcChain, srcChan, dstChain, dstChan, srcTimeouts)
				fmt.Printf("[%s:%s -> %s:%s] - There were %d MsgTimeouts. \n", dstChain, dstChan, srcChain, srcChan, dstTimeouts)
				fmt.Printf("[%s:%s -> %s:%s] - There were %d MsgRecvPackets. \n", srcChain, srcChan, dstChain, dstChan, srcRecvPackets)
				fmt.Printf("[%s:%s -> %s:%s] - There were %d MsgRecvPackets. \n", dstChain, dstChan, srcChain, srcChan, dstRecvPackets)
				fmt.Println("---------------------------------------------------")
			}

			return nil
		},
	}

	cmd.Flags().StringP("conn", "c", "host=127.0.0.1 port=5432 user=anon dbname=relayer sslmode=disable", "database connection string")
	cmd.Flags().StringP("start", "s", time.Now().AddDate(0, -1, 0).Format("2006-01-02 15:04:05"), "start date-time for QoS query")
	cmd.Flags().StringP("end", "e", time.Now().Format("2006-01-02 15:04:05"), "end date-time for QoS query")
	return cmd
}

func extractCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "extract [chain-id]",
		Aliases: []string{"e"},
		Short:   "extract pertinent IBC/tx data from a chain and load into a postgres db",
		Args:    cobra.ExactArgs(1),
		Example: strings.TrimSpace(fmt.Sprintf(`
$ %s etl extract cosmoshub-4 -c "host=127.0.0.1 port=5432 user=anon dbname=relayer sslmode=disable" --height 0
$ %s etl e osmosis-1 --height 5000000
$ %s etl extract sentinelhub-2 --conn "host=127.0.0.1 port=5432 user=anon dbname=relayer sslmode=disable"`,
			appName, appName, appName,
		)),
		RunE: func(cmd *cobra.Command, args []string) error {
			const driverName = "postgres"
			chain, err := config.Chains.Get(args[0])
			if err != nil {
				return err
			}

			connString, _ := cmd.Flags().GetString("conn")
			fmt.Printf("Connecting to database with conn string: %s \n", connString)
			db, err := connectToDatabase(driverName, connString)
			if err != nil {
				return err
			}
			defer db.Close()
			fmt.Println("Successfully connected to db instance.")

			if err = createTables(db); err != nil {
				return err
			}

			// If the user does not provide a height, attempt to use the last height stored in the DB
			// & if there are no previous entries in db then start from height 1.
			srcStart, _ := cmd.Flags().GetInt64("height")
			if srcStart == 0 {
				srcStart, _ = getLastStoredBlock(chain.ChainID, db)
			}

			srcBlocks, err := makeBlockArray(chain, srcStart)
			if err != nil {
				return err
			}
			fmt.Printf("chain-id[%s] startBlock(%d) endBlock(%d)\n", chain.ChainID, srcBlocks[0], srcBlocks[len(srcBlocks)-1])

			return queryBlocks(chain, srcBlocks, db)
		},
	}

	cmd.Flags().Int64("height", 0, "block height which you wish to begin the query from")
	cmd.Flags().StringP("conn", "c", "host=127.0.0.1 port=5432 user=anon dbname=relayer sslmode=disable", "database connection string")
	return cmd
}

func queryBlocks(chain *relayer.Chain, blocks []int64, db *sql.DB) error {
	fmt.Println("starting block queries for", chain.ChainID)
	var (
		eg    errgroup.Group
		mutex sync.Mutex
	)
	failedBlocks := make([]int64, 0)
	sem := make(chan struct{}, 100)
	coinGeckoData := buildCoinGeckoData()
	cache := make(map[string]*CacheEntry, 3)

	for _, h := range blocks {
		h := h
		sem <- struct{}{}

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
						mutex.Lock()
						failedBlocks = append(failedBlocks, h)
						mutex.Unlock()
					} else {
						fmt.Printf("[Height %d] - Failed to get block. Err: %s \n", h, err.Error())
					}
				}
			}

			if block != nil {
				parseTxs(chain, block, h, db, coinGeckoData, cache)
			}

			<-sem
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return err
	}
	if len(failedBlocks) > 0 {
		return queryBlocks(chain, failedBlocks, db)
	}
	return nil
}

func parseTxs(chain *relayer.Chain, block *coretypes.ResultBlock, h int64, db *sql.DB, coinGeckoData *CoinGeckoData, cache map[string]*CacheEntry) {
	for i, tx := range block.Block.Data.Txs {
		sdkTx, err := chain.Encoding.TxConfig.TxDecoder()(tx)
		if err != nil {
			// TODO application specific txs fail here (e.g. DEX swaps, Akash deployments, etc.)
			fmt.Printf("[Height %d] {%d/%d txs} - Failed to decode tx. Err: %s \n", block.Block.Height, i+1, len(block.Block.Data.Txs), err.Error())
			continue
		}

		txRes, err := chain.QueryTx(hex.EncodeToString(tx.Hash()))
		if err != nil {
			fmt.Printf("[Height %d] {%d/%d txs} - Failed to query tx results. Err: %s \n", block.Block.Height, i+1, len(block.Block.Data.Txs), err.Error())
			continue
		}

		fee := sdkTx.(sdk.FeeTx)
		var feeAmount, feeDenom string
		tokenValue := float64(0)

		if len(fee.GetFee()) == 0 {
			feeAmount = "0"
			feeDenom = ""
			tokenValue = 0
		} else {
			feeAmount = fee.GetFee()[0].Amount.String()
			feeDenom = fee.GetFee()[0].Denom
			if val, exists := coinGeckoData.Networks[feeDenom]; exists {
				// attempt to use cached token value if it exists, and it is from the same date.
				// if the data has not been cached or newer data is needed query from CoinGecko API
				if cacheEntry, exists := cache[feeDenom]; exists {
					if DateEqual(block.Block.Time, cacheEntry.time) {
						tokenValue = cacheEntry.value
					} else {
						tokenValue = GetPriceAndUpdateCache(cache, block.Block.Time, val)
					}
				} else {
					tokenValue = GetPriceAndUpdateCache(cache, block.Block.Time, val)
				}
			}
		}

		if txRes.TxResult.Code > 0 {
			log := fmt.Sprintf("{\"error\":\"%s\"}", txRes.TxResult.Log)
			err = insertTxRow(tx.Hash(), chain.ChainID, log, feeAmount, feeDenom, h, txRes.TxResult.GasUsed,
				txRes.TxResult.GasWanted, block.Block.Time, db, txRes.TxResult.Code, tokenValue)

			logTxInsertion(err, i, len(sdkTx.GetMsgs()), len(block.Block.Data.Txs), block.Block.Height)

			if err != nil {
				err = updateTxRow(tx.Hash(), db, tokenValue)
				if err != nil {
					fmt.Printf("Failed to update tx. Index: %d Height: %d Err: %s \n", i, block.Block.Height, err.Error())
				}
			}
		} else {
			err = insertTxRow(tx.Hash(), chain.ChainID, txRes.TxResult.Log, feeAmount, feeDenom, h, txRes.TxResult.GasUsed,
				txRes.TxResult.GasWanted, block.Block.Time, db, txRes.TxResult.Code, tokenValue)

			logTxInsertion(err, i, len(sdkTx.GetMsgs()), len(block.Block.Data.Txs), block.Block.Height)

			if err != nil {
				err = updateTxRow(tx.Hash(), db, tokenValue)
				if err != nil {
					fmt.Printf("Failed to update tx. Index: %d Height: %d Err: %s \n", i, block.Block.Height, err.Error())
				}
			}
		}

		for msgIndex, msg := range sdkTx.GetMsgs() {
			handleMsg(chain, msg, msgIndex, block.Block.Height, tx.Hash(), db, coinGeckoData, block.Block.Time, cache)
		}
	}
}

func connectToDatabase(driver, connString string) (*sql.DB, error) {
	db, err := sql.Open(driver, connString)
	if err != nil {
		return nil, fmt.Errorf("Failed to open db, ensure db server is running & check conn string. Err: %s \n", err.Error())
	}
	if err = db.Ping(); err != nil {
		return nil, fmt.Errorf("Failed to connect to db, ensure db server is running & check conn string. Err: %s \n", err.Error())
	}
	return db, nil
}

func makeBlockArray(src *relayer.Chain, srcStart int64) ([]int64, error) {
	srcBlocks := make([]int64, 0)
	srcCurrent, err := src.QueryLatestHeight()
	if err != nil {
		return srcBlocks, err
	}
	for i := srcStart; i < srcCurrent; i++ {
		srcBlocks = append(srcBlocks, i)
	}
	return srcBlocks, nil
}

func handleMsg(c *relayer.Chain, msg sdk.Msg, msgIndex int, height int64, hash []byte, db *sql.DB, coinGeckoData *CoinGeckoData, time time.Time, cache map[string]*CacheEntry) {
	switch m := msg.(type) {
	case *transfertypes.MsgTransfer:
		done := c.UseSDKContext()

		var (
			tokenValue float64
			err        error
		)

		tokenValue = 0
		denom := m.Token.Denom
		if strings.Contains(denom, "ibc/") {
			denomRes, err := c.QueryDenomTrace(strings.Trim(denom, "ibc/"))
			if err != nil {
				fmt.Printf("Error querying denom trace %s. Err: %s \n", denom, err.Error())
			} else {
				denom = denomRes.DenomTrace.BaseDenom
				if val, exists := coinGeckoData.Networks[denom]; exists {
					// attempt to use cached token value if it exists, and it is from the same date.
					// if the data has not been cached or newer data is needed query from CoinGecko API
					if cacheEntry, exists := cache[denom]; exists {
						if DateEqual(cacheEntry.time, time) {
							tokenValue = cacheEntry.value
						} else {
							tokenValue = GetPriceAndUpdateCache(cache, time, val)
						}
					} else {
						tokenValue = GetPriceAndUpdateCache(cache, time, val)
					}
				}
			}
		} else {
			if val, exists := coinGeckoData.Networks[denom]; exists {
				// attempt to use cached token value if it exists, and it is from the same date.
				// if the data has not been cached or newer data is needed query from CoinGecko API
				if cacheEntry, exists := cache[denom]; exists {
					if DateEqual(cacheEntry.time, time) {
						tokenValue = cacheEntry.value
					} else {
						tokenValue = GetPriceAndUpdateCache(cache, time, val)
					}
				} else {
					tokenValue = GetPriceAndUpdateCache(cache, time, val)
				}
			}
		}

		err = insertMsgTransferRow(hash, denom, m.SourceChannel, m.Route(), m.Token.Amount.String(), m.Sender,
			m.GetSigners()[0].String(), m.Receiver, m.SourcePort, msgIndex, db, tokenValue)

		if err != nil {
			fmt.Printf("Failed to insert MsgTransfer. Index: %d Height: %d Err: %s \n", msgIndex, height, err.Error())
			err = updateTransferRow(hash, denom, db, tokenValue, msgIndex)
			if err != nil {
				fmt.Printf("Failed to update MsgTransfer. Index: %d Height: %d Err: %s \n", msgIndex, height, err.Error())
			}
		}

		done()
	case *channeltypes.MsgRecvPacket:
		done := c.UseSDKContext()

		err := insertMsgRecvPacketRow(hash, m.Signer, m.Packet.SourceChannel,
			m.Packet.DestinationChannel, m.Packet.SourcePort, m.Packet.DestinationPort, msgIndex, db)
		if err != nil {
			fmt.Printf("Failed to insert MsgRecvPacket. Index: %d Height: %d Err: %s \n", msgIndex, height, err.Error())
		}

		done()
	case *channeltypes.MsgTimeout:
		done := c.UseSDKContext()

		err := insertMsgTimeoutRow(hash, m.Signer, m.Packet.SourceChannel,
			m.Packet.DestinationChannel, m.Packet.SourcePort, m.Packet.DestinationPort, msgIndex, db)
		if err != nil {
			fmt.Printf("Failed to insert MsgTimeout. Index: %d Height: %d Err: %s \n", msgIndex, height, err.Error())
		}

		done()
	case *channeltypes.MsgAcknowledgement:
		done := c.UseSDKContext()

		err := insertMsgAckRow(hash, m.Signer, m.Packet.SourceChannel,
			m.Packet.DestinationChannel, m.Packet.SourcePort, m.Packet.DestinationPort, msgIndex, db)
		if err != nil {
			fmt.Printf("Failed to insert MsgAck. Index: %d Height: %d Err: %s \n", msgIndex, height, err.Error())
		}

		done()
	default:
	}
}

func createTables(db *sql.DB) error {
	txs := "CREATE TABLE IF NOT EXISTS txs ( " +
		"hash bytea PRIMARY KEY, " +
		"block_time TIMESTAMP NOT NULL, " +
		"chainid TEXT NOT NULL, " +
		"block_height BIGINT NOT NULL, " +
		"raw_log JSONB NOT NULL," +
		"code INT NOT NULL, " +
		"fee_amount TEXT, " +
		"fee_denom TEXT, " +
		"token_value NUMERIC NOT NULL," +
		"gas_used BIGINT NOT NULL," +
		"gas_wanted BIGINT NOT NULL" +
		")"

	transfer := "CREATE TABLE IF NOT EXISTS msg_transfer (" +
		"tx_hash bytea," +
		"msg_index INT," +
		"signer TEXT NOT NULL," +
		"sender TEXT NOT NULL," +
		"receiver TEXT NOT NULL," +
		"amount TEXT NOT NULL," +
		"denom TEXT NOT NULL," +
		"token_value NUMERIC NOT NULL," +
		"src_chan TEXT NOT NULL," +
		"src_port TEXT NOT NULL," +
		"route TEXT NOT NULL," +
		"PRIMARY KEY (tx_hash, msg_index)," +
		"FOREIGN KEY (tx_hash) REFERENCES txs(hash) ON DELETE CASCADE" +
		")"

	recvpacket := "CREATE TABLE IF NOT EXISTS msg_recvpacket ( " +
		"tx_hash bytea," +
		"msg_index INT," +
		"signer TEXT NOT NULL," +
		"src_chan TEXT NOT NULL," +
		"dst_chan TEXT NOT NULL," +
		"src_port TEXT NOT NULL," +
		"dst_port TEXT NOT NULL," +
		"PRIMARY KEY (tx_hash, msg_index)," +
		"FOREIGN KEY (tx_hash) REFERENCES txs(hash) ON DELETE CASCADE" +
		")"

	timeout := "CREATE TABLE IF NOT EXISTS msg_timeout (" +
		"tx_hash bytea," +
		"msg_index INT," +
		"signer TEXT NOT NULL," +
		"src_chan TEXT NOT NULL," +
		"dst_chan TEXT NOT NULL," +
		"src_port TEXT NOT NULL," +
		"dst_port TEXT NOT NULL," +
		"PRIMARY KEY (tx_hash, msg_index)," +
		"FOREIGN KEY (tx_hash) REFERENCES txs(hash) ON DELETE CASCADE" +
		")"

	acks := "CREATE TABLE IF NOT EXISTS msg_ack (" +
		"tx_hash bytea," +
		"msg_index INT," +
		"signer TEXT NOT NULL," +
		"src_chan TEXT NOT NULL," +
		"dst_chan TEXT NOT NULL," +
		"src_port TEXT NOT NULL," +
		"dst_port TEXT NOT NULL," +
		"PRIMARY KEY (tx_hash, msg_index)," +
		"FOREIGN KEY (tx_hash) REFERENCES txs(hash) ON DELETE CASCADE" +
		")"

	tables := []string{txs, transfer, recvpacket, timeout, acks}
	for _, table := range tables {
		if _, err := db.Exec(table); err != nil {
			return err
		}
	}
	return nil
}

func insertTxRow(hash []byte, chainid, log, feeAmount, feeDenom string, height, gasUsed, gasWanted int64, timestamp time.Time, db *sql.DB, code uint32, tokenValue float64) error {
	query := "INSERT INTO txs(hash, block_time, chainid, block_height, raw_log, code, gas_used, gas_wanted, fee_amount, fee_denom, token_value) " +
		"VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)"
	stmt, err := db.Prepare(query)
	if err != nil {
		return fmt.Errorf("Fail to create query for new tx. Err: %s \n", err.Error())
	}
	defer stmt.Close()

	_, err = stmt.Exec(hash, timestamp, chainid, height, log, code, gasUsed, gasWanted, feeAmount, feeDenom, tokenValue)
	if err != nil {
		return fmt.Errorf("Fail to execute query for new tx. Err: %s \n", err.Error())
	}

	return nil
}

func insertMsgTransferRow(hash []byte, denom, srcChan, route, amount, sender, signer, receiver, port string, msgIndex int, db *sql.DB, tokenValue float64) error {
	query := "INSERT INTO msg_transfer(tx_hash, msg_index, amount, denom, src_chan, route, signer, sender, receiver, src_port, token_value) " +
		"VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)"
	stmt, err := db.Prepare(query)
	if err != nil {
		return fmt.Errorf("Fail to create query for MsgTransfer. Err: %s ", err.Error())
	}
	defer stmt.Close()

	_, err = stmt.Exec(hash, msgIndex, amount, denom, srcChan, route, signer, sender, receiver, port, tokenValue)
	if err != nil {
		return fmt.Errorf("Fail to execute query for MsgTransfer. Err: %s ", err.Error())
	}

	return nil
}

func insertMsgTimeoutRow(hash []byte, signer, srcChan, dstChan, srcPort, dstPort string, msgIndex int, db *sql.DB) error {
	query := "INSERT INTO msg_timeout(tx_hash, msg_index, signer, src_chan, dst_chan, src_port, dst_port) " +
		"VALUES($1, $2, $3, $4, $5, $6, $7)"
	stmt, err := db.Prepare(query)
	if err != nil {
		return fmt.Errorf("Fail to create query for MsgTimeout. Err: %s \n", err.Error())
	}
	defer stmt.Close()

	_, err = stmt.Exec(hash, msgIndex, signer, srcChan, dstChan, srcPort, dstPort)
	if err != nil {
		return fmt.Errorf("Fail to execute query for MsgTimeout. Err: %s \n", err.Error())
	}

	return nil
}

func insertMsgRecvPacketRow(hash []byte, signer, srcChan, dstChan, srcPort, dstPort string, msgIndex int, db *sql.DB) error {
	query := "INSERT INTO msg_recvpacket(tx_hash, msg_index, signer, src_chan, dst_chan, src_port, dst_port) " +
		"VALUES($1, $2, $3, $4, $5, $6, $7)"
	stmt, err := db.Prepare(query)
	if err != nil {
		return fmt.Errorf("Fail to create query for MsgRecvPacket. Err: %s \n", err.Error())
	}
	defer stmt.Close()

	_, err = stmt.Exec(hash, msgIndex, signer, srcChan, dstChan, srcPort, dstPort)
	if err != nil {
		return fmt.Errorf("Fail to execute query for MsgRecvPacket. Err: %s \n", err.Error())
	}

	return nil
}

func insertMsgAckRow(hash []byte, signer, srcChan, dstChan, srcPort, dstPort string, msgIndex int, db *sql.DB) error {
	query := "INSERT INTO msg_ack(tx_hash, msg_index, signer, src_chan, dst_chan, src_port, dst_port) " +
		"VALUES($1, $2, $3, $4, $5, $6, $7)"
	stmt, err := db.Prepare(query)
	if err != nil {
		return fmt.Errorf("Fail to create query for MsgAck. Err: %s \n", err.Error())
	}
	defer stmt.Close()

	_, err = stmt.Exec(hash, msgIndex, signer, srcChan, dstChan, srcPort, dstPort)
	if err != nil {
		return fmt.Errorf("Fail to execute query for MsgAck. Err: %s \n", err.Error())
	}

	return nil
}

func updateTxRow(hash []byte, db *sql.DB, tokenValue float64) error {
	query := "UPDATE txs SET token_value = $1 WHERE hash = $2"
	_, err := db.Exec(query, tokenValue, hash)
	if err != nil {
		return fmt.Errorf("Fail to execute query for updating tx. Err: %s \n", err.Error())
	}
	return nil
}

func updateTransferRow(hash []byte, denom string, db *sql.DB, tokenValue float64, msgIndex int) error {
	query := "UPDATE msg_transfer SET token_value = $1, denom = $2 WHERE tx_hash = $3 AND msg_index = $4"
	_, err := db.Exec(query, tokenValue, denom, hash, msgIndex)
	if err != nil {
		return fmt.Errorf("Fail to execute query for updating msg_transfer. Err: %s ", err.Error())
	}
	return nil
}

func getLastStoredBlock(chainId string, db *sql.DB) (int64, error) {
	var height int64
	err := db.QueryRow("SELECT MAX(block_height) FROM txs WHERE chainid=$1", chainId).Scan(&height)
	if err != nil {
		return 1, err
	}
	return height, nil
}

func getTransfersForPeriod(chainId, channel string, db *sql.DB, start, end time.Time) (int64, error) {
	query := "SELECT count(*) " +
		"FROM txs " +
		"INNER JOIN msg_transfer msg ON msg.tx_hash=txs.hash " +
		"WHERE block_time >= $1 " +
		"AND block_time < $2 " +
		"AND chainid = $3 " +
		"AND src_chan = $4 " +
		"AND code = 0"
	var transfers int64
	err := db.QueryRow(query, start.Format("2006-01-02 15:04:05"), end.Format("2006-01-02 15:04:05"),
		chainId, channel).Scan(&transfers)

	if err != nil {
		return 0, err
	}
	return transfers, nil
}

func getTimeoutsForPeriod(chainId, srcChan, dstChan string, db *sql.DB, start, end time.Time) (int64, error) {
	query := "SELECT count(*) " +
		"FROM txs " +
		"INNER JOIN msg_timeout msg ON msg.tx_hash=txs.hash " +
		"WHERE block_time >= $1 " +
		"AND block_time < $2 " +
		"AND chainid = $3 " +
		"AND src_chan = $4 " +
		"AND dst_chan = $5 " +
		"AND code = 0"
	var timeouts int64
	err := db.QueryRow(query, start.Format("2006-01-02 15:04:05"), end.Format("2006-01-02 15:04:05"),
		chainId, srcChan, dstChan).Scan(&timeouts)

	if err != nil {
		return 0, err
	}
	return timeouts, nil
}

func getRecvPacketsForPeriod(chainId, srcChan, dstChan string, db *sql.DB, start, end time.Time) (int64, error) {
	query := "SELECT count(*) " +
		"FROM txs " +
		"INNER JOIN msg_recvpacket msg ON msg.tx_hash=txs.hash " +
		"WHERE block_time >= $1 " +
		"AND block_time < $2 " +
		"AND chainid = $3 " +
		"AND src_chan = $4 " +
		"AND dst_chan = $5 " +
		"AND code = 0"
	var recvPackets int64
	err := db.QueryRow(query, start.Format("2006-01-02 15:04:05"), end.Format("2006-01-02 15:04:05"),
		chainId, srcChan, dstChan).Scan(&recvPackets)

	if err != nil {
		return 0, err
	}
	return recvPackets, nil
}

func getTransferedAmounts(chain *relayer.Chain, path string, start, end time.Time, db *sql.DB) (map[string]int64, error) {
	amounts := make(map[string]int64, 1)
	query := "SELECT sum(amount::bigint), denom " +
		"FROM msg_transfer " +
		"INNER JOIN txs tx ON msg_transfer.tx_hash=tx.hash " +
		"WHERE block_time >= $1 " +
		"AND block_time < $2 " +
		"AND chainid = $3 " +
		"AND src_chan = $4 " +
		"AND code = 0 " +
		"GROUP BY denom"
	rows, err := db.Query(query, start.Format("2006-01-02 15:04:05"), end.Format("2006-01-02 15:04:05"), chain.ChainID, path)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var denom string
	var amount int64
	for rows.Next() {
		err = rows.Scan(&amount, &denom)
		if err != nil {
			return nil, err
		}

		if strings.Contains(denom, "ibc/") {
			denomRes, err := chain.QueryDenomTrace(strings.Trim(denom, "ibc/"))
			if err != nil {
				fmt.Printf("Error querying denom trace %s. Err: %s \n", denom, err.Error())
			} else {
				denom = denomRes.DenomTrace.BaseDenom
			}
		}
		amounts[denom] = amount
	}

	if rows.Err() != nil {
		return nil, err
	}
	return amounts, nil
}

func logTxInsertion(err error, msgIndex, msgs, txs int, height int64) {
	if err != nil {
		fmt.Printf("[Height %d] {%d/%d txs} - Failed to write tx to db. Err: %s \n", height, msgIndex+1, txs, err.Error())
	} else {
		fmt.Printf("[Height %d] {%d/%d txs} - Successfuly wrote tx to db with %d msgs. \n", height, msgIndex+1, txs, msgs)
	}
}

type NetworkDetails struct {
	ChainID     string `yaml:"chain-id" json:"chain-id" mapstructure:"chain-id"`
	Prefix      string `yaml:"prefix" json:"prefix" mapstructure:"prefix"`
	Token       string `yaml:"token" json:"token" mapstructure:"token"`
	CoinGeckoID string `yaml:"coin-gecko-id" json:"coin-gecko-id" mapstructure:"coin-gecko-id"`

	context client.Context
}

type CoinGeckoData struct {
	Networks map[string]*NetworkDetails `yaml:"networks"`
}

type CacheEntry struct {
	value float64
	time  time.Time
}

func DateEqual(date1, date2 time.Time) bool {
	y1, m1, d1 := date1.Date()
	y2, m2, d2 := date2.Date()
	return y1 == y2 && m1 == m2 && d1 == d2
}

func GetPriceAndUpdateCache(cache map[string]*CacheEntry, date time.Time, networkDetails *NetworkDetails) float64 {
	tokenValue, err := networkDetails.getPrice(date)
	if err != nil {
		fmt.Printf("Failed to get price of %s from Coin Gecko. Err: %s\n", networkDetails.Token, err.Error())
	}
	cache[networkDetails.Token] = &CacheEntry{
		value: tokenValue,
		time:  date,
	}
	return tokenValue
}

type ErrRateLimitExceeded error

type priceHistory struct {
	ID     string `json:"id"`
	Symbol string `json:"symbol"`
	Name   string `json:"name"`
	Image  struct {
		Thumb string `json:"thumb"`
		Small string `json:"small"`
	} `json:"image"`
	MarketData struct {
		CurrentPrice map[string]float64 `json:"current_price"`
		MarketCap    map[string]float64 `json:"market_cap"`
		TotalVolume  map[string]float64 `json:"total_volume"`
	} `json:"market_data"`
}

func buildCoinGeckoData() *CoinGeckoData {
	const fileName = "coin-gecko.yaml"

	// Parse coin-gecko.yaml & build NetworkDetails slice
	networkDetails := &CoinGeckoData{Networks: make(map[string]*NetworkDetails)}
	file, err := ioutil.ReadFile(path.Join(".", "go", "src", "github.com", "strangelove-ventures", "relayer", fileName))
	if err != nil {
		fmt.Println("Error reading file:", err)
		os.Exit(1)
	}

	err = yaml.Unmarshal(file, networkDetails)
	if err != nil {
		fmt.Println("Error unmarshalling coin gecko token value file (coin-gecko.yaml):", err)
		os.Exit(1)
	}

	for _, nd := range networkDetails.Networks {
		networkDetails.Networks[nd.Token] = nd
	}
	return networkDetails
}

func (nd *NetworkDetails) getPrice(date time.Time) (float64, error) {
	url := fmt.Sprintf("https://api.coingecko.com/api/v3/coins/%s/history?date=%s&localization=false", nd.CoinGeckoID, fmt.Sprintf("%d-%d-%d", date.Day(), date.Month(), date.Year()))
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Accept", "application/json")

	var resp *http.Response
	err = retry.Do(func() error {
		resp, err = http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		switch {
		case resp.StatusCode == 429:
			return ErrRateLimitExceeded(fmt.Errorf("429"))
		case resp.StatusCode < 200 || resp.StatusCode > 299:
			return fmt.Errorf("non 2xx or 429 status code %d", resp.StatusCode)
		default:
			return nil
		}
	}, retry.RetryIf(func(err error) bool {
		_, ok := err.(ErrRateLimitExceeded)
		fmt.Println("Hit rate limit for Coin Gecko API, must wait 60 seconds...")
		return ok
	}), retry.Delay(time.Second*60))
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	bz, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return 0, err
	}

	data := priceHistory{}
	if err := json.Unmarshal(bz, &data); err != nil {
		return 0, err
	}

	return data.MarketData.CurrentPrice["usd"], nil
}
