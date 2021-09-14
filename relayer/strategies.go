package relayer

import (
	"fmt"
	"time"
)

var (
	txEvents = "tm.event='Tx'"
	blEvents = "tm.event='NewBlock'"
)

// Strategy defines
type Strategy interface {
	GetType() string
	UnrelayedSequences(src, dst *Chain) (*RelaySequences, error)
	UnrelayedAcknowledgements(src, dst *Chain) (*RelaySequences, error)
	RelayPackets(src, dst *Chain, sp *RelaySequences) error
	RelayAcknowledgements(src, dst *Chain, sp *RelaySequences) error
}

// MustGetStrategy returns the strategy and panics on error
func (p *Path) MustGetStrategy() Strategy {
	strategy, err := p.GetStrategy()
	if err != nil {
		panic(err)
	}

	return strategy
}

// GetStrategy the strategy defined in the relay messages
func (p *Path) GetStrategy() (Strategy, error) {
	switch p.Strategy.Type {
	case (&NaiveStrategy{}).GetType():
		return &NaiveStrategy{}, nil
	default:
		return nil, fmt.Errorf("invalid strategy: %s", p.Strategy.Type)
	}
}

// StrategyCfg defines which relaying strategy to take for a given path
type StrategyCfg struct {
	Type string `json:"type" yaml:"type"`
}

// RunStrategy runs a given strategy
func RunStrategy(src, dst *Chain, strategy Strategy) (func(), error) {
	doneChan := make(chan struct{})
	// Keep trying until we're timed out or got a result or got an error
	go func() {
		for {
			select {
			// Got a timeout! fail with a timeout error
			case <-doneChan:
				return
			default:
				fmt.Println("tick begin")
				// Fetch any unrelayed sequences depending on the channel order
				sp, err := strategy.UnrelayedSequences(src, dst)
				if err != nil {
					src.Log(fmt.Sprintf("unrelayed sequences error: %s", err))
				}

				fmt.Println("unrelayed sequences passed")
				if err = strategy.RelayPackets(src, dst, sp); err != nil {
					src.Log(fmt.Sprintf("relay packets error: %s", err))
				}

				fmt.Println("relay packets passed")
				ap, err := strategy.UnrelayedAcknowledgements(src, dst)
				if err != nil {
					src.Log(fmt.Sprintf("unrelayed acks error: %s", err))
				}

				fmt.Println("relay acks passed")
				if err = strategy.RelayAcknowledgements(src, dst, ap); err != nil {
					src.Log(fmt.Sprintf("relay acks error: %s", err))
				}
				time.Sleep(100 * time.Millisecond)
			}
		}
	}()

	// Return a function to stop the relayer goroutine
	return func() { doneChan <- struct{}{} }, nil
}
