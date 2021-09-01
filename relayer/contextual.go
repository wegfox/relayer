package relayer

import (
	"github.com/cosmos/cosmos-sdk/codec"
	"github.com/gogo/protobuf/proto"
)

type contextualStdCodec struct {
	codec.Codec
	useContext func() func()
}

var _ codec.Codec = &contextualStdCodec{}

// newContextualStdCodec creates a codec that sets and resets context
func newContextualStdCodec(cdc codec.Codec, useContext func() func()) *contextualStdCodec {
	return &contextualStdCodec{
		Codec:      cdc,
		useContext: useContext,
	}
}

// MarshalJSON marshals with the original codec and new context
func (cdc *contextualStdCodec) MarshalJSON(ptr proto.Message) ([]byte, error) {
	done := cdc.useContext()
	bz, err := cdc.Codec.MarshalJSON(ptr)
	done()

	return bz, err
}

func (cdc *contextualStdCodec) MustMarshalJSON(ptr proto.Message) []byte {
	out, err := cdc.MarshalJSON(ptr)
	if err != nil {
		panic(err)
	}
	return out
}

// UnmarshalJSON unmarshals with the original codec and new context
func (cdc *contextualStdCodec) UnmarshalJSON(bz []byte, ptr proto.Message) error {
	done := cdc.useContext()
	err := cdc.Codec.UnmarshalJSON(bz, ptr)
	done()

	return err
}

func (cdc *contextualStdCodec) MustUnmarshalJSON(bz []byte, ptr proto.Message) {
	if err := cdc.UnmarshalJSON(bz, ptr); err != nil {
		panic(err)
	}
}

func (cdc *contextualStdCodec) Marshal(ptr codec.ProtoMarshaler) ([]byte, error) {
	done := cdc.useContext()
	bz, err := cdc.Codec.Marshal(ptr)
	done()

	return bz, err
}

func (cdc *contextualStdCodec) MustMarshal(ptr codec.ProtoMarshaler) []byte {
	out, err := cdc.Marshal(ptr)
	if err != nil {
		panic(err)
	}
	return out
}

func (cdc *contextualStdCodec) Unmarshal(bz []byte, ptr codec.ProtoMarshaler) error {
	done := cdc.useContext()
	err := cdc.Codec.Unmarshal(bz, ptr)
	done()

	return err
}

func (cdc *contextualStdCodec) MustUnmarshal(bz []byte, ptr codec.ProtoMarshaler) {
	if err := cdc.Unmarshal(bz, ptr); err != nil {
		panic(err)
	}
}
