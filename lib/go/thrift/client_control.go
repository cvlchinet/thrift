// (c) Chi Vinh Le <cvl@chinet.info> – 08.10.2014

package thrift

// Behaviour of client is controlled using this interface
type TClientControl interface {
	// Called to ask if rpc should run this time. Retry can be implemented using this
	ShouldRun(serviceName string, count int, err error) bool
	// Called before executing a rpc call.
	AcquireProtocols(serviceName string) (in TProtocol, out TProtocol, err error)
	// Called before receiving a rpc packet.
	WaitForDelivery(serviceName string, in TProtocol, seqId int32) (err error)
}

type TClientControlFactory interface {
	ClientControl() TClientControl
}

// Schedulers that just returns the TProtocols given was set during creation. Allows traditional synchronous rpc.
type TSimpleClientControl struct {
	in  TProtocol
	out TProtocol
}

func NewTSimpleClientControl(in TProtocol, out TProtocol) *TSimpleClientControl {
	return &TSimpleClientControl{in: in, out: out}
}

func (self *TSimpleClientControl) ShouldRun(serviceName string, count int, err error) bool {
	if count == 0 {
		return true
	}
	return false
}

func (self *TSimpleClientControl) AcquireProtocols(serviceName string) (in TProtocol, out TProtocol, err error) {
	err = nil
	in = self.in
	out = self.out
	return
}

func (self *TSimpleClientControl) WaitForDelivery(serviceName string, in TProtocol, seqId int32) (err error) {
	return nil
}
