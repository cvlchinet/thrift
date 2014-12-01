/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package tests

import (
	"code.google.com/p/gomock/gomock"
	"errors"
	"errortest"
	"testing"
	"thrift"
)

type ErrorTestHandler struct {
}

func NewErrorTestHandler() *ErrorTestHandler {
	return &ErrorTestHandler{}
}

func (p *ErrorTestHandler) TestStruct(thing *errortest.TestStruct) (r *errortest.TestStruct, err error) {
	return thing, nil
}

func (p *ErrorTestHandler) TestString(s string) (r string, err error) {
	return s, nil
}

func (p *ErrorTestHandler) TestFail() (err error) {
	return errors.New("test fail")
}

func (p *ErrorTestHandler) TestOneWayFail() (err error) {
	return errors.New("test oneway fail")
}

// TestCase: Requested method is unknown.
// This will setup mock according the test case and fail at given position with given error.
// Returns true if failing position is valid.
func prepareProcessUnkownFunction(protocol *MockTProtocol, failAt int, failWith error) bool {
	var err error = nil

	// read a message with unkown function
	if failAt == 0 {
		err = failWith
	}
	last := protocol.EXPECT().ReadMessageBegin().Return("testUnkown", thrift.CALL, int32(1), err)
	if failAt == 0 {
		return true
	}

	// process should skip sruct afterwards and finish reading
	if failAt == 1 {
		err = failWith
	}
	last = protocol.EXPECT().Skip(thrift.TType(thrift.STRUCT)).Return(err).After(last)
	if failAt == 1 {
		return true
	}
	if failAt == 2 {
		err = failWith
	}
	last = protocol.EXPECT().ReadMessageEnd().Return(err).After(last)
	if failAt == 2 {
		return true
	}

	// process should send TApplicationException and flush.
	if failAt == 3 {
		err = failWith
	}
	last = protocol.EXPECT().WriteMessageBegin("testUnkown", thrift.EXCEPTION, int32(1)).Return(err)
	if failAt == 3 {
		return true
	}
	if failAt == 4 {
		err = failWith
	}
	last = protocol.EXPECT().WriteStructBegin("TApplicationException").Return(err).After(last)
	if failAt == 4 {
		return true
	}
	if failAt == 5 {
		err = failWith
	}
	last = protocol.EXPECT().WriteFieldBegin("message", thrift.TType(thrift.STRING), int16(1)).Return(err).After(last)
	if failAt == 5 {
		return true
	}
	if failAt == 6 {
		err = failWith
	}
	last = protocol.EXPECT().WriteString("Unknown function testUnkown").Return(err).After(last)
	if failAt == 6 {
		return true
	}
	if failAt == 7 {
		err = failWith
	}
	last = protocol.EXPECT().WriteFieldEnd().Return(err).After(last)
	if failAt == 7 {
		return true
	}
	if failAt == 8 {
		err = failWith
	}
	last = protocol.EXPECT().WriteFieldBegin("type", thrift.TType(thrift.I32), int16(2)).Return(err).After(last)
	if failAt == 8 {
		return true
	}
	if failAt == 9 {
		err = failWith
	}
	last = protocol.EXPECT().WriteI32(int32(thrift.UNKNOWN_METHOD)).Return(err).After(last)
	if failAt == 9 {
		return true
	}
	if failAt == 10 {
		err = failWith
	}
	last = protocol.EXPECT().WriteFieldEnd().Return(err).After(last)
	if failAt == 10 {
		return true
	}
	if failAt == 11 {
		err = failWith
	}
	last = protocol.EXPECT().WriteFieldStop().Return(err).After(last)
	if failAt == 11 {
		return true
	}
	if failAt == 12 {
		err = failWith
	}
	last = protocol.EXPECT().WriteStructEnd().Return(err).After(last)
	if failAt == 12 {
		return true
	}
	if failAt == 13 {
		err = failWith
	}
	last = protocol.EXPECT().WriteMessageEnd().Return(err).After(last)
	if failAt == 13 {
		return true
	}
	if failAt == 14 {
		err = failWith
	}
	last = protocol.EXPECT().Flush().Return(err).After(last)
	if failAt == 14 {
		return true
	}

	return false
}

// TestCase: Requested method is unknown.
func TestProcessUnknownFunction(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	err := thrift.NewTTransportException(0, "test")
	processor := errortest.NewErrorTestProcessor(NewErrorTestHandler())
	for i := 0; ; i++ {
		protocol := NewMockTProtocol(mockCtrl)
		willFail := prepareProcessUnkownFunction(protocol, i, err)
		success, err2 := processor.Process(protocol, protocol)
		mockCtrl.Finish()
		if success {
			t.Fatal("Expected process to fail")
		}
		if willFail {
			if err2 != err {
				t.Fatal("Expected different error")
			}
		} else {
			err3, ok := err2.(thrift.TApplicationException)
			if !ok {
				t.Fatal("Expected a TApplicationException")
			}
			if err3.TypeId() != thrift.UNKNOWN_METHOD {
				t.Fatal("Expected a TApplicationException with typeId UNKNOWN_METHOD")
			}
			return
		}
	}
}

// TestCase: Requested method exists but reading full message fails.
// This will setup mock according the test case and fail at given position with given error.
// Returns true if failing position is valid.
func prepareProcessMethodReadFail(protocol *MockTProtocol, failAt int, failWith error) bool {
	var err error = nil
	var last *gomock.Call

	// Process should send TApplicationException and flush.
	// This function will prepare mock for that.
	var prepareWrite = func() bool {
		// reset error
		err = nil

		if failAt == 6 {
			err = failWith
		}
		last = protocol.EXPECT().WriteMessageBegin("testString", thrift.EXCEPTION, int32(2)).Return(err)
		if failAt == 6 {
			return true
		}
		if failAt == 7 {
			err = failWith
		}
		last = protocol.EXPECT().WriteStructBegin("TApplicationException").Return(err).After(last)
		if failAt == 7 {
			return true
		}
		if failAt == 8 {
			err = failWith
		}
		last = protocol.EXPECT().WriteFieldBegin("message", thrift.TType(thrift.STRING), int16(1)).Return(err).After(last)
		if failAt == 8 {
			return true
		}
		if failAt == 9 {
			err = failWith
		}
		last = protocol.EXPECT().WriteString(gomock.Any()).Return(err).After(last)
		if failAt == 9 {
			return true
		}
		if failAt == 10 {
			err = failWith
		}
		last = protocol.EXPECT().WriteFieldEnd().Return(err).After(last)
		if failAt == 10 {
			return true
		}
		if failAt == 11 {
			err = failWith
		}
		last = protocol.EXPECT().WriteFieldBegin("type", thrift.TType(thrift.I32), int16(2)).Return(err).After(last)
		if failAt == 11 {
			return true
		}
		if failAt == 12 {
			err = failWith
		}
		last = protocol.EXPECT().WriteI32(int32(thrift.PROTOCOL_ERROR)).Return(err).After(last)
		if failAt == 12 {
			return true
		}
		if failAt == 13 {
			err = failWith
		}
		last = protocol.EXPECT().WriteFieldEnd().Return(err).After(last)
		if failAt == 13 {
			return true
		}
		if failAt == 14 {
			err = failWith
		}
		last = protocol.EXPECT().WriteFieldStop().Return(err).After(last)
		if failAt == 14 {
			return true
		}
		if failAt == 15 {
			err = failWith
		}
		last = protocol.EXPECT().WriteStructEnd().Return(err).After(last)
		if failAt == 15 {
			return true
		}
		if failAt == 16 {
			err = failWith
		}
		last = protocol.EXPECT().WriteMessageEnd().Return(err).After(last)
		if failAt == 16 {
			return true
		}
		if failAt == 17 {
			err = failWith
		}
		last = protocol.EXPECT().Flush().Return(err).After(last)
		if failAt == 17 {
			return true
		}
		return false
	}

	// Reading message header succeeds.
	last = protocol.EXPECT().ReadMessageBegin().Return("testString", thrift.CALL, int32(2), nil)

	// But message body will fail at some point.
	if failAt == 0 {
		err = failWith
	}
	last = protocol.EXPECT().ReadStructBegin().Return("testString_args", err).After(last)
	if failAt == 0 {
		prepareWrite()
		return true
	}
	if failAt == 1 {
		err = failWith
	}
	last = protocol.EXPECT().ReadFieldBegin().Return("s", thrift.TType(thrift.STRING), int16(1), err).After(last)
	if failAt == 1 {
		prepareWrite()
		return true
	}
	if failAt == 2 {
		err = failWith
	}
	last = protocol.EXPECT().ReadString().Return("test", err).After(last)
	if failAt == 2 {
		prepareWrite()
		return true
	}
	if failAt == 3 {
		err = failWith
	}
	last = protocol.EXPECT().ReadFieldEnd().Return(err).After(last)
	if failAt == 3 {
		prepareWrite()
		return true
	}
	if failAt == 4 {
		err = failWith
	}
	last = protocol.EXPECT().ReadFieldBegin().Return("_", thrift.TType(thrift.STOP), int16(2), err).After(last)
	if failAt == 4 {
		prepareWrite()
		return true
	}
	if failAt == 5 {
		err = failWith
	}
	last = protocol.EXPECT().ReadStructEnd().Return(err).After(last)
	if failAt == 5 {
		prepareWrite()
		return true
	}
	// As mentioned above. Reading message body will fail definitely in this test case.
	last = protocol.EXPECT().ReadMessageEnd().Return(failWith).After(last)

	return prepareWrite()
}

// TestCase: Requested method exists but reading full message fails.
func TestProcessMethodReadFail(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	err := thrift.NewTTransportException(thrift.TIMED_OUT, "test")
	processor := errortest.NewErrorTestProcessor(NewErrorTestHandler())
	for i := 0; ; i++ {
		protocol := NewMockTProtocol(mockCtrl)
		if !prepareProcessMethodReadFail(protocol, i, err) {
			break
		}
		success, err2 := processor.Process(protocol, protocol)
		if success {
			t.Fatal("Expected process to fail")
		}

		err3, ok := err2.(thrift.TTransportException)
		if !ok {
			t.Fatal("Expected a TTrasportException")
		}
		if err3.TypeId() != thrift.TIMED_OUT {
			t.Fatal("Expected TIMED_OUT error")
		}

		mockCtrl.Finish()
	}
}

// TestCase: Requested method exists, read was a success and handler does not fail.
// This will setup mock according the test case and fail at given position with given error.
// Returns true if failing position is valid.
func prepareProcessHandlerSuccess(protocol *MockTProtocol, failAt int, failWith error) bool {
	var err error = nil

	// Reading message succeeds.
	last := protocol.EXPECT().ReadMessageBegin().Return("testString", thrift.CALL, int32(2), nil)
	last = protocol.EXPECT().ReadStructBegin().Return("testString_args", nil).After(last)
	last = protocol.EXPECT().ReadFieldBegin().Return("s", thrift.TType(thrift.STRING), int16(1), nil).After(last)
	last = protocol.EXPECT().ReadString().Return("test", nil).After(last)
	last = protocol.EXPECT().ReadFieldEnd().Return(nil).After(last)
	last = protocol.EXPECT().ReadFieldBegin().Return("_", thrift.TType(thrift.STOP), int16(2), nil).After(last)
	last = protocol.EXPECT().ReadStructEnd().Return(nil).After(last)
	last = protocol.EXPECT().ReadMessageEnd().Return(nil).After(last)

	// Process should send reply.
	if failAt == 0 {
		err = failWith
	}
	last = protocol.EXPECT().WriteMessageBegin("testString", thrift.REPLY, int32(2)).Return(err)
	if failAt == 0 {
		return true
	}
	if failAt == 1 {
		err = failWith
	}
	last = protocol.EXPECT().WriteStructBegin("testString_result").Return(err).After(last)
	if failAt == 1 {
		return true
	}
	if failAt == 2 {
		err = failWith
	}
	last = protocol.EXPECT().WriteFieldBegin("success", thrift.TType(thrift.STRING), int16(0)).Return(err).After(last)
	if failAt == 2 {
		return true
	}
	if failAt == 3 {
		err = failWith
	}
	last = protocol.EXPECT().WriteString("test").Return(err).After(last)
	if failAt == 3 {
		return true
	}
	if failAt == 4 {
		err = failWith
	}
	last = protocol.EXPECT().WriteFieldEnd().Return(err).After(last)
	if failAt == 4 {
		return true
	}
	if failAt == 5 {
		err = failWith
	}
	last = protocol.EXPECT().WriteFieldStop().Return(err).After(last)
	if failAt == 5 {
		return true
	}
	if failAt == 6 {
		err = failWith
	}
	last = protocol.EXPECT().WriteStructEnd().Return(err).After(last)
	if failAt == 6 {
		return true
	}
	if failAt == 7 {
		err = failWith
	}
	last = protocol.EXPECT().WriteMessageEnd().Return(err).After(last)
	if failAt == 7 {
		return true
	}
	if failAt == 8 {
		err = failWith
	}
	last = protocol.EXPECT().Flush().Return(err).After(last)
	if failAt == 8 {
		return true
	}
	return false
}

// TestCase: Requested method exists, read was a success and handler does not fail.
func TestProcessHandlerSuccess(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	err := thrift.NewTTransportException(thrift.TIMED_OUT, "test")
	processor := errortest.NewErrorTestProcessor(NewErrorTestHandler())
	for i := 0; ; i++ {
		protocol := NewMockTProtocol(mockCtrl)
		willFail := prepareProcessHandlerSuccess(protocol, i, err)
		success, err2 := processor.Process(protocol, protocol)
		mockCtrl.Finish()
		if willFail {
			if success {
				t.Fatal("Expected process to fail")
			}
			err3, ok := err2.(thrift.TTransportException)
			if !ok {
				t.Fatal("Expected a TTrasportException")
			}
			if err3.TypeId() != thrift.TIMED_OUT {
				t.Fatal("Expected TIMED_OUT error")
			}
		} else {
			if !success {
				t.Fatal("Expected process to succeed")
			}
			if err2 != nil {
				t.Fatal("Expected error to be nil")
			}
			break
		}
	}
}

// TestCase: Requested method exists, read was a success but handler fails unexpectedly.
// This will setup mock according the test case and fail at given position with given error.
// Returns true if failing position is valid.
func prepareProcessHandlerFail(protocol *MockTProtocol, failAt int, failWith error) bool {
	var err error = nil

	// Reading message succeeds.
	last := protocol.EXPECT().ReadMessageBegin().Return("testFail", thrift.CALL, int32(2), nil)
	last = protocol.EXPECT().ReadStructBegin().Return("testFail_args", nil).After(last)
	last = protocol.EXPECT().ReadFieldBegin().Return("_", thrift.TType(thrift.STOP), int16(2), nil).After(last)
	last = protocol.EXPECT().ReadStructEnd().Return(nil).After(last)
	last = protocol.EXPECT().ReadMessageEnd().Return(nil).After(last)

	// Process should send exception because handler failed.
	if failAt == 0 {
		err = failWith
	}
	last = protocol.EXPECT().WriteMessageBegin("testFail", thrift.EXCEPTION, int32(2)).Return(err).After(last)
	if failAt == 0 {
		return true
	}
	if failAt == 1 {
		err = failWith
	}
	last = protocol.EXPECT().WriteStructBegin("TApplicationException").Return(err).After(last)
	if failAt == 1 {
		return true
	}
	if failAt == 2 {
		err = failWith
	}
	last = protocol.EXPECT().WriteFieldBegin("message", thrift.TType(thrift.STRING), int16(1)).Return(err).After(last)
	if failAt == 2 {
		return true
	}
	if failAt == 3 {
		err = failWith
	}
	last = protocol.EXPECT().WriteString(gomock.Any()).Return(err).After(last)
	if failAt == 3 {
		return true
	}
	if failAt == 4 {
		err = failWith
	}
	last = protocol.EXPECT().WriteFieldEnd().Return(err).After(last)
	if failAt == 4 {
		return true
	}
	if failAt == 5 {
		err = failWith
	}
	last = protocol.EXPECT().WriteFieldBegin("type", thrift.TType(thrift.I32), int16(2)).Return(err).After(last)
	if failAt == 5 {
		return true
	}
	if failAt == 6 {
		err = failWith
	}
	last = protocol.EXPECT().WriteI32(int32(thrift.INTERNAL_ERROR)).Return(err).After(last)
	if failAt == 6 {
		return true
	}
	if failAt == 7 {
		err = failWith
	}
	last = protocol.EXPECT().WriteFieldEnd().Return(err).After(last)
	if failAt == 7 {
		return true
	}
	if failAt == 8 {
		err = failWith
	}
	last = protocol.EXPECT().WriteFieldStop().Return(err).After(last)
	if failAt == 8 {
		return true
	}
	if failAt == 9 {
		err = failWith
	}
	last = protocol.EXPECT().WriteStructEnd().Return(err).After(last)
	if failAt == 9 {
		return true
	}
	if failAt == 10 {
		err = failWith
	}
	last = protocol.EXPECT().WriteMessageEnd().Return(err).After(last)
	if failAt == 10 {
		return true
	}
	if failAt == 11 {
		err = failWith
	}
	last = protocol.EXPECT().Flush().Return(err).After(last)
	if failAt == 11 {
		return true
	}
	return false
}

// TestCase: Requested method exists, read was a success but handler fails unexpectedly.
func TestProcessHandlerFail(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	err := thrift.NewTTransportException(thrift.TIMED_OUT, "test")
	processor := errortest.NewErrorTestProcessor(NewErrorTestHandler())
	for i := 0; ; i++ {
		protocol := NewMockTProtocol(mockCtrl)
		willFail := prepareProcessHandlerFail(protocol, i, err)
		success, err2 := processor.Process(protocol, protocol)
		mockCtrl.Finish()
		if willFail {
			if success {
				t.Fatal("Expected process to fail")
			}
			err3, ok := err2.(thrift.TTransportException)
			if !ok {
				t.Fatal("Expected a TTransportException")
			}
			if err3.TypeId() != thrift.TIMED_OUT {
				t.Fatal("Expected TIMED_OUT error")
			}
		} else {
			if !success {
				t.Fatal("Expected process to succeed")
			}
			if err2.Error() != "test fail" {
				t.Fatal("Expected error with message \"test fail\"")
			}
			break
		}
	}
}

// TestCase: Requested ONEWAY method exists, read was a success but handler fails unexpectedly.
func TestProcessHandlerOneWayFail(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	processor := errortest.NewErrorTestProcessor(NewErrorTestHandler())
	protocol := NewMockTProtocol(mockCtrl)

	gomock.InOrder(
		protocol.EXPECT().ReadMessageBegin().Return("testOneWayFail", thrift.ONEWAY, int32(1), nil),
		protocol.EXPECT().ReadStructBegin().Return("testOneWayFail_args", nil),
		protocol.EXPECT().ReadFieldBegin().Return("_", thrift.TType(thrift.STOP), int16(1), nil),
		protocol.EXPECT().ReadStructEnd().Return(nil),
		protocol.EXPECT().ReadMessageEnd().Return(nil),
	)

	success, err := processor.Process(protocol, protocol)
	mockCtrl.Finish()
	if !success {
		t.Fatal("Expected process to succeed")
	}
	if err.Error() != "test oneway fail" {
		t.Fatal("Expected error with message \"test oneway fail\"")
	}
}

// TestCase: Wrong message type has been received.
func TestProcessWrongMessageType(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	processor := errortest.NewErrorTestProcessor(NewErrorTestHandler())
	protocol := NewMockTProtocol(mockCtrl)
	protocol.EXPECT().ReadMessageBegin().Return("testString", thrift.INVALID_TMESSAGE_TYPE, int32(1), nil)

	success, err := processor.Process(protocol, protocol)
	mockCtrl.Finish()
	if success {
		t.Fatal("Expected process to fail")
	}
	err2, ok := err.(thrift.TApplicationException)
	if !ok {
		t.Fatal("Expected a TApplicationException")
	}
	if err2.TypeId() != thrift.INVALID_MESSAGE_TYPE_EXCEPTION {
		t.Fatal("Expected a TApplicationException with typeId INVALID_MESSAGE_TYPE_EXCEPTION")
	}
}
