/*
 * Copyright (c) 2026, WSO2 LLC. (https://www.wso2.com).
 *
 * WSO2 LLC. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package asb

import (
	"context"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
)

// messageReceiver is the subset of *azservicebus.Receiver the consume path
// uses. The SDK's type is concrete and its methods open AMQP links, which left
// handleMessage unreachable from a test — and handleMessage is where a message
// is settled, the difference between delivering an event once and twice. The
// settle bug that shipped in this file got through precisely because no test
// could call it.
type messageReceiver interface {
	ReceiveMessages(ctx context.Context, maxMessages int, options *azservicebus.ReceiveMessagesOptions) (
		[]*azservicebus.ReceivedMessage, error)
	CompleteMessage(ctx context.Context, message *azservicebus.ReceivedMessage,
		options *azservicebus.CompleteMessageOptions) error
	DeadLetterMessage(ctx context.Context, message *azservicebus.ReceivedMessage,
		options *azservicebus.DeadLetterOptions) error
	RenewMessageLock(ctx context.Context, message *azservicebus.ReceivedMessage,
		options *azservicebus.RenewMessageLockOptions) error
	Close(ctx context.Context) error
}

// messageSender is the subset of *azservicebus.Sender the publish path uses,
// for the same reason: Publish encodes the message, and what a fake records is
// the only way to assert what actually reached the wire without a namespace.
type messageSender interface {
	SendMessage(ctx context.Context, message *azservicebus.Message, options *azservicebus.SendMessageOptions) error
	Close(ctx context.Context) error
}

// Compile-time proof the SDK types still satisfy the seams. An SDK upgrade that
// changes a signature fails here rather than at the assignment in NewConsumer.
var (
	_ messageReceiver = (*azservicebus.Receiver)(nil)
	_ messageSender   = (*azservicebus.Sender)(nil)
)
