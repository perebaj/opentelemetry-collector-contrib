// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package internal // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/prometheusreceiver/internal"

import (
	"context"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/receiver/receiverhelper"
)

// appendableV2 is a wrapper around the appendable struct that implements the storage.AppendableV2 interface.
type appendableV2 struct {
	sink           consumer.Metrics
	useMetadata    bool
	trimSuffixes   bool
	externalLabels labels.Labels

	settings receiver.Settings
	obsrecv  *receiverhelper.ObsReport
}

// NewAppendableV2 returns a storage.AppendableV2 instance that emits metrics to the sink.
func NewAppendableV2(
	sink consumer.Metrics,
	settings receiver.Settings,
	useMetadata bool,
	externalLabels labels.Labels,
	trimSuffixes bool,
) (storage.AppendableV2, error) {
	obsrecv, err := receiverhelper.NewObsReport(receiverhelper.ObsReportSettings{ReceiverID: settings.ID, Transport: transport, ReceiverCreateSettings: settings})
	if err != nil {
		return nil, err
	}
	return &appendableV2{
		sink:           sink,
		settings:       settings,
		useMetadata:    useMetadata,
		externalLabels: externalLabels,
		trimSuffixes:   trimSuffixes,
		obsrecv:        obsrecv,
	}, nil
}

// AppenderV2 implements the storage.AppendableV2 interface.
func (a *appendableV2) AppenderV2(ctx context.Context) storage.AppenderV2 {
	return newTransactionV2(ctx, a.sink, a.externalLabels, a.settings, a.obsrecv, a.trimSuffixes, a.useMetadata)
}
