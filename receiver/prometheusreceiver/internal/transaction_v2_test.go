// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package internal

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/scrape"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/tsdbutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/receiver/receivertest"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func newTxnV2(t *testing.T, useMetadata bool) *transactionV2 {
	ctx := t.Context()
	lbls := labels.FromMap(map[string]string{
		string(model.InstanceLabel): "localhost:1234",
		string(model.JobLabel):      "job-a",
	})
	tgt := scrape.NewTarget(
		lbls,
		&config.ScrapeConfig{},
		map[model.LabelName]model.LabelValue{
			model.AddressLabel: "localhost:1234",
			model.SchemeLabel:  "http",
		},
		nil,
	)
	ctx = scrape.ContextWithTarget(ctx, tgt)
	if useMetadata {
		ctx = scrape.ContextWithMetricMetadataStore(ctx, newFakeMetadataStore(map[string]scrape.MetricMetadata{}))
	}
	sink := &consumertest.MetricsSink{}
	settings := receivertest.NewNopSettings(receivertest.NopType)
	settings.Logger = zap.NewNop()
	return newTransactionV2(ctx, sink, labels.EmptyLabels(), settings, newObs(t), false, useMetadata)
}

// --- Basic transaction lifecycle tests ---

func TestTransactionV2CommitWithoutAdding(t *testing.T) {
	tr := newTransactionV2(scrapeCtx, consumertest.NewNop(), labels.EmptyLabels(), receivertest.NewNopSettings(receivertest.NopType), nopObsRecv(t), false, true)
	assert.NoError(t, tr.Commit())
}

func TestTransactionV2RollbackDoesNothing(t *testing.T) {
	tr := newTransactionV2(scrapeCtx, consumertest.NewNop(), labels.EmptyLabels(), receivertest.NewNopSettings(receivertest.NopType), nopObsRecv(t), false, true)
	assert.NoError(t, tr.Rollback())
}

// --- Append error handling tests ---

func TestTransactionV2AppendNoTarget(t *testing.T) {
	badLabels := labels.FromStrings(model.MetricNameLabel, "counter_test")
	tr := newTransactionV2(scrapeCtx, consumertest.NewNop(), labels.EmptyLabels(), receivertest.NewNopSettings(receivertest.NopType), nopObsRecv(t), false, true)
	_, err := tr.Append(0, badLabels, 0, time.Now().Unix()*1000, 1.0, nil, nil, storage.AOptions{})
	assert.Error(t, err)
}

func TestTransactionV2AppendNoMetricName(t *testing.T) {
	jobNotFoundLb := labels.FromMap(map[string]string{
		model.InstanceLabel: "localhost:8080",
		model.JobLabel:      "test2",
	})
	sink := new(consumertest.MetricsSink)
	tr := newTransactionV2(scrapeCtx, sink, labels.EmptyLabels(), receivertest.NewNopSettings(receivertest.NopType), nopObsRecv(t), false, true)
	_, err := tr.Append(0, jobNotFoundLb, 0, time.Now().Unix()*1000, 1.0, nil, nil, storage.AOptions{})
	assert.ErrorIs(t, err, errMetricNameNotFound)
	assert.ErrorIs(t, tr.Commit(), errNoDataToBuild)
}

func TestTransactionV2AppendEmptyMetricName(t *testing.T) {
	tr := newTransactionV2(scrapeCtx, consumertest.NewNop(), labels.EmptyLabels(), receivertest.NewNopSettings(receivertest.NopType), nopObsRecv(t), false, true)
	_, err := tr.Append(0, labels.FromMap(map[string]string{
		model.InstanceLabel:   "localhost:8080",
		model.JobLabel:        "test2",
		model.MetricNameLabel: "",
	}), 0, time.Now().Unix()*1000, 1.0, nil, nil, storage.AOptions{})
	assert.ErrorIs(t, err, errMetricNameNotFound)
}

func TestTransactionV2AppendDuplicateLabels(t *testing.T) {
	sink := new(consumertest.MetricsSink)
	tr := newTransactionV2(scrapeCtx, sink, labels.EmptyLabels(), receivertest.NewNopSettings(receivertest.NopType), nopObsRecv(t), false, true)

	dupLabels := labels.FromStrings(
		model.InstanceLabel, "0.0.0.0:8855",
		model.JobLabel, "test",
		model.MetricNameLabel, "counter_test",
		"a", "1",
		"a", "6",
		"z", "9",
	)

	_, err := tr.Append(0, dupLabels, 0, 1917, 1.0, nil, nil, storage.AOptions{})
	assert.ErrorContains(t, err, `invalid sample: non-unique label names: "a"`)
}

func TestTransactionV2AppendContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(scrapeCtx)
	cancel()
	sink := new(consumertest.MetricsSink)
	tr := newTransactionV2(ctx, sink, labels.EmptyLabels(), receivertest.NewNopSettings(receivertest.NopType), nopObsRecv(t), false, true)

	_, err := tr.Append(0, labels.FromStrings(
		model.InstanceLabel, "localhost:8080",
		model.JobLabel, "test",
		model.MetricNameLabel, "counter_test",
	), 0, time.Now().Unix()*1000, 1.0, nil, nil, storage.AOptions{})
	assert.ErrorIs(t, err, errTransactionAborted)
}

// --- Resource and scope tests ---

func TestTransactionV2AppendResource(t *testing.T) {
	sink := new(consumertest.MetricsSink)
	tr := newTransactionV2(scrapeCtx, sink, labels.EmptyLabels(), receivertest.NewNopSettings(receivertest.NopType), nopObsRecv(t), false, true)
	_, err := tr.Append(0, labels.FromMap(map[string]string{
		model.InstanceLabel:   "localhost:8080",
		model.JobLabel:        "test",
		model.MetricNameLabel: "counter_test",
	}), 0, time.Now().Unix()*1000, 1.0, nil, nil, storage.AOptions{})
	assert.NoError(t, err)
	_, err = tr.Append(0, labels.FromMap(map[string]string{
		model.InstanceLabel:   "localhost:8080",
		model.JobLabel:        "test",
		model.MetricNameLabel: startTimeMetricName,
	}), 0, time.Now().UnixMilli(), 1.0, nil, nil, storage.AOptions{})
	assert.NoError(t, err)
	assert.NoError(t, tr.Commit())

	expectedResource := CreateResource("test", "localhost:8080", labels.FromStrings(model.SchemeLabel, "http"))
	mds := sink.AllMetrics()
	require.Len(t, mds, 1)
	gotResource := mds[0].ResourceMetrics().At(0).Resource()
	require.Equal(t, expectedResource, gotResource)
}

func TestTransactionV2AppendMultipleResources(t *testing.T) {
	sink := new(consumertest.MetricsSink)
	tr := newTransactionV2(scrapeCtx, sink, labels.EmptyLabels(), receivertest.NewNopSettings(receivertest.NopType), nopObsRecv(t), false, true)
	_, err := tr.Append(0, labels.FromMap(map[string]string{
		model.InstanceLabel:   "localhost:8080",
		model.JobLabel:        "test-1",
		model.MetricNameLabel: "counter_test",
	}), 0, time.Now().Unix()*1000, 1.0, nil, nil, storage.AOptions{})
	assert.NoError(t, err)
	_, err = tr.Append(0, labels.FromMap(map[string]string{
		model.InstanceLabel:   "localhost:8080",
		model.JobLabel:        "test-2",
		model.MetricNameLabel: startTimeMetricName,
	}), 0, time.Now().UnixMilli(), 1.0, nil, nil, storage.AOptions{})
	assert.NoError(t, err)
	assert.NoError(t, tr.Commit())

	expectedResources := []pcommon.Resource{
		CreateResource("test-1", "localhost:8080", labels.FromStrings(model.SchemeLabel, "http")),
		CreateResource("test-2", "localhost:8080", labels.FromStrings(model.SchemeLabel, "http")),
	}

	mds := sink.AllMetrics()
	require.Len(t, mds, 1)
	require.Equal(t, 2, mds[0].ResourceMetrics().Len())

	for _, expectedResource := range expectedResources {
		foundResource := false
		expectedServiceName, _ := expectedResource.Attributes().Get("service.name")
		for i := 0; i < mds[0].ResourceMetrics().Len(); i++ {
			res := mds[0].ResourceMetrics().At(i).Resource()
			if serviceName, ok := res.Attributes().Get("service.name"); ok {
				if serviceName.AsString() == expectedServiceName.AsString() {
					foundResource = true
					require.Equal(t, expectedResource, res)
					break
				}
			}
		}
		require.True(t, foundResource)
	}
}

func TestReceiverVersionAndNameAreAttachedV2(t *testing.T) {
	sink := new(consumertest.MetricsSink)
	tr := newTransactionV2(scrapeCtx, sink, labels.EmptyLabels(), receivertest.NewNopSettings(receivertest.NopType), nopObsRecv(t), false, true)
	_, err := tr.Append(0, labels.FromMap(map[string]string{
		model.InstanceLabel:   "localhost:8080",
		model.JobLabel:        "test",
		model.MetricNameLabel: "counter_test",
	}), 0, time.Now().Unix()*1000, 1.0, nil, nil, storage.AOptions{})
	assert.NoError(t, err)
	assert.NoError(t, tr.Commit())

	mds := sink.AllMetrics()
	require.Len(t, mds, 1)

	gotScope := mds[0].ResourceMetrics().At(0).ScopeMetrics().At(0).Scope()
	require.Equal(t, "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/prometheusreceiver", gotScope.Name())
	require.Equal(t, component.NewDefaultBuildInfo().Version, gotScope.Version())
}

// --- Histogram tests ---

func TestTransactionV2AppendHistogramNoLe(t *testing.T) {
	sink := new(consumertest.MetricsSink)
	receiverSettings := receivertest.NewNopSettings(receivertest.NopType)
	core, observedLogs := observer.New(zap.InfoLevel)
	receiverSettings.Logger = zap.New(core)
	tr := newTransactionV2(scrapeCtx, sink, labels.EmptyLabels(), receiverSettings, nopObsRecv(t), false, true)

	goodLabels := labels.FromStrings(
		model.InstanceLabel, "0.0.0.0:8855",
		model.JobLabel, "test",
		model.MetricNameLabel, "hist_test_bucket",
	)

	_, err := tr.Append(0, goodLabels, 0, 1917, 1.0, nil, nil, storage.AOptions{})
	require.NoError(t, err)
	assert.Equal(t, 1, observedLogs.Len())
	assert.Equal(t, 1, observedLogs.FilterMessage("failed to add datapoint").Len())

	assert.NoError(t, tr.Commit())
	assert.Empty(t, sink.AllMetrics())
}

// --- Summary tests ---

func TestTransactionV2AppendSummaryNoQuantile(t *testing.T) {
	sink := new(consumertest.MetricsSink)
	receiverSettings := receivertest.NewNopSettings(receivertest.NopType)
	core, observedLogs := observer.New(zap.InfoLevel)
	receiverSettings.Logger = zap.New(core)
	tr := newTransactionV2(scrapeCtx, sink, labels.EmptyLabels(), receiverSettings, nopObsRecv(t), false, true)

	goodLabels := labels.FromStrings(
		model.InstanceLabel, "0.0.0.0:8855",
		model.JobLabel, "test",
		model.MetricNameLabel, "summary_test",
	)

	_, err := tr.Append(0, goodLabels, 0, 1917, 1.0, nil, nil, storage.AOptions{})
	require.NoError(t, err)
	assert.Equal(t, 1, observedLogs.Len())
	assert.Equal(t, 1, observedLogs.FilterMessage("failed to add datapoint").Len())

	assert.NoError(t, tr.Commit())
	assert.Empty(t, sink.AllMetrics())
}

// --- Valid and invalid mixed tests ---

func TestTransactionV2AppendValidAndInvalid(t *testing.T) {
	sink := new(consumertest.MetricsSink)
	receiverSettings := receivertest.NewNopSettings(receivertest.NopType)
	core, observedLogs := observer.New(zap.InfoLevel)
	receiverSettings.Logger = zap.New(core)
	tr := newTransactionV2(scrapeCtx, sink, labels.EmptyLabels(), receiverSettings, nopObsRecv(t), false, true)

	_, err := tr.Append(0, labels.FromMap(map[string]string{
		model.InstanceLabel:   "localhost:8080",
		model.JobLabel:        "test",
		model.MetricNameLabel: "counter_test",
	}), 0, time.Now().Unix()*1000, 1.0, nil, nil, storage.AOptions{})
	assert.NoError(t, err)

	summarylabels := labels.FromStrings(
		model.InstanceLabel, "0.0.0.0:8855",
		model.JobLabel, "test",
		model.MetricNameLabel, "summary_test",
	)

	_, err = tr.Append(0, summarylabels, 0, 1917, 1.0, nil, nil, storage.AOptions{})
	require.NoError(t, err)

	assert.Equal(t, 1, observedLogs.Len())
	assert.Equal(t, 1, observedLogs.FilterMessage("failed to add datapoint").Len())

	assert.NoError(t, tr.Commit())
	expectedResource := CreateResource("test", "localhost:8080", labels.FromStrings(model.SchemeLabel, "http"))
	mds := sink.AllMetrics()
	require.Len(t, mds, 1)
	gotResource := mds[0].ResourceMetrics().At(0).Resource()
	require.Equal(t, expectedResource, gotResource)
	require.Equal(t, 1, mds[0].MetricCount())
}

// --- Federate / fallback label tests ---

func TestTransactionV2AppendWithEmptyLabelArrayFallbackToTargetLabels(t *testing.T) {
	sink := new(consumertest.MetricsSink)

	scrapeTarget := scrape.NewTarget(
		labels.FromMap(map[string]string{
			model.InstanceLabel: "localhost:8080",
			model.JobLabel:      "federate",
		}),
		&config.ScrapeConfig{},
		map[model.LabelName]model.LabelValue{
			model.AddressLabel: "address:8080",
			model.SchemeLabel:  "http",
		},
		nil,
	)

	ctx := scrape.ContextWithMetricMetadataStore(
		scrape.ContextWithTarget(t.Context(), scrapeTarget),
		testMetadataStore(testMetadata))

	tr := newTransactionV2(ctx, sink, labels.EmptyLabels(), receivertest.NewNopSettings(receivertest.NopType), nopObsRecv(t), false, true)

	_, err := tr.Append(0, labels.FromMap(map[string]string{
		model.MetricNameLabel: "counter_test",
	}), 0, time.Now().Unix()*1000, 1.0, nil, nil, storage.AOptions{})
	assert.NoError(t, err)
}

// --- Start timestamp (stMs) tests ---

func TestAppendSTZeroSampleNoLabelsV2(t *testing.T) {
	sink := new(consumertest.MetricsSink)
	tr := newTransactionV2(scrapeCtx, sink, labels.EmptyLabels(), receivertest.NewNopSettings(receivertest.NopType), nopObsRecv(t), false, true)

	_, err := tr.Append(0, labels.FromStrings(), 100, 0, 0, nil, nil, storage.AOptions{})
	assert.ErrorContains(t, err, "job or instance cannot be found from labels")
}

func TestAppendHistogramCTZeroSampleNoLabelsV2(t *testing.T) {
	sink := new(consumertest.MetricsSink)
	tr := newTransactionV2(scrapeCtx, sink, labels.EmptyLabels(), receivertest.NewNopSettings(receivertest.NopType), nopObsRecv(t), false, true)

	_, err := tr.Append(0, labels.FromStrings(), 100, 0, 0, nil, nil, storage.AOptions{})
	assert.ErrorContains(t, err, "job or instance cannot be found from labels")
}

func TestAppendSTZeroSampleDuplicateLabelsV2(t *testing.T) {
	sink := new(consumertest.MetricsSink)
	tr := newTransactionV2(scrapeCtx, sink, labels.EmptyLabels(), receivertest.NewNopSettings(receivertest.NopType), nopObsRecv(t), false, true)

	_, err := tr.Append(0, labels.FromStrings(
		model.InstanceLabel, "0.0.0.0:8855",
		model.JobLabel, "test",
		model.MetricNameLabel, "counter_test",
		"a", "b",
		"a", "c",
	), 100, 0, 0, nil, nil, storage.AOptions{})
	assert.ErrorContains(t, err, "invalid sample: non-unique label names")
}

func TestAppendHistogramCTZeroSampleDuplicateLabelsV2(t *testing.T) {
	sink := new(consumertest.MetricsSink)
	tr := newTransactionV2(scrapeCtx, sink, labels.EmptyLabels(), receivertest.NewNopSettings(receivertest.NopType), nopObsRecv(t), false, true)

	_, err := tr.Append(0, labels.FromStrings(
		model.InstanceLabel, "0.0.0.0:8855",
		model.JobLabel, "test",
		model.MetricNameLabel, "hist_test_bucket",
		"a", "b",
		"a", "c",
	), 100, 0, 0, nil, nil, storage.AOptions{})
	assert.ErrorContains(t, err, "invalid sample: non-unique label names")
}

func TestAppendSTZeroSampleEmptyMetricNameV2(t *testing.T) {
	sink := new(consumertest.MetricsSink)
	tr := newTransactionV2(scrapeCtx, sink, labels.EmptyLabels(), receivertest.NewNopSettings(receivertest.NopType), nopObsRecv(t), false, true)

	_, err := tr.Append(0, labels.FromStrings(
		model.InstanceLabel, "0.0.0.0:8855",
		model.JobLabel, "test",
		model.MetricNameLabel, "",
	), 100, 0, 0, nil, nil, storage.AOptions{})
	assert.ErrorContains(t, err, "metricName not found")
}

func TestAppendHistogramCTZeroSampleEmptyMetricNameV2(t *testing.T) {
	sink := new(consumertest.MetricsSink)
	tr := newTransactionV2(scrapeCtx, sink, labels.EmptyLabels(), receivertest.NewNopSettings(receivertest.NopType), nopObsRecv(t), false, true)

	_, err := tr.Append(0, labels.FromStrings(
		model.InstanceLabel, "0.0.0.0:8855",
		model.JobLabel, "test",
		model.MetricNameLabel, "",
	), 100, 0, 0, nil, nil, storage.AOptions{})
	assert.ErrorContains(t, err, "metricName not found")
}

func TestAppendSTZeroSampleV2(t *testing.T) {
	sink := new(consumertest.MetricsSink)
	tr := newTransactionV2(scrapeCtx, sink, labels.EmptyLabels(), receivertest.NewNopSettings(receivertest.NopType), nopObsRecv(t), false, true)

	var atMs, ctMs int64
	atMs, ctMs = 200, 100

	_, err := tr.Append(0, labels.FromStrings(
		model.InstanceLabel, "0.0.0.0:8855",
		model.JobLabel, "test",
		model.MetricNameLabel, "counter_test",
	), ctMs, atMs, 100, nil, nil, storage.AOptions{})
	assert.NoError(t, err)

	assert.NoError(t, tr.Commit())
	expectedResource := CreateResource("test", "0.0.0.0:8855", labels.FromStrings(model.SchemeLabel, "http"))
	mds := sink.AllMetrics()
	require.Len(t, mds, 1)
	gotResource := mds[0].ResourceMetrics().At(0).Resource()
	require.Equal(t, expectedResource, gotResource)
	require.Equal(t, 1, mds[0].MetricCount())
	require.Equal(
		t,
		pcommon.NewTimestampFromTime(time.UnixMilli(ctMs)),
		mds[0].ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Sum().DataPoints().At(0).StartTimestamp(),
	)
}

func TestAppendHistogramCTZeroSampleV2(t *testing.T) {
	sink := new(consumertest.MetricsSink)
	tr := newTransactionV2(scrapeCtx, sink, labels.EmptyLabels(), receivertest.NewNopSettings(receivertest.NopType), nopObsRecv(t), false, true)

	var atMs, ctMs int64
	atMs, ctMs = 200, 100

	h := tsdbutil.GenerateTestHistogram(1)
	_, err := tr.Append(0, labels.FromStrings(
		model.InstanceLabel, "0.0.0.0:8855",
		model.JobLabel, "test",
		model.MetricNameLabel, "hist_test_bucket",
	), ctMs, atMs, 0, h, nil, storage.AOptions{})
	assert.NoError(t, err)

	assert.NoError(t, tr.Commit())
	expectedResource := CreateResource("test", "0.0.0.0:8855", labels.FromStrings(model.SchemeLabel, "http"))
	mds := sink.AllMetrics()
	require.Len(t, mds, 1)
	gotResource := mds[0].ResourceMetrics().At(0).Resource()
	require.Equal(t, expectedResource, gotResource)
	require.Equal(t, 1, mds[0].MetricCount())
	require.Equal(
		t,
		pcommon.NewTimestampFromTime(time.UnixMilli(ctMs)),
		mds[0].ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).ExponentialHistogram().DataPoints().At(0).StartTimestamp(),
	)
}

// --- Exemplar tests (inline via opts.Exemplars) ---

func TestAppendExemplarInlineV2(t *testing.T) {
	sink := new(consumertest.MetricsSink)
	tr := newTransactionV2(scrapeCtx, sink, labels.EmptyLabels(), receivertest.NewNopSettings(receivertest.NopType), nopObsRecv(t), false, true)

	ls := labels.FromStrings(
		model.InstanceLabel, "0.0.0.0:8855",
		model.JobLabel, "test",
		model.MetricNameLabel, "counter_test",
	)

	exemplars := []exemplar.Exemplar{
		{
			Value:  42.0,
			Ts:     1663113420863,
			Labels: labels.FromStrings("trace_id", "abc123", "span_id", "def456"),
		},
	}

	_, err := tr.Append(0, ls, 0, ts, 100.0, nil, nil, storage.AOptions{Exemplars: exemplars})
	assert.NoError(t, err)
	assert.NoError(t, tr.Commit())

	mds := sink.AllMetrics()
	require.Len(t, mds, 1)
	require.Equal(t, 1, mds[0].MetricCount())

	metric := mds[0].ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0)
	dp := metric.Sum().DataPoints().At(0)
	require.Equal(t, 1, dp.Exemplars().Len())
}

// --- Unit tests for helper methods ---

func TestDetectAndStoreNativeHistogramStaleness_NonHistogramReturnsFalseV2(t *testing.T) {
	tr := newTxnV2(t, true)
	tr.mc = newFakeMetadataStore(map[string]scrape.MetricMetadata{
		"foo": {MetricFamily: "foo", Type: model.MetricTypeGauge},
	})

	rk := resourceKey{job: "job-a", instance: "localhost:1234"}
	ok := tr.detectAndStoreNativeHistogramStaleness(time.Now().UnixMilli(), &rk, emptyScopeID, "foo", labels.FromMap(map[string]string{
		string(model.MetricNameLabel): "foo",
	}))
	require.False(t, ok, "expected false when metadata type != histogram")
}

func TestGetOrCreateMetricFamily_DistinctFamiliesForNativeVsClassicV2(t *testing.T) {
	tr := newTxnV2(t, true)
	tr.mc = newFakeMetadataStore(map[string]scrape.MetricMetadata{
		"same_family": {MetricFamily: "same_family", Type: model.MetricTypeHistogram},
	})

	rk := resourceKey{job: "job-a", instance: "localhost:1234"}

	tr.addingNativeHistogram = false
	mfClassic := tr.getOrCreateMetricFamily(rk, emptyScopeID, "same_family")

	tr.addingNativeHistogram = true
	mfNative := tr.getOrCreateMetricFamily(rk, emptyScopeID, "same_family")

	require.NotNil(t, mfClassic)
	require.NotNil(t, mfNative)
	require.NotEqual(t, mfClassic, mfNative, "expected distinct metric family instances for native vs classic")
}

func TestAddTargetInfo_DoesNotCopyJobInstanceOrMetricNameV2(t *testing.T) {
	tr := newTxnV2(t, false)
	rk := resourceKey{job: "job-a", instance: "localhost:1234"}
	tr.nodeResources[rk] = CreateResource(rk.job, rk.instance, labels.FromStrings(model.SchemeLabel, "http"))

	ls := labels.FromStrings(
		string(model.MetricNameLabel), "target_info",
		string(model.JobLabel), rk.job,
		string(model.InstanceLabel), rk.instance,
		"extra", "v",
		"another", "x",
	)
	tr.addTargetInfo(rk, ls)

	res := tr.nodeResources[rk]
	attrs := res.Attributes()
	_, hasJob := attrs.Get(string(model.JobLabel))
	_, hasInstance := attrs.Get(string(model.InstanceLabel))
	_, hasName := attrs.Get(string(model.MetricNameLabel))
	_, hasExtra := attrs.Get("extra")
	_, hasAnother := attrs.Get("another")

	require.False(t, hasJob, "job label must not be copied to resource attributes")
	require.False(t, hasInstance, "instance label must not be copied to resource attributes")
	require.False(t, hasName, "metric name label must not be copied to resource attributes")
	require.True(t, hasExtra, "custom label should be copied")
	require.True(t, hasAnother, "custom label should be copied")
}

// --- Builder tests (counter, native histogram) using v2 Append ---

func TestMetricBuilderCountersV2(t *testing.T) {
	tests := []buildTestDataV2{
		{
			name: "single-item",
			inputs: []*testScrapedPage{
				{
					pts: []*testDataPoint{
						createDataPoint("counter_test", 100, nil, "foo", "bar"),
					},
				},
			},
			wants: func() []pmetric.Metrics {
				md0 := pmetric.NewMetrics()
				mL0 := md0.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics()
				m0 := mL0.AppendEmpty()
				m0.SetName("counter_test")
				m0.Metadata().PutStr("prometheus.type", "counter")
				sum := m0.SetEmptySum()
				sum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
				sum.SetIsMonotonic(true)
				pt0 := sum.DataPoints().AppendEmpty()
				pt0.SetDoubleValue(100.0)
				pt0.SetTimestamp(tsNanos)
				pt0.Attributes().PutStr("foo", "bar")

				return []pmetric.Metrics{md0}
			},
		},
		{
			name: "single-item-with-exemplars",
			inputs: []*testScrapedPage{
				{
					pts: []*testDataPoint{
						createDataPoint(
							"counter_test",
							100,
							[]exemplar.Exemplar{
								{
									Value:  1,
									Ts:     1663113420863,
									Labels: labels.New([]labels.Label{{Name: model.MetricNameLabel, Value: "counter_test"}, {Name: model.JobLabel, Value: "job"}, {Name: model.InstanceLabel, Value: "instance"}, {Name: "foo", Value: "bar"}}...),
								},
								{
									Value:  1,
									Ts:     1663113420863,
									Labels: labels.New([]labels.Label{{Name: "foo", Value: "bar"}, {Name: "trace_id", Value: ""}, {Name: "span_id", Value: ""}}...),
								},
								{
									Value:  1,
									Ts:     1663113420863,
									Labels: labels.New([]labels.Label{{Name: "foo", Value: "bar"}, {Name: "trace_id", Value: "10a47365b8aa04e08291fab9deca84db6170"}, {Name: "span_id", Value: "719cee4a669fd7d109ff"}}...),
								},
								{
									Value:  1,
									Ts:     1663113420863,
									Labels: labels.New([]labels.Label{{Name: "foo", Value: "bar"}, {Name: "trace_id", Value: "174137cab66dc880"}, {Name: "span_id", Value: "dfa4597a9d"}}...),
								},
							},
							"foo", "bar"),
					},
				},
			},
			wants: func() []pmetric.Metrics {
				md0 := pmetric.NewMetrics()
				mL0 := md0.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics()
				m0 := mL0.AppendEmpty()
				m0.SetName("counter_test")
				m0.Metadata().PutStr("prometheus.type", "counter")
				sum := m0.SetEmptySum()
				sum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
				sum.SetIsMonotonic(true)
				pt0 := sum.DataPoints().AppendEmpty()
				pt0.SetDoubleValue(100.0)
				pt0.SetTimestamp(tsNanos)
				pt0.Attributes().PutStr("foo", "bar")

				e0 := pt0.Exemplars().AppendEmpty()
				e0.SetTimestamp(timestampFromMs(1663113420863))
				e0.SetDoubleValue(1)
				e0.FilteredAttributes().PutStr(model.MetricNameLabel, "counter_test")
				e0.FilteredAttributes().PutStr("foo", "bar")
				e0.FilteredAttributes().PutStr(model.InstanceLabel, "instance")
				e0.FilteredAttributes().PutStr(model.JobLabel, "job")

				e1 := pt0.Exemplars().AppendEmpty()
				e1.SetTimestamp(timestampFromMs(1663113420863))
				e1.SetDoubleValue(1)
				e1.FilteredAttributes().PutStr("foo", "bar")

				e2 := pt0.Exemplars().AppendEmpty()
				e2.SetTimestamp(timestampFromMs(1663113420863))
				e2.SetDoubleValue(1)
				e2.FilteredAttributes().PutStr("foo", "bar")
				e2.SetTraceID([16]byte{0x10, 0xa4, 0x73, 0x65, 0xb8, 0xaa, 0x04, 0xe0, 0x82, 0x91, 0xfa, 0xb9, 0xde, 0xca, 0x84, 0xdb})
				e2.SetSpanID([8]byte{0x71, 0x9c, 0xee, 0x4a, 0x66, 0x9f, 0xd7, 0xd1})

				e3 := pt0.Exemplars().AppendEmpty()
				e3.SetTimestamp(timestampFromMs(1663113420863))
				e3.SetDoubleValue(1)
				e3.FilteredAttributes().PutStr("foo", "bar")
				e3.SetTraceID([16]byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x17, 0x41, 0x37, 0xca, 0xb6, 0x6d, 0xc8, 0x80})
				e3.SetSpanID([8]byte{0x0, 0x0, 0x0, 0xdf, 0xa4, 0x59, 0x7a, 0x9d})

				return []pmetric.Metrics{md0}
			},
		},
	}

	for _, tt := range tests {
		tt.run(t)
	}
}

func TestMetricBuilderNativeHistogramV2(t *testing.T) {
	h0 := tsdbutil.GenerateTestHistogram(0)

	tests := []buildTestDataV2{
		{
			name: "integer histogram",
			inputs: []*testScrapedPage{
				{
					pts: []*testDataPoint{
						createHistogramDataPoint("hist_test", h0, nil, nil, "foo", "bar"),
					},
				},
			},
			wants: func() []pmetric.Metrics {
				md0 := pmetric.NewMetrics()
				mL0 := md0.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics()
				m0 := mL0.AppendEmpty()
				m0.SetName("hist_test")
				m0.Metadata().PutStr("prometheus.type", "histogram")
				m0.SetEmptyExponentialHistogram()
				m0.ExponentialHistogram().SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
				pt0 := m0.ExponentialHistogram().DataPoints().AppendEmpty()
				pt0.Attributes().PutStr("foo", "bar")
				pt0.SetTimestamp(tsNanos)
				pt0.SetCount(12)
				pt0.SetSum(18.4)
				pt0.SetScale(1)
				pt0.SetZeroThreshold(0.001)
				pt0.SetZeroCount(2)
				pt0.Positive().SetOffset(-1)
				pt0.Positive().BucketCounts().Append(1)
				pt0.Positive().BucketCounts().Append(2)
				pt0.Positive().BucketCounts().Append(0)
				pt0.Positive().BucketCounts().Append(1)
				pt0.Positive().BucketCounts().Append(1)
				pt0.Negative().SetOffset(-1)
				pt0.Negative().BucketCounts().Append(1)
				pt0.Negative().BucketCounts().Append(2)
				pt0.Negative().BucketCounts().Append(0)
				pt0.Negative().BucketCounts().Append(1)
				pt0.Negative().BucketCounts().Append(1)

				return []pmetric.Metrics{md0}
			},
		},
	}

	for _, tt := range tests {
		tt.run(t)
	}
}

// --- V2 builder test infrastructure ---
// Reuses v1's testScrapedPage and testDataPoint types.

type buildTestDataV2 struct {
	name   string
	inputs []*testScrapedPage
	wants  func() []pmetric.Metrics
}

func (tt buildTestDataV2) run(t *testing.T) {
	t.Run(tt.name, func(t *testing.T) {
		wants := tt.wants()
		assert.Len(t, tt.inputs, len(wants))
		st := ts
		for i, page := range tt.inputs {
			sink := new(consumertest.MetricsSink)
			tr := newTransactionV2(scrapeCtx, sink, labels.EmptyLabels(), receivertest.NewNopSettings(receivertest.NopType), nopObsRecv(t), false, true)
			for _, pt := range page.pts {
				pt.t = st
				var err error
				switch {
				case pt.fh != nil:
					_, err = tr.Append(0, pt.lb, 0, pt.t, 0, nil, pt.fh, storage.AOptions{Exemplars: pt.exemplars})
				case pt.h != nil:
					_, err = tr.Append(0, pt.lb, 0, pt.t, 0, pt.h, nil, storage.AOptions{Exemplars: pt.exemplars})
				default:
					_, err = tr.Append(0, pt.lb, 0, pt.t, pt.v, nil, nil, storage.AOptions{Exemplars: pt.exemplars})
				}
				assert.NoError(t, err)
			}
			assert.NoError(t, tr.Commit())
			mds := sink.AllMetrics()
			if wants[i].ResourceMetrics().Len() == 0 {
				require.Empty(t, mds)
				st += interval
				continue
			}
			require.Len(t, mds, 1)
			assertEquivalentMetrics(t, wants[i], mds[0])
			st += interval
		}
	})
}
