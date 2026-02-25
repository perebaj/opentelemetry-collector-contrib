// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package internal // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/prometheusreceiver/internal"

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/translator/prometheus"
	mdata "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/prometheusreceiver/internal/metadata"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/scrape"
	"github.com/prometheus/prometheus/storage"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/receiver/receiverhelper"
	"go.uber.org/zap"
)

// transactionV2 is a wrapper around the transaction struct that implements the storage.AppenderV2 interface.
type transactionV2 struct {
	isNew                 bool
	trimSuffixes          bool
	useMetadata           bool
	addingNativeHistogram bool
	addingNHCB            bool
	ctx                   context.Context
	families              map[resourceKey]map[scopeID]map[metricFamilyKey]*metricFamily
	mc                    scrape.MetricMetadataStore
	sink                  consumer.Metrics
	externalLabels        labels.Labels
	nodeResources         map[resourceKey]pcommon.Resource
	scopeAttributes       map[resourceKey]map[scopeID]pcommon.Map
	logger                *zap.Logger
	buildInfo             component.BuildInfo
	obsrecv               *receiverhelper.ObsReport
	bufBytes              []byte
}

func newTransactionV2(
	ctx context.Context,
	sink consumer.Metrics,
	externalLabels labels.Labels,
	settings receiver.Settings,
	obsrecv *receiverhelper.ObsReport,
	trimSuffixes bool,
	useMetadata bool,
) *transactionV2 {
	return &transactionV2{
		ctx:             ctx,
		families:        make(map[resourceKey]map[scopeID]map[metricFamilyKey]*metricFamily),
		isNew:           true,
		trimSuffixes:    trimSuffixes,
		useMetadata:     useMetadata,
		sink:            sink,
		externalLabels:  externalLabels,
		logger:          settings.Logger,
		buildInfo:       settings.BuildInfo,
		obsrecv:         obsrecv,
		bufBytes:        make([]byte, 0, 1024),
		scopeAttributes: make(map[resourceKey]map[scopeID]pcommon.Map),
		nodeResources:   map[resourceKey]pcommon.Resource{},
	}
}

// Append implements storage.AppenderV2. Different than the v1 Appender, this method handles float samples, histograms, exemplars, metadata, and start timestamps in a single unified call.
func (t *transactionV2) Append(
	_ storage.SeriesRef,
	ls labels.Labels,
	// on AppenderV1, stMs is the start timestamp in milliseconds, atMs is the scrape timestamp in milliseconds. Now, those values came directly from the Prometheus scrape.
	stMs, atMs int64,
	val float64,
	h *histogram.Histogram,
	fh *histogram.FloatHistogram,
	opts storage.AOptions,
) (storage.SeriesRef, error) {
	select {
	case <-t.ctx.Done():
		return 0, errTransactionAborted
	default:
	}

	// indentify if the sample is a histogram based on the histogram.Histogram and histogram.FloatHistogram pointers.
	isHistogram := h != nil || fh != nil

	// assign the schema based on the histogram.Histogram and histogram.FloatHistogram pointers.
	var schema int32
	if h != nil {
		schema = h.Schema
	} else if fh != nil {
		schema = fh.Schema
	}

	t.addingNativeHistogram = isHistogram
	// if the sample is a histogram and the schema is -53, then the sample is a NHCB.
	// Reference: https://prometheus.io/docs/specs/native_histograms/#schema
	t.addingNHCB = isHistogram && schema == -53

	if t.externalLabels.Len() != 0 {
		b := labels.NewBuilder(ls)
		t.externalLabels.Range(func(l labels.Label) {
			b.Set(l.Name, l.Value)
		})
		ls = b.Labels()
	}

	rKey, err := t.initTransaction(ls)
	if err != nil {
		return 0, err
	}

	if dupLabel, hasDup := ls.HasDuplicateLabelNames(); hasDup {
		return 0, fmt.Errorf("invalid sample: non-unique label names: %q", dupLabel)
	}

	metricName := ls.Get(model.MetricNameLabel)
	if metricName == "" {
		return 0, errMetricNameNotFound
	}

	if !isHistogram {
		if metricName == scrapeUpMetricName && val != 1.0 && !value.IsStaleNaN(val) {
			if val == 0.0 {
				t.logger.Warn("Failed to scrape Prometheus endpoint",
					zap.Int64("scrape_timestamp", atMs),
					zap.Stringer("target_labels", ls))
			} else {
				t.logger.Warn("The 'up' metric contains invalid value",
					zap.Float64("value", val),
					zap.Int64("scrape_timestamp", atMs),
					zap.Stringer("target_labels", ls))
			}
		}

		if metricName == prometheus.TargetInfoMetricName {
			t.addTargetInfo(*rKey, ls)
			return 0, nil
		}

		if metricName == prometheus.ScopeInfoMetricName {
			t.addScopeInfo(*rKey, ls)
			return 0, nil
		}

		scope := getScopeID(ls)

		if value.IsStaleNaN(val) {
			if t.detectAndStoreNativeHistogramStaleness(atMs, rKey, scope, metricName, ls) {
				return 0, nil
			}
		}

		curMF := t.getOrCreateMetricFamily(*rKey, scope, metricName)
		seriesRef := t.getSeriesRef(ls, curMF.mtype)

		if stMs > 0 {
			curMF.addCreationTimestamp(seriesRef, ls, atMs, stMs)
		}

		if err := curMF.addSeries(seriesRef, metricName, ls, atMs, val); err != nil {
			t.logger.Warn("failed to add datapoint", zap.Error(err), zap.String("metric_name", metricName), zap.Any("labels", ls))
			return 0, nil
		}

		for _, e := range opts.Exemplars {
			curMF.addExemplar(seriesRef, e)
		}

		return storage.SeriesRef(ls.Hash()), nil
	}

	// Histogram path.
	scope := getScopeID(ls)
	curMF := t.getOrCreateMetricFamily(*rKey, scope, metricName)

	if h != nil && h.CounterResetHint == histogram.GaugeType || fh != nil && fh.CounterResetHint == histogram.GaugeType {
		t.logger.Warn("dropping unsupported gauge histogram datapoint", zap.String("metric_name", metricName), zap.Any("labels", ls))
	}

	seriesRef := t.getSeriesRef(ls, curMF.mtype)

	if stMs > 0 {
		curMF.addCreationTimestamp(seriesRef, ls, atMs, stMs)
	}

	if schema == -53 {
		err = curMF.addNHCBSeries(seriesRef, metricName, ls, atMs, h, fh)
	} else {
		err = curMF.addExponentialHistogramSeries(seriesRef, metricName, ls, atMs, h, fh)
	}
	if err != nil {
		t.logger.Warn("failed to add histogram datapoint", zap.Error(err), zap.String("metric_name", metricName), zap.Any("labels", ls))
		return 0, nil
	}

	for _, e := range opts.Exemplars {
		curMF.addExemplar(seriesRef, e)
	}

	return 1, nil
}

func (t *transactionV2) Commit() error {
	if t.isNew {
		return nil
	}

	ctx := t.obsrecv.StartMetricsOp(t.ctx)
	md, err := t.getMetrics()
	if err != nil {
		t.obsrecv.EndMetricsOp(ctx, dataformat, 0, err)
		return err
	}

	numPoints := md.DataPointCount()
	if numPoints == 0 {
		return nil
	}

	err = t.sink.ConsumeMetrics(ctx, md)
	t.obsrecv.EndMetricsOp(ctx, dataformat, numPoints, err)
	return err
}

func (*transactionV2) Rollback() error {
	return nil
}

// Helper methods below mirror those on transaction (v1).

func (t *transactionV2) detectAndStoreNativeHistogramStaleness(atMs int64, key *resourceKey, scope scopeID, metricName string, ls labels.Labels) bool {
	md, ok := t.mc.GetMetadata(metricName)
	if !ok {
		return false
	}
	if md.Type != model.MetricTypeHistogram {
		return false
	}
	if md.MetricFamily != metricName {
		return false
	}
	t.addingNativeHistogram = true
	t.addingNHCB = false

	curMF := t.getOrCreateMetricFamily(*key, scope, metricName)
	seriesRef := t.getSeriesRef(ls, curMF.mtype)

	_ = curMF.addExponentialHistogramSeries(seriesRef, metricName, ls, atMs, &histogram.Histogram{Sum: math.Float64frombits(value.StaleNaN)}, nil)
	return true
}

func (t *transactionV2) getOrCreateMetricFamily(key resourceKey, scope scopeID, mn string) *metricFamily {
	if _, ok := t.families[key]; !ok {
		t.families[key] = make(map[scopeID]map[metricFamilyKey]*metricFamily)
	}
	if _, ok := t.families[key][scope]; !ok {
		t.families[key][scope] = make(map[metricFamilyKey]*metricFamily)
	}

	mfKey := metricFamilyKey{isExponentialHistogram: t.addingNativeHistogram, name: mn}
	curMf, ok := t.families[key][scope][mfKey]

	if !ok {
		fn := mn
		if _, ok := t.mc.GetMetadata(mn); !ok {
			fn = normalizeMetricName(mn)
			if isCounterCreatedLine(mn, fn, t.mc) {
				fn += metricSuffixTotal
			}
		}
		fnKey := metricFamilyKey{isExponentialHistogram: mfKey.isExponentialHistogram, name: fn}
		mf, ok := t.families[key][scope][fnKey]
		if !ok || !mf.includesMetric(mn) {
			curMf = newMetricFamily(mn, t.mc, t.logger, t.addingNativeHistogram, t.addingNHCB)
			t.families[key][scope][metricFamilyKey{isExponentialHistogram: mfKey.isExponentialHistogram, name: curMf.name}] = curMf
			return curMf
		}
		curMf = mf
	}
	return curMf
}

func (t *transactionV2) getSeriesRef(ls labels.Labels, mtype pmetric.MetricType) uint64 {
	var hash uint64
	hash, t.bufBytes = getSeriesRef(t.bufBytes, ls, mtype)
	return hash
}

func (t *transactionV2) getMetrics() (pmetric.Metrics, error) {
	if len(t.families) == 0 {
		return pmetric.Metrics{}, errNoDataToBuild
	}

	md := pmetric.NewMetrics()

	for rKey, families := range t.families {
		if len(families) == 0 {
			continue
		}
		resource, ok := t.nodeResources[rKey]
		if !ok {
			continue
		}
		rms := md.ResourceMetrics().AppendEmpty()
		resource.CopyTo(rms.Resource())

		for scope, mfs := range families {
			ils := rms.ScopeMetrics().AppendEmpty()
			if scope == emptyScopeID {
				ils.Scope().SetName(mdata.ScopeName)
				ils.Scope().SetVersion(t.buildInfo.Version)
			} else {
				ils.Scope().SetName(scope.name)
				ils.Scope().SetVersion(scope.version)
				if scope.schemaURL != "" {
					ils.SetSchemaUrl(scope.schemaURL)
				}
				if scopeAttributes, ok := t.scopeAttributes[rKey]; ok {
					if attributes, ok := scopeAttributes[scope]; ok {
						attributes.CopyTo(ils.Scope().Attributes())
					}
				}
			}
			metrics := ils.Metrics()
			for _, mf := range mfs {
				mf.appendMetric(metrics, t.trimSuffixes)
			}
		}
	}
	md.ResourceMetrics().RemoveIf(func(metrics pmetric.ResourceMetrics) bool {
		if metrics.ScopeMetrics().Len() == 0 {
			return true
		}
		remove := true
		for i := 0; i < metrics.ScopeMetrics().Len(); i++ {
			if metrics.ScopeMetrics().At(i).Metrics().Len() > 0 {
				remove = false
				break
			}
		}
		return remove
	})

	return md, nil
}

func (t *transactionV2) initTransaction(lbs labels.Labels) (*resourceKey, error) {
	target, ok := scrape.TargetFromContext(t.ctx)
	if !ok {
		return nil, errors.New("unable to find target in context")
	}
	if t.useMetadata {
		t.mc, ok = scrape.MetricMetadataStoreFromContext(t.ctx)
		if !ok {
			return nil, errors.New("unable to find MetricMetadataStore in context")
		}
	} else {
		t.mc = &emptyMetadataStore{}
	}

	rKey, err := t.getJobAndInstance(lbs)
	if err != nil {
		return nil, err
	}
	if _, ok := t.nodeResources[*rKey]; !ok {
		t.nodeResources[*rKey] = CreateResource(rKey.job, rKey.instance, target.DiscoveredLabels(labels.NewBuilder(labels.EmptyLabels())))
	}

	t.isNew = false
	return rKey, nil
}

func (t *transactionV2) getJobAndInstance(ls labels.Labels) (*resourceKey, error) {
	job, instance := ls.Get(model.JobLabel), ls.Get(model.InstanceLabel)
	if job != "" && instance != "" {
		return &resourceKey{job: job, instance: instance}, nil
	}

	if target, ok := scrape.TargetFromContext(t.ctx); ok {
		if job == "" {
			job = target.GetValue(model.JobLabel)
		}
		if instance == "" {
			instance = target.GetValue(model.InstanceLabel)
		}
		if job != "" && instance != "" {
			return &resourceKey{job: job, instance: instance}, nil
		}
	}
	return nil, errNoJobInstance
}

func (t *transactionV2) addTargetInfo(key resourceKey, ls labels.Labels) {
	t.addingNativeHistogram = false
	t.addingNHCB = false
	if resource, ok := t.nodeResources[key]; ok {
		attrs := resource.Attributes()
		ls.Range(func(lbl labels.Label) {
			if lbl.Name == model.JobLabel || lbl.Name == model.InstanceLabel || lbl.Name == model.MetricNameLabel {
				return
			}
			attrs.PutStr(lbl.Name, lbl.Value)
		})
	}
}

func (t *transactionV2) addScopeInfo(key resourceKey, ls labels.Labels) {
	t.addingNativeHistogram = false
	t.addingNHCB = false
	attrs := pcommon.NewMap()
	scope := scopeID{}
	ls.Range(func(lbl labels.Label) {
		if lbl.Name == model.JobLabel || lbl.Name == model.InstanceLabel || lbl.Name == model.MetricNameLabel {
			return
		}
		if lbl.Name == prometheus.ScopeNameLabelKey {
			scope.name = lbl.Value
			return
		}
		if lbl.Name == prometheus.ScopeVersionLabelKey {
			scope.version = lbl.Value
			return
		}
		if lbl.Name == prometheus.ScopeSchemaURLLabelKey {
			scope.schemaURL = lbl.Value
			return
		}
		attrs.PutStr(lbl.Name, lbl.Value)
	})
	if _, ok := t.scopeAttributes[key]; !ok {
		t.scopeAttributes[key] = make(map[scopeID]pcommon.Map)
	}
	t.scopeAttributes[key][scope] = attrs
}
