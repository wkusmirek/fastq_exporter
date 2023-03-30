package main

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/krallistic/kazoo-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	plog "github.com/prometheus/common/promlog"
	plogflag "github.com/prometheus/common/promlog/flag"
	"github.com/prometheus/common/version"
	"github.com/rcrowley/go-metrics"
	"gopkg.in/alecthomas/kingpin.v2"
	"k8s.io/klog/v2"
)

const (
	namespace = "minknow"
	clientID  = "minknow_exporter"
)

const (
	INFO  = 0
	DEBUG = 1
	TRACE = 2
)

var (
	topicOldestOffset                *prometheus.Desc
	topicPartitionLeader             *prometheus.Desc
	readCountMetric                  *prometheus.Desc
	fractionBasecalledMetric         *prometheus.Desc
	fractionSkippedMetric            *prometheus.Desc
	basecalledPassReadCountMetric    *prometheus.Desc
	basecalledFailReadCountMetric    *prometheus.Desc
	basecalledSkippedReadCountMetric *prometheus.Desc
	basecalledPassBasesMetric        *prometheus.Desc
	basecalledFailBasesMetric        *prometheus.Desc
	basecalledSamplesMetric          *prometheus.Desc
	selectedRawSamplesMetric         *prometheus.Desc
	selectedEventsMetric             *prometheus.Desc
	estimatedSelectedBasesMetric     *prometheus.Desc
	alignmentMatchesMetric           *prometheus.Desc
	alignmentMismatchesMetric        *prometheus.Desc
	alignmentInsertionsMetric        *prometheus.Desc
	alignmentDeletionsMetric         *prometheus.Desc
	alignmentCoverageMetric          *prometheus.Desc
	biasVoltageMetric                *prometheus.Desc
	targetTempMetric                 *prometheus.Desc
	asicTempMetric                   *prometheus.Desc
	heatSinkTempMetric               *prometheus.Desc
	pendingMuxChangeMetric           *prometheus.Desc
	inrangeMetric                    *prometheus.Desc
	aboveMetric                      *prometheus.Desc
	strandMetric                     *prometheus.Desc
	unavailableMetric                *prometheus.Desc
	belowMetric                      *prometheus.Desc
	multipleMetric                   *prometheus.Desc
	saturatedMetric                  *prometheus.Desc
	goodSingleMetric                 *prometheus.Desc
	unknownMetric                    *prometheus.Desc
	unclassifiedMetric               *prometheus.Desc
	unblockingMetric                 *prometheus.Desc
	adapterMetric                    *prometheus.Desc
)

type Exporter struct {
	client                  sarama.Client
	topicFilter             *regexp.Regexp
	groupFilter             *regexp.Regexp
	mu                      sync.Mutex
	useZooKeeperLag         bool
	zookeeperClient         *kazoo.Kazoo
	nextMetadataRefresh     time.Time
	metadataRefreshInterval time.Duration
	offsetShowAll           bool
	topicWorkers            int
	allowConcurrent         bool
	sgMutex                 sync.Mutex
	sgWaitCh                chan struct{}
	sgChans                 []chan<- prometheus.Metric
	consumerGroupFetchAll   bool
}

type exporterOpts struct {
	uri                      []string
	useSASL                  bool
	useSASLHandshake         bool
	saslUsername             string
	saslPassword             string
	saslMechanism            string
	saslDisablePAFXFast      bool
	useTLS                   bool
	tlsServerName            string
	tlsCAFile                string
	tlsCertFile              string
	tlsKeyFile               string
	serverUseTLS             bool
	serverMutualAuthEnabled  bool
	serverTlsCAFile          string
	serverTlsCertFile        string
	serverTlsKeyFile         string
	tlsInsecureSkipTLSVerify bool
	minknowVersion           string
	useZooKeeperLag          bool
	uriZookeeper             []string
	labels                   string
	metadataRefreshInterval  string
	serviceName              string
	kerberosConfigPath       string
	realm                    string
	keyTabPath               string
	kerberosAuthType         string
	offsetShowAll            bool
	topicWorkers             int
	allowConcurrent          bool
	allowAutoTopicCreation   bool
	verbosityLogLevel        int
}

// CanReadCertAndKey returns true if the certificate and key files already exists,
// otherwise returns false. If lost one of cert and key, returns error.
func CanReadCertAndKey(certPath, keyPath string) (bool, error) {
	certReadable := canReadFile(certPath)
	keyReadable := canReadFile(keyPath)

	if certReadable == false && keyReadable == false {
		return false, nil
	}

	if certReadable == false {
		return false, fmt.Errorf("error reading %s, certificate and key must be supplied as a pair", certPath)
	}

	if keyReadable == false {
		return false, fmt.Errorf("error reading %s, certificate and key must be supplied as a pair", keyPath)
	}

	return true, nil
}

// If the file represented by path exists and
// readable, returns true otherwise returns false.
func canReadFile(path string) bool {
	f, err := os.Open(path)
	if err != nil {
		return false
	}

	defer f.Close()

	return true
}

// NewExporter returns an initialized Exporter.
func NewExporter(opts exporterOpts, topicFilter string, groupFilter string) (*Exporter, error) {
	var zookeeperClient *kazoo.Kazoo
	config := sarama.NewConfig()
	config.ClientID = clientID
	minknowVersion, err := sarama.ParseKafkaVersion(opts.minknowVersion)
	if err != nil {
		return nil, err
	}
	config.Version = minknowVersion

	if opts.useSASL {
		// Convert to lowercase so that SHA512 and SHA256 is still valid
		opts.saslMechanism = strings.ToLower(opts.saslMechanism)
		switch opts.saslMechanism {
		case "scram-sha512":
			config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: SHA512} }
			config.Net.SASL.Mechanism = sarama.SASLMechanism(sarama.SASLTypeSCRAMSHA512)
		case "scram-sha256":
			config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: SHA256} }
			config.Net.SASL.Mechanism = sarama.SASLMechanism(sarama.SASLTypeSCRAMSHA256)
		case "gssapi":
			config.Net.SASL.Mechanism = sarama.SASLMechanism(sarama.SASLTypeGSSAPI)
			config.Net.SASL.GSSAPI.ServiceName = opts.serviceName
			config.Net.SASL.GSSAPI.KerberosConfigPath = opts.kerberosConfigPath
			config.Net.SASL.GSSAPI.Realm = opts.realm
			config.Net.SASL.GSSAPI.Username = opts.saslUsername
			if opts.kerberosAuthType == "keytabAuth" {
				config.Net.SASL.GSSAPI.AuthType = sarama.KRB5_KEYTAB_AUTH
				config.Net.SASL.GSSAPI.KeyTabPath = opts.keyTabPath
			} else {
				config.Net.SASL.GSSAPI.AuthType = sarama.KRB5_USER_AUTH
				config.Net.SASL.GSSAPI.Password = opts.saslPassword
			}
			if opts.saslDisablePAFXFast {
				config.Net.SASL.GSSAPI.DisablePAFXFAST = true
			}
		case "plain":
		default:
			return nil, fmt.Errorf(
				`invalid sasl mechanism "%s": can only be "scram-sha256", "scram-sha512", "gssapi" or "plain"`,
				opts.saslMechanism,
			)
		}

		config.Net.SASL.Enable = true
		config.Net.SASL.Handshake = opts.useSASLHandshake

		if opts.saslUsername != "" {
			config.Net.SASL.User = opts.saslUsername
		}

		if opts.saslPassword != "" {
			config.Net.SASL.Password = opts.saslPassword
		}
	}

	if opts.useTLS {
		config.Net.TLS.Enable = true

		config.Net.TLS.Config = &tls.Config{
			ServerName:         opts.tlsServerName,
			RootCAs:            x509.NewCertPool(),
			InsecureSkipVerify: opts.tlsInsecureSkipTLSVerify,
		}

		if opts.tlsCAFile != "" {
			if ca, err := ioutil.ReadFile(opts.tlsCAFile); err == nil {
				config.Net.TLS.Config.RootCAs.AppendCertsFromPEM(ca)
			} else {
				return nil, err
			}
		}

		canReadCertAndKey, err := CanReadCertAndKey(opts.tlsCertFile, opts.tlsKeyFile)
		if err != nil {
			return nil, errors.Wrap(err, "error reading cert and key")
		}
		if canReadCertAndKey {
			cert, err := tls.LoadX509KeyPair(opts.tlsCertFile, opts.tlsKeyFile)
			if err == nil {
				config.Net.TLS.Config.Certificates = []tls.Certificate{cert}
			} else {
				return nil, err
			}
		}
	}

	if opts.useZooKeeperLag {
		klog.V(DEBUG).Infoln("Using zookeeper lag, so connecting to zookeeper")
		zookeeperClient, err = kazoo.NewKazoo(opts.uriZookeeper, nil)
		if err != nil {
			return nil, errors.Wrap(err, "error connecting to zookeeper")
		}
	}

	interval, err := time.ParseDuration(opts.metadataRefreshInterval)
	if err != nil {
		return nil, errors.Wrap(err, "Cannot parse metadata refresh interval")
	}

	config.Metadata.RefreshFrequency = interval

	config.Metadata.AllowAutoTopicCreation = opts.allowAutoTopicCreation

	client, err := sarama.NewClient(opts.uri, config)

	if err != nil {
		return nil, errors.Wrap(err, "Error Init Minknow Client")
	}

	klog.V(TRACE).Infoln("Done Init Clients")
	// Init our exporter.
	return &Exporter{
		client:                  client,
		topicFilter:             regexp.MustCompile(topicFilter),
		groupFilter:             regexp.MustCompile(groupFilter),
		useZooKeeperLag:         opts.useZooKeeperLag,
		zookeeperClient:         zookeeperClient,
		nextMetadataRefresh:     time.Now(),
		metadataRefreshInterval: interval,
		offsetShowAll:           opts.offsetShowAll,
		topicWorkers:            opts.topicWorkers,
		allowConcurrent:         opts.allowConcurrent,
		sgMutex:                 sync.Mutex{},
		sgWaitCh:                nil,
		sgChans:                 []chan<- prometheus.Metric{},
		consumerGroupFetchAll:   config.Version.IsAtLeast(sarama.V2_0_0_0),
	}, nil
}

//func (e *Exporter) fetchOffsetVersion() int16 {
//	version := e.client.Config().Version
//	if e.client.Config().Version.IsAtLeast(sarama.V2_0_0_0) {
//		return 4
//	} else if version.IsAtLeast(sarama.V0_10_2_0) {
//		return 2
//	} else if version.IsAtLeast(sarama.V0_8_2_2) {
//		return 1
//	}
//	return 0
//}

// Describe describes all the metrics ever exported by the Minknow exporter. It
// implements prometheus.Collector.
func (e *Exporter) Describe(ch chan<- *prometheus.Desc) {
	ch <- topicOldestOffset
	ch <- topicPartitionLeader
	ch <- readCountMetric
	ch <- fractionBasecalledMetric
	ch <- fractionSkippedMetric
	ch <- basecalledPassReadCountMetric
	ch <- basecalledFailReadCountMetric
	ch <- basecalledSkippedReadCountMetric
	ch <- basecalledPassBasesMetric
	ch <- basecalledFailBasesMetric
	ch <- basecalledSamplesMetric
	ch <- selectedRawSamplesMetric
	ch <- selectedEventsMetric
	ch <- estimatedSelectedBasesMetric
	ch <- alignmentMatchesMetric
	ch <- alignmentMismatchesMetric
	ch <- alignmentInsertionsMetric
	ch <- alignmentDeletionsMetric
	ch <- alignmentCoverageMetric
	ch <- biasVoltageMetric
	ch <- targetTempMetric
	ch <- asicTempMetric
	ch <- heatSinkTempMetric
	ch <- pendingMuxChangeMetric
	ch <- inrangeMetric
	ch <- aboveMetric
	ch <- strandMetric
	ch <- unavailableMetric
	ch <- belowMetric
	ch <- multipleMetric
	ch <- saturatedMetric
	ch <- goodSingleMetric
	ch <- unknownMetric
	ch <- unclassifiedMetric
	ch <- unblockingMetric
	ch <- adapterMetric
}

func (e *Exporter) Collect(ch chan<- prometheus.Metric) {
	//	   	if e.allowConcurrent {
	e.collect(ch)
	return
	//	   	}
	// Locking to avoid race add
	/*e.sgMutex.Lock()
	  e.sgChans = append(e.sgChans, ch)
	  // Safe to compare length since we own the Lock

	  	if len(e.sgChans) == 1 {
	  		e.sgWaitCh = make(chan struct{})
	  		go e.collectChans(e.sgWaitCh)
	  	} else {

	  		klog.V(TRACE).Info("concurrent calls detected, waiting for first to finish")
	  	}

	  // Put in another variable to ensure not overwriting it in another Collect once we wait
	  waiter := e.sgWaitCh
	  e.sgMutex.Unlock()
	  // Released lock, we have insurance that our chan will be part of the collectChan slice
	  <-waiter*/
	// collectChan finished

}

func (e *Exporter) collectChans(quit chan struct{}) {
	original := make(chan prometheus.Metric)
	container := make([]prometheus.Metric, 0, 100)
	go func() {
		for metric := range original {
			container = append(container, metric)
		}
	}()
	e.collect(original)
	close(original)
	e.sgMutex.Lock()
	for _, ch := range e.sgChans {
		for _, metric := range container {
			ch <- metric
		}
	}
	// Reset the slice
	e.sgChans = e.sgChans[:0]
	close(quit)
	e.sgMutex.Unlock()
}

type sequencingPositionsData struct {
	Address string `json:'address'`
	Name    string `json:'name'`
	State   string `json:'state'`
}

type currentAcquisitionRunData struct {
	Address                       string `json:'address'`
	Name                          string `json:'name'`
	Seconds                       string `json:'seconds'`
	Read_count                    string `json:'read_count'`
	Fraction_basecalled           string `json:'fraction_basecalled'`
	Fraction_skipped              string `json:'fraction_skipped'`
	Basecalled_pass_read_count    string `json:'basecalled_pass_read_count'`
	Basecalled_fail_read_count    string `json:'basecalled_fail_read_count'`
	Basecalled_skipped_read_count string `json:'basecalled_skipped_read_count'`
	Basecalled_pass_bases         string `json:'basecalled_pass_bases'`
	Basecalled_fail_bases         string `json:'basecalled_fail_bases'`
	Basecalled_samples            string `json:'basecalled_samples'`
	Selected_raw_samples          string `json:'selected_raw_samples'`
	Selected_events               string `json:'selected_events'`
	Estimated_selected_bases      string `json:'estimated_selected_bases'`
	Alignment_matches             string `json:'alignment_matches'`
	Alignment_mismatches          string `json:'alignment_mismatches'`
	Alignment_insertions          string `json:'alignment_insertions'`
	Alignment_deletions           string `json:'alignment_deletions'`
	Alignment_coverage            string `json:'alignment_coverage'`
}

type voltageData struct {
	Address      string `json:'address'`
	Name         string `json:'name'`
	Bias_voltage string `json:'bias_voltage'`
}

type temperatureData struct {
	Address        string `json:'address'`
	Name           string `json:'name'`
	Target_temp    string `json:'target_temp'`
	Asic_temp      string `json:'asic_temp'`
	Heat_sink_temp string `json:'heat_sink_temp'`
}

type channelStatesData struct {
	Address            string `json:'address'`
	Name               string `json:'name'`
	Pending_mux_change string `json:'pending_mux_change'`
	Inrange            string `json:'inrange'`
	Above              string `json:'above'`
	Strand             string `json:'strand'`
	Unavailable        string `json:'unavailable'`
	Below              string `json:'below'`
	Multiple           string `json:'multiple'`
	Saturated          string `json:'saturated'`
	Good_single        string `json:'good_single'`
	Unknown            string `json:'unknown'`
	Unclassified       string `json:'unclassified'`
	Unblocking         string `json:'unblocking'`
	Adapter            string `json:'adapter'`
}

func (e *Exporter) collect(ch chan<- prometheus.Metric) {
	var sequencingPositions []*sequencingPositionsData
	result, err := exec.Command("python3", "python/list_sequencing_positions.py").Output()
	if err != nil {
		log.Fatal(err)
	}
	err = json.Unmarshal([]byte(result), &sequencingPositions)
	if err != nil {
		log.Fatal(err)
	}
	for _, flowCell := range sequencingPositions {
		ch <- prometheus.MustNewConstMetric(
			topicOldestOffset, prometheus.GaugeValue, float64(1), flowCell.Address, flowCell.Name, flowCell.State,
		)
	}

	var currentAcquisitionRun []*currentAcquisitionRunData
	result2, err2 := exec.Command("python3", "python/get_current_acquisition_run.py").Output()
	if err2 != nil {
		log.Fatal(err2)
	}
	err2 = json.Unmarshal([]byte(result2), &currentAcquisitionRun)
	if err2 != nil {
		log.Fatal(err2)
	}
	for _, statistic := range currentAcquisitionRun {
		Read_count, _ := strconv.Atoi(statistic.Read_count)
		Fraction_basecalled, _ := strconv.Atoi(statistic.Fraction_basecalled)
		Fraction_skipped, _ := strconv.Atoi(statistic.Fraction_skipped)
		Basecalled_pass_read_count, _ := strconv.Atoi(statistic.Basecalled_pass_read_count)
		Basecalled_fail_read_count, _ := strconv.Atoi(statistic.Basecalled_fail_read_count)
		Basecalled_skipped_read_count, _ := strconv.Atoi(statistic.Basecalled_skipped_read_count)
		Basecalled_pass_bases, _ := strconv.Atoi(statistic.Basecalled_pass_bases)
		Basecalled_fail_bases, _ := strconv.Atoi(statistic.Basecalled_fail_bases)
		Basecalled_samples, _ := strconv.Atoi(statistic.Basecalled_samples)
		Selected_raw_samples, _ := strconv.Atoi(statistic.Selected_raw_samples)
		Selected_events, _ := strconv.Atoi(statistic.Selected_events)
		Estimated_selected_bases, _ := strconv.Atoi(statistic.Estimated_selected_bases)
		Alignment_matches, _ := strconv.Atoi(statistic.Alignment_matches)
		Alignment_mismatches, _ := strconv.Atoi(statistic.Alignment_mismatches)
		Alignment_insertions, _ := strconv.Atoi(statistic.Alignment_insertions)
		Alignment_deletions, _ := strconv.Atoi(statistic.Alignment_deletions)
		Alignment_coverage, _ := strconv.Atoi(statistic.Alignment_coverage)
		ch <- prometheus.MustNewConstMetric(
			readCountMetric, prometheus.GaugeValue, float64(Read_count),
			statistic.Address,
			statistic.Name,
			statistic.Seconds,
		)
		ch <- prometheus.MustNewConstMetric(
			fractionBasecalledMetric, prometheus.GaugeValue, float64(Fraction_basecalled),
			statistic.Address,
			statistic.Name,
			statistic.Seconds,
		)
		ch <- prometheus.MustNewConstMetric(
			fractionSkippedMetric, prometheus.GaugeValue, float64(Fraction_skipped),
			statistic.Address,
			statistic.Name,
			statistic.Seconds,
		)
		ch <- prometheus.MustNewConstMetric(
			basecalledPassReadCountMetric, prometheus.GaugeValue, float64(Basecalled_pass_read_count),
			statistic.Address,
			statistic.Name,
			statistic.Seconds,
		)
		ch <- prometheus.MustNewConstMetric(
			basecalledFailReadCountMetric, prometheus.GaugeValue, float64(Basecalled_fail_read_count),
			statistic.Address,
			statistic.Name,
			statistic.Seconds,
		)
		ch <- prometheus.MustNewConstMetric(
			basecalledSkippedReadCountMetric, prometheus.GaugeValue, float64(Basecalled_skipped_read_count),
			statistic.Address,
			statistic.Name,
			statistic.Seconds,
		)
		ch <- prometheus.MustNewConstMetric(
			basecalledPassBasesMetric, prometheus.GaugeValue, float64(Basecalled_pass_bases),
			statistic.Address,
			statistic.Name,
			statistic.Seconds,
		)
		ch <- prometheus.MustNewConstMetric(
			basecalledFailBasesMetric, prometheus.GaugeValue, float64(Basecalled_fail_bases),
			statistic.Address,
			statistic.Name,
			statistic.Seconds,
		)
		ch <- prometheus.MustNewConstMetric(
			basecalledSamplesMetric, prometheus.GaugeValue, float64(Basecalled_samples),
			statistic.Address,
			statistic.Name,
			statistic.Seconds,
		)
		ch <- prometheus.MustNewConstMetric(
			selectedRawSamplesMetric, prometheus.GaugeValue, float64(Selected_raw_samples),
			statistic.Address,
			statistic.Name,
			statistic.Seconds,
		)
		ch <- prometheus.MustNewConstMetric(
			selectedEventsMetric, prometheus.GaugeValue, float64(Selected_events),
			statistic.Address,
			statistic.Name,
			statistic.Seconds,
		)
		ch <- prometheus.MustNewConstMetric(
			estimatedSelectedBasesMetric, prometheus.GaugeValue, float64(Estimated_selected_bases),
			statistic.Address,
			statistic.Name,
			statistic.Seconds,
		)
		ch <- prometheus.MustNewConstMetric(
			alignmentMatchesMetric, prometheus.GaugeValue, float64(Alignment_matches),
			statistic.Address,
			statistic.Name,
			statistic.Seconds,
		)
		ch <- prometheus.MustNewConstMetric(
			alignmentMismatchesMetric, prometheus.GaugeValue, float64(Alignment_mismatches),
			statistic.Address,
			statistic.Name,
			statistic.Seconds,
		)
		ch <- prometheus.MustNewConstMetric(
			alignmentInsertionsMetric, prometheus.GaugeValue, float64(Alignment_insertions),
			statistic.Address,
			statistic.Name,
			statistic.Seconds,
		)
		ch <- prometheus.MustNewConstMetric(
			alignmentDeletionsMetric, prometheus.GaugeValue, float64(Alignment_deletions),
			statistic.Address,
			statistic.Name,
			statistic.Seconds,
		)
		ch <- prometheus.MustNewConstMetric(
			alignmentCoverageMetric, prometheus.GaugeValue, float64(Alignment_coverage),
			statistic.Address,
			statistic.Name,
			statistic.Seconds,
		)
	}

	var voltage []*voltageData
	result3, err3 := exec.Command("python3", "python/get_voltage.py").Output()
	if err3 != nil {
		log.Fatal(err3)
	}
	err3 = json.Unmarshal([]byte(result3), &voltage)
	if err3 != nil {
		log.Fatal(err3)
	}
	for _, v := range voltage {
		Bias_voltage, _ := strconv.ParseFloat(v.Bias_voltage, 64)
		ch <- prometheus.MustNewConstMetric(
			biasVoltageMetric, prometheus.GaugeValue, float64(Bias_voltage), v.Address, v.Name,
		)
	}

	var temperature []*temperatureData
	result4, err4 := exec.Command("python3", "python/get_temperature.py").Output()
	if err4 != nil {
		log.Fatal(err4)
	}
	err4 = json.Unmarshal([]byte(result4), &temperature)
	if err4 != nil {
		log.Fatal(err4)
	}
	for _, t := range temperature {
		Target_temp, _ := strconv.ParseFloat(t.Target_temp, 64)
		Asic_temp, _ := strconv.ParseFloat(t.Asic_temp, 64)
		Heat_sink_temp, _ := strconv.ParseFloat(t.Heat_sink_temp, 64)
		ch <- prometheus.MustNewConstMetric(
			targetTempMetric, prometheus.GaugeValue, float64(Target_temp), t.Address, t.Name,
		)
		ch <- prometheus.MustNewConstMetric(
			asicTempMetric, prometheus.GaugeValue, float64(Asic_temp), t.Address, t.Name,
		)
		ch <- prometheus.MustNewConstMetric(
			heatSinkTempMetric, prometheus.GaugeValue, float64(Heat_sink_temp), t.Address, t.Name,
		)
	}

	var channelStates []*channelStatesData
	result5, err5 := exec.Command("python3", "python/get_channel_states.py").Output()
	if err5 != nil {
		log.Fatal(err5)
	}
	err5 = json.Unmarshal([]byte(result5), &channelStates)
	if err5 != nil {
		log.Fatal(err5)
	}
	for _, s := range channelStates {
		Pending_mux_change, _ := strconv.Atoi(s.Pending_mux_change)
		Inrange, _ := strconv.Atoi(s.Inrange)
		Above, _ := strconv.Atoi(s.Above)
		Strand, _ := strconv.Atoi(s.Strand)
		Unavailable, _ := strconv.Atoi(s.Unavailable)
		Below, _ := strconv.Atoi(s.Below)
		Multiple, _ := strconv.Atoi(s.Multiple)
		Saturated, _ := strconv.Atoi(s.Saturated)
		Good_single, _ := strconv.Atoi(s.Good_single)
		Unknown, _ := strconv.Atoi(s.Unknown)
		Unclassified, _ := strconv.Atoi(s.Unclassified)
		Unblocking, _ := strconv.Atoi(s.Unblocking)
		Adapter, _ := strconv.Atoi(s.Adapter)
		ch <- prometheus.MustNewConstMetric(
			pendingMuxChangeMetric, prometheus.GaugeValue, float64(Pending_mux_change), s.Address, s.Name,
		)
		ch <- prometheus.MustNewConstMetric(
			inrangeMetric, prometheus.GaugeValue, float64(Inrange), s.Address, s.Name,
		)
		ch <- prometheus.MustNewConstMetric(
			aboveMetric, prometheus.GaugeValue, float64(Above), s.Address, s.Name,
		)
		ch <- prometheus.MustNewConstMetric(
			strandMetric, prometheus.GaugeValue, float64(Strand), s.Address, s.Name,
		)
		ch <- prometheus.MustNewConstMetric(
			unavailableMetric, prometheus.GaugeValue, float64(Unavailable), s.Address, s.Name,
		)
		ch <- prometheus.MustNewConstMetric(
			belowMetric, prometheus.GaugeValue, float64(Below), s.Address, s.Name,
		)
		ch <- prometheus.MustNewConstMetric(
			multipleMetric, prometheus.GaugeValue, float64(Multiple), s.Address, s.Name,
		)
		ch <- prometheus.MustNewConstMetric(
			saturatedMetric, prometheus.GaugeValue, float64(Saturated), s.Address, s.Name,
		)
		ch <- prometheus.MustNewConstMetric(
			goodSingleMetric, prometheus.GaugeValue, float64(Good_single), s.Address, s.Name,
		)
		ch <- prometheus.MustNewConstMetric(
			unknownMetric, prometheus.GaugeValue, float64(Unknown), s.Address, s.Name,
		)
		ch <- prometheus.MustNewConstMetric(
			unclassifiedMetric, prometheus.GaugeValue, float64(Unclassified), s.Address, s.Name,
		)
		ch <- prometheus.MustNewConstMetric(
			unblockingMetric, prometheus.GaugeValue, float64(Unblocking), s.Address, s.Name,
		)
		ch <- prometheus.MustNewConstMetric(
			adapterMetric, prometheus.GaugeValue, float64(Adapter), s.Address, s.Name,
		)
	}

	/*
		files, _ := ioutil.ReadDir("/tmp/fast5")
		for _, file := range files {
			var vars map[string]interface{}
			result, err := exec.Command("python3", "python/parse_ont_fast5_file.py", "/tmp/fast5/"+file.Name()).Output()
			if err != nil {
				log.Fatal(err)
			}
			err = json.Unmarshal(result, &vars)
			if err != nil {
				log.Fatal(err)
			}
			//fmt.Printf("%#v", vars)

			ch <- prometheus.MustNewConstMetric(
				topicOldestOffset, prometheus.GaugeValue, float64(1), file.Name(),
			)
		}*/

	/*for _, file := range files {
		fmt.Printf("%#v", file)
		ch <- prometheus.MustNewConstMetric(
			clusterBrokers, prometheus.GaugeValue, float64(len(files)),
		)
	}*/
}

func init() {
	metrics.UseNilMetrics = true
	prometheus.MustRegister(version.NewCollector("minknow_exporter"))
}

func toFlagString(name string, help string, value string) *string {
	flag.CommandLine.String(name, value, help) // hack around flag.Parse and klog.init flags
	return kingpin.Flag(name, help).Default(value).String()
}

func toFlagBool(name string, help string, value bool, valueString string) *bool {
	flag.CommandLine.Bool(name, value, help) // hack around flag.Parse and klog.init flags
	return kingpin.Flag(name, help).Default(valueString).Bool()
}

func toFlagStringsVar(name string, help string, value string, target *[]string) {
	flag.CommandLine.String(name, value, help) // hack around flag.Parse and klog.init flags
	kingpin.Flag(name, help).Default(value).StringsVar(target)
}

func toFlagStringVar(name string, help string, value string, target *string) {
	flag.CommandLine.String(name, value, help) // hack around flag.Parse and klog.init flags
	kingpin.Flag(name, help).Default(value).StringVar(target)
}

func toFlagBoolVar(name string, help string, value bool, valueString string, target *bool) {
	flag.CommandLine.Bool(name, value, help) // hack around flag.Parse and klog.init flags
	kingpin.Flag(name, help).Default(valueString).BoolVar(target)
}

func toFlagIntVar(name string, help string, value int, valueString string, target *int) {
	flag.CommandLine.Int(name, value, help) // hack around flag.Parse and klog.init flags
	kingpin.Flag(name, help).Default(valueString).IntVar(target)
}

func main() {
	var (
		ontFast5DirPath = toFlagString("ont-fast5-dir-path", "Path to the dir where fast5 from ONT sequencer will be stored.", "/tmp")
		listenAddress   = toFlagString("web.listen-address", "Address to listen on for web interface and telemetry.", ":9309")
		metricsPath     = toFlagString("web.telemetry-path", "Path under which to expose metrics.", "/metrics")
		topicFilter     = toFlagString("topic.filter", "Regex that determines which topics to collect.", ".*")
		groupFilter     = toFlagString("group.filter", "Regex that determines which consumer groups to collect.", ".*")
		logSarama       = toFlagBool("log.enable-sarama", "Turn on Sarama logging, default is false.", false, "false")

		opts = exporterOpts{}
	)

	toFlagStringsVar("minknow.server", "Address (host:port) of minknow server.", "minknow:9092", &opts.uri)
	toFlagBoolVar("sasl.enabled", "Connect using SASL/PLAIN, default is false.", false, "false", &opts.useSASL)
	toFlagBoolVar("sasl.handshake", "Only set this to false if using a non-minknow SASL proxy, default is true.", true, "true", &opts.useSASLHandshake)
	toFlagStringVar("sasl.username", "SASL user name.", "", &opts.saslUsername)
	toFlagStringVar("sasl.password", "SASL user password.", "", &opts.saslPassword)
	toFlagStringVar("sasl.mechanism", "The SASL SCRAM SHA algorithm sha256 or sha512 or gssapi as mechanism", "", &opts.saslMechanism)
	toFlagStringVar("sasl.service-name", "Service name when using kerberos Auth", "", &opts.serviceName)
	toFlagStringVar("sasl.kerberos-config-path", "Kerberos config path", "", &opts.kerberosConfigPath)
	toFlagStringVar("sasl.realm", "Kerberos realm", "", &opts.realm)
	toFlagStringVar("sasl.kerberos-auth-type", "Kerberos auth type. Either 'keytabAuth' or 'userAuth'", "", &opts.kerberosAuthType)
	toFlagStringVar("sasl.keytab-path", "Kerberos keytab file path", "", &opts.keyTabPath)
	toFlagBoolVar("sasl.disable-PA-FX-FAST", "Configure the Kerberos client to not use PA_FX_FAST, default is false.", false, "false", &opts.saslDisablePAFXFast)
	toFlagBoolVar("tls.enabled", "Connect to minknow using TLS, default is false.", false, "false", &opts.useTLS)
	toFlagStringVar("tls.server-name", "Used to verify the hostname on the returned certificates unless tls.insecure-skip-tls-verify is given. The minknow server's name should be given.", "", &opts.tlsServerName)
	toFlagStringVar("tls.ca-file", "The optional certificate authority file for minknow TLS client authentication.", "", &opts.tlsCAFile)
	toFlagStringVar("tls.cert-file", "The optional certificate file for minknow client authentication.", "", &opts.tlsCertFile)
	toFlagStringVar("tls.key-file", "The optional key file for minknow client authentication.", "", &opts.tlsKeyFile)
	toFlagBoolVar("server.tls.enabled", "Enable TLS for web server, default is false.", false, "false", &opts.serverUseTLS)
	toFlagBoolVar("server.tls.mutual-auth-enabled", "Enable TLS client mutual authentication, default is false.", false, "false", &opts.serverMutualAuthEnabled)
	toFlagStringVar("server.tls.ca-file", "The certificate authority file for the web server.", "", &opts.serverTlsCAFile)
	toFlagStringVar("server.tls.cert-file", "The certificate file for the web server.", "", &opts.serverTlsCertFile)
	toFlagStringVar("server.tls.key-file", "The key file for the web server.", "", &opts.serverTlsKeyFile)
	toFlagBoolVar("tls.insecure-skip-tls-verify", "If true, the server's certificate will not be checked for validity. This will make your HTTPS connections insecure. Default is false", false, "false", &opts.tlsInsecureSkipTLSVerify)
	toFlagStringVar("minknow.version", "Minknow version", sarama.V2_0_0_0.String(), &opts.minknowVersion)
	toFlagBoolVar("use.consumelag.zookeeper", "if you need to use a group from zookeeper, default is false", false, "false", &opts.useZooKeeperLag)
	toFlagStringsVar("zookeeper.server", "Address (hosts) of zookeeper server.", "localhost:2181", &opts.uriZookeeper)
	toFlagStringVar("minknow.labels", "minknow cluster name", "", &opts.labels)
	toFlagStringVar("refresh.metadata", "Metadata refresh interval", "30s", &opts.metadataRefreshInterval)
	toFlagBoolVar("offset.show-all", "Whether show the offset/lag for all consumer group, otherwise, only show connected consumer groups, default is true", true, "true", &opts.offsetShowAll)
	toFlagBoolVar("concurrent.enable", "If true, all scrapes will trigger minknow operations otherwise, they will share results. WARN: This should be disabled on large clusters. Default is false", false, "false", &opts.allowConcurrent)
	toFlagIntVar("topic.workers", "Number of topic workers", 100, "100", &opts.topicWorkers)
	toFlagBoolVar("minknow.allow-auto-topic-creation", "If true, the broker may auto-create topics that we requested which do not already exist, default is false.", false, "false", &opts.allowAutoTopicCreation)
	toFlagIntVar("verbosity", "Verbosity log level", 0, "0", &opts.verbosityLogLevel)

	plConfig := plog.Config{}
	plogflag.AddFlags(kingpin.CommandLine, &plConfig)
	kingpin.Version(version.Print("minknow_exporter"))
	kingpin.HelpFlag.Short('h')
	kingpin.Parse()

	labels := make(map[string]string)

	if opts.labels != "" {
		for _, label := range strings.Split(opts.labels, ",") {
			splitted := strings.Split(label, "=")
			if len(splitted) >= 2 {
				labels[splitted[0]] = splitted[1]
			}
		}
	}

	setup(*ontFast5DirPath, *listenAddress, *metricsPath, *topicFilter, *groupFilter, *logSarama, opts, labels)
}

func setup(
	ontFast5DirPath string,
	listenAddress string,
	metricsPath string,
	topicFilter string,
	groupFilter string,
	logSarama bool,
	opts exporterOpts,
	labels map[string]string,
) {
	klog.InitFlags(flag.CommandLine)
	if err := flag.Set("logtostderr", "true"); err != nil {
		klog.Errorf("Error on setting logtostderr to true: %v", err)
	}
	err := flag.Set("v", strconv.Itoa(opts.verbosityLogLevel))
	if err != nil {
		klog.Errorf("Error on setting v to %v: %v", strconv.Itoa(opts.verbosityLogLevel), err)
	}
	defer klog.Flush()

	klog.V(INFO).Infoln("Starting minknow_exporter", version.Info())
	klog.V(DEBUG).Infoln("Build context", version.BuildContext())

	topicOldestOffset = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "flowcell", "info"),
		"Flowcell info",
		[]string{"address", "name", "state"}, labels,
	)
	readCountMetric = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "read_count"),
		"Stats",
		[]string{
			"address",
			"name",
			"seconds",
		}, labels,
	)
	fractionBasecalledMetric = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "fraction_basecalled"),
		"Stats",
		[]string{
			"address",
			"name",
			"seconds",
		}, labels,
	)
	fractionSkippedMetric = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "fraction_skipped"),
		"Stats",
		[]string{
			"address",
			"name",
			"seconds",
		}, labels,
	)
	basecalledPassReadCountMetric = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "basecalled_pass_read_count"),
		"Stats",
		[]string{
			"address",
			"name",
			"seconds",
		}, labels,
	)
	basecalledFailReadCountMetric = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "basecalled_fail_read_count"),
		"Stats",
		[]string{
			"address",
			"name",
			"seconds",
		}, labels,
	)
	basecalledSkippedReadCountMetric = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "basecalled_skipped_read_count"),
		"Stats",
		[]string{
			"address",
			"name",
			"seconds",
		}, labels,
	)
	basecalledPassBasesMetric = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "basecalled_pass_bases"),
		"Stats",
		[]string{
			"address",
			"name",
			"seconds",
		}, labels,
	)
	basecalledFailBasesMetric = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "basecalled_fail_bases"),
		"Stats",
		[]string{
			"address",
			"name",
			"seconds",
		}, labels,
	)
	basecalledSamplesMetric = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "basecalled_samples"),
		"Stats",
		[]string{
			"address",
			"name",
			"seconds",
		}, labels,
	)
	selectedRawSamplesMetric = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "selected_raw_samples"),
		"Stats",
		[]string{
			"address",
			"name",
			"seconds",
		}, labels,
	)
	selectedEventsMetric = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "selected_events"),
		"Stats",
		[]string{
			"address",
			"name",
			"seconds",
		}, labels,
	)
	estimatedSelectedBasesMetric = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "estimated_selected_bases"),
		"Stats",
		[]string{
			"address",
			"name",
			"seconds",
		}, labels,
	)
	alignmentMatchesMetric = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "alignment_matches"),
		"Stats",
		[]string{
			"address",
			"name",
			"seconds",
		}, labels,
	)
	alignmentMismatchesMetric = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "alignment_mismatches"),
		"Stats",
		[]string{
			"address",
			"name",
			"seconds",
		}, labels,
	)
	alignmentInsertionsMetric = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "alignment_insertions"),
		"Stats",
		[]string{
			"address",
			"name",
			"seconds",
		}, labels,
	)
	alignmentDeletionsMetric = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "alignment_deletions"),
		"Stats",
		[]string{
			"address",
			"name",
			"seconds",
		}, labels,
	)
	alignmentCoverageMetric = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "alignment_coverage"),
		"Stats",
		[]string{
			"address",
			"name",
			"seconds",
		}, labels,
	)
	topicPartitionLeader = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "stats"),
		"Stats",
		[]string{
			"address",
			"name",
			"seconds",
			"read_count",
			"fraction_basecalled",
			"fraction_skipped",
			"basecalled_pass_read_count",
			"basecalled_fail_read_count",
			"basecalled_skipped_read_count",
			"basecalled_pass_bases",
			"basecalled_fail_bases",
			"basecalled_samples",
			"selected_raw_samples",
			"selected_events",
			"estimated_selected_bases",
			"alignment_matches",
			"alignment_mismatches",
			"alignment_insertions",
			"alignment_deletions",
			"alignment_coverage",
		}, labels,
	)
	biasVoltageMetric = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "bias_voltage"),
		"Stats",
		[]string{
			"address",
			"name",
		}, labels,
	)
	targetTempMetric = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "target_temp"),
		"Stats",
		[]string{
			"address",
			"name",
		}, labels,
	)
	asicTempMetric = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "asic_temp"),
		"Stats",
		[]string{
			"address",
			"name",
		}, labels,
	)
	heatSinkTempMetric = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "heat_sink_temp"),
		"Stats",
		[]string{
			"address",
			"name",
		}, labels,
	)
	pendingMuxChangeMetric = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "pending_mux_change"),
		"Stats",
		[]string{
			"address",
			"name",
		}, labels,
	)
	inrangeMetric = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "inrange"),
		"Stats",
		[]string{
			"address",
			"name",
		}, labels,
	)
	aboveMetric = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "above"),
		"Stats",
		[]string{
			"address",
			"name",
		}, labels,
	)
	strandMetric = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "strand"),
		"Stats",
		[]string{
			"address",
			"name",
		}, labels,
	)
	unavailableMetric = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "unavailable"),
		"Stats",
		[]string{
			"address",
			"name",
		}, labels,
	)
	belowMetric = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "below"),
		"Stats",
		[]string{
			"address",
			"name",
		}, labels,
	)
	multipleMetric = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "multiple"),
		"Stats",
		[]string{
			"address",
			"name",
		}, labels,
	)
	saturatedMetric = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "saturated"),
		"Stats",
		[]string{
			"address",
			"name",
		}, labels,
	)
	goodSingleMetric = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "good_single"),
		"Stats",
		[]string{
			"address",
			"name",
		}, labels,
	)
	unknownMetric = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "unknown"),
		"Stats",
		[]string{
			"address",
			"name",
		}, labels,
	)
	unclassifiedMetric = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "unclassified"),
		"Stats",
		[]string{
			"address",
			"name",
		}, labels,
	)
	unblockingMetric = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "unblocking"),
		"Stats",
		[]string{
			"address",
			"name",
		}, labels,
	)
	adapterMetric = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "adapter"),
		"Stats",
		[]string{
			"address",
			"name",
		}, labels,
	)

	if logSarama {
		sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	}

	exporter, err := NewExporter(opts, topicFilter, groupFilter)
	if err != nil {
		//			klog.Fatalln(err)
	}
	//		defer exporter.client.Close()
	prometheus.MustRegister(exporter)

	http.Handle(metricsPath, promhttp.Handler())
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		_, err := w.Write([]byte(`<html>
		        <head><title>Minknow Exporter</title></head>
		        <body>
		        <h1>Minknow Exporter</h1>
		        <p><a href='` + metricsPath + `'>Metrics</a></p>
		        </body>
		        </html>`))
		if err != nil {
			klog.Error("Error handle / request", err)
		}
	})
	http.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		_, err := w.Write([]byte("ok"))
		if err != nil {
			klog.Error("Error handle /healthz request", err)
		}
	})

	if opts.serverUseTLS {
		klog.V(INFO).Infoln("Listening on HTTPS", listenAddress)

		_, err := CanReadCertAndKey(opts.serverTlsCertFile, opts.serverTlsKeyFile)
		if err != nil {
			klog.Error("error reading server cert and key")
		}

		clientAuthType := tls.NoClientCert
		if opts.serverMutualAuthEnabled {
			clientAuthType = tls.RequireAndVerifyClientCert
		}

		certPool := x509.NewCertPool()
		if opts.serverTlsCAFile != "" {
			if caCert, err := ioutil.ReadFile(opts.serverTlsCAFile); err == nil {
				certPool.AppendCertsFromPEM(caCert)
			} else {
				klog.Error("error reading server ca")
			}
		}

		tlsConfig := &tls.Config{
			ClientCAs:                certPool,
			ClientAuth:               clientAuthType,
			MinVersion:               tls.VersionTLS12,
			CurvePreferences:         []tls.CurveID{tls.CurveP521, tls.CurveP384, tls.CurveP256},
			PreferServerCipherSuites: true,
			CipherSuites: []uint16{
				tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
				tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
				tls.TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA,
				tls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256,
				tls.TLS_RSA_WITH_AES_256_GCM_SHA384,
				tls.TLS_RSA_WITH_AES_256_CBC_SHA,
				tls.TLS_RSA_WITH_AES_128_CBC_SHA256,
			},
		}
		server := &http.Server{
			Addr:      listenAddress,
			TLSConfig: tlsConfig,
		}
		klog.Fatal(server.ListenAndServeTLS(opts.serverTlsCertFile, opts.serverTlsKeyFile))
	} else {
		klog.V(INFO).Infoln("Listening on HTTP", listenAddress)
		klog.Fatal(http.ListenAndServe(listenAddress, nil))
	}
}
