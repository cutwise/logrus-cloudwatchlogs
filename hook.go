package logrus_cloudwatchlogs

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"sort"
	"sync"
	"time"
)

type CloudWatchLogsHook struct {
	client       *cloudwatchlogs.Client
	formatter    logrus.Formatter
	levels       []logrus.Level
	group        *string
	stream       *string
	nextSeqToken *string
	queue        chan types.InputLogEvent
	tick         *time.Ticker
	mux          *sync.RWMutex
	wg           sync.WaitGroup
}

type CloudWatchLogsHookOptions struct {
	AwsConfig     aws.Config
	GroupName     string
	StreamNameFn  func() string
	BatchDuration time.Duration
	BatchMaxSize  int
	Formatter     logrus.Formatter
	Levels        []logrus.Level
}

func (h *CloudWatchLogsHook) Levels() []logrus.Level {
	return h.levels
}

func NewCloudWatchLogsHook(ctx context.Context, options *CloudWatchLogsHookOptions) (*CloudWatchLogsHook, error) {
	cwlog := &CloudWatchLogsHook{
		client:       cloudwatchlogs.NewFromConfig(options.AwsConfig),
		group:        &options.GroupName,
		stream:       aws.String(options.StreamNameFn()),
		nextSeqToken: nil,
		levels:       options.Levels,
		formatter:    options.Formatter,

		queue: make(chan types.InputLogEvent, options.BatchMaxSize),
		tick:  time.NewTicker(options.BatchDuration),
		mux:   new(sync.RWMutex),
	}

	_, err := cwlog.client.CreateLogStream(ctx, &cloudwatchlogs.CreateLogStreamInput{
		LogGroupName:  cwlog.group,
		LogStreamName: cwlog.stream,
	})
	if err != nil {
		return nil, errors.Wrap(err, "CloudWatchLogs create stream")
	}

	if cwlog.formatter == nil {
		cwlog.formatter = &logrus.JSONFormatter{
			PrettyPrint: false,
		}
	}

	if len(cwlog.levels) == 0 {
		cwlog.levels = []logrus.Level{
			logrus.PanicLevel,
			logrus.FatalLevel,
			logrus.ErrorLevel,
			logrus.WarnLevel,
			logrus.InfoLevel,
			logrus.DebugLevel,
		}
	}

	go cwlog.batchCycle(ctx)

	return cwlog, nil
}

func (h *CloudWatchLogsHook) Fire(entry *logrus.Entry) error {
	payload, err := h.formatter.Format(entry)
	if err != nil {
		return errors.Wrap(err, "Unable to read entry")
	}

	switch entry.Level {
	case logrus.PanicLevel:
		fallthrough
	case logrus.FatalLevel:
		fallthrough
	case logrus.ErrorLevel:
		fallthrough
	case logrus.WarnLevel:
		fallthrough
	case logrus.InfoLevel:
		fallthrough
	case logrus.DebugLevel:
		h.putEvent(string(payload), entry.Time)
	}

	return nil
}

func (h *CloudWatchLogsHook) putEvent(evMessage string, evTime time.Time) {
	h.mux.Lock()
	h.wg.Add(1)
	h.queue <- types.InputLogEvent{
		Message:   &evMessage,
		Timestamp: aws.Int64(evTime.UnixNano() / int64(time.Millisecond)),
	}
	h.mux.Unlock()
}

func (h *CloudWatchLogsHook) sendLogs(ctx context.Context, list []types.InputLogEvent) {
	defer h.wg.Add(-len(list))

	// Log events in a single PutLogEvents request must be in chronological order.
	sort.Slice(list, func(i, j int) bool {
		return *list[i].Timestamp < *list[j].Timestamp
	})

	out, err := h.client.PutLogEvents(ctx, &cloudwatchlogs.PutLogEventsInput{
		LogEvents:     list,
		SequenceToken: h.nextSeqToken,
		LogGroupName:  h.group,
		LogStreamName: h.stream,
	})
	if err != nil {
		fmt.Println(errors.Wrap(err, "CloudWatchLogs logs send"))
		return
	}

	h.nextSeqToken = out.NextSequenceToken
}

func (h *CloudWatchLogsHook) flushList() (list []types.InputLogEvent) {
	h.mux.Lock()
	defer h.mux.Unlock()

	length := len(h.queue)
	if length == 0 {
		return
	}

	list = make([]types.InputLogEvent, length)
	for i := 0; i < length; i++ {
		list[i] = <-h.queue
	}

	return
}

func (h *CloudWatchLogsHook) sendBatch(ctx context.Context) {
	eventsList := h.flushList()
	if len(eventsList) > 0 {
		h.sendLogs(ctx, eventsList)
	}
}

func (h *CloudWatchLogsHook) batchCycle(ctx context.Context) {
	for {
		select {
		case <-h.tick.C:
			h.sendBatch(ctx)
		case <-ctx.Done():
			h.Close(ctx)
			return
		}
	}
}

func (h *CloudWatchLogsHook) Close(ctx context.Context) {
	h.tick.Stop()
	h.sendBatch(ctx)
	h.wg.Wait()
}
