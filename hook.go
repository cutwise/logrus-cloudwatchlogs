package logrus_cloudwatchlogs

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"sync"
	"time"
)

type CloudWatchLogsHook struct {
	client       *cloudwatchlogs.Client
	group        *string
	stream       *string
	nextSeqToken *string
	queue        chan types.InputLogEvent
	tick         *time.Ticker
	mux          *sync.RWMutex
	ErrCh        chan error
}

type CloudWatchLogsHookOptions struct {
	AwsConfig     aws.Config
	GroupName     string
	StreamNameFn  func() string
	BatchDuration time.Duration
	BatchMaxSize  int
}

func (h *CloudWatchLogsHook) Levels() []logrus.Level {
	return []logrus.Level{
		logrus.PanicLevel,
		logrus.FatalLevel,
		logrus.ErrorLevel,
		logrus.WarnLevel,
		logrus.InfoLevel,
		logrus.DebugLevel,
	}
}

func NewCloudWatchLogsHook(ctx context.Context, options *CloudWatchLogsHookOptions) (*CloudWatchLogsHook, error) {
	cwlog := &CloudWatchLogsHook{
		client:       cloudwatchlogs.NewFromConfig(options.AwsConfig),
		group:        aws.String(options.GroupName),
		stream:       aws.String(options.StreamNameFn()),
		nextSeqToken: nil,

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

	go cwlog.batchCycle(ctx)

	return cwlog, nil
}

func (h *CloudWatchLogsHook) Fire(entry *logrus.Entry) error {
	line, err := entry.String()
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
		h.putEvent(line, entry.Time)
	}

	return nil
}

func (h *CloudWatchLogsHook) putEvent(evMessage string, evTime time.Time) {
	h.mux.Lock()
	h.queue <- types.InputLogEvent{
		Message:   aws.String(evMessage),
		Timestamp: aws.Int64(evTime.UnixNano() / int64(time.Millisecond)),
	}
	h.mux.Unlock()
}

func (h *CloudWatchLogsHook) sendLogs(ctx context.Context, list []types.InputLogEvent) {
	out, err := h.client.PutLogEvents(ctx, &cloudwatchlogs.PutLogEventsInput{
		LogEvents:     list,
		SequenceToken: h.nextSeqToken,
		LogGroupName:  h.group,
		LogStreamName: h.stream,
	})
	if err != nil {
		h.ErrCh <- errors.Wrap(err, "CloudWatchLogs logs send")
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

func (h *CloudWatchLogsHook) batchCycle(ctx context.Context) {
	for {
		select {
		case <-h.tick.C:
			eventsList := h.flushList()
			if len(eventsList) > 0 {
				h.sendLogs(ctx, eventsList)
			}

		case <-ctx.Done():
			h.Close()
			return
		}
	}
}

func (h *CloudWatchLogsHook) Close() {
	h.tick.Stop()
}
