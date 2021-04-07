package logrus_cloudwatchlogs

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"math/rand"
	"sync"
	"testing"
	"time"
)

func generateLogStreamName() string {
	return fmt.Sprintf("%s-%d", time.Now().Format("01-01-2021"), time.Now().UnixNano())
}

func TestBatching(t *testing.T) {
	ctx := context.TODO()
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion("us-east-1"))
	require.NoError(t, err)

	hook, err := NewCloudWatchLogsHook(ctx, &CloudWatchLogsHookOptions{
		AwsConfig:     cfg,
		GroupName:     "test_group",
		StreamNameFn:  generateLogStreamName,
		BatchDuration: 100 * time.Millisecond,
		BatchMaxSize:  1024,
	})
	require.NoError(t, err)
	defer hook.Close(ctx)

	logger := logrus.New()
	logger.AddHook(hook)
	logger.SetFormatter(&logrus.TextFormatter{
		DisableColors:    true,
		DisableTimestamp: true,
	})
	contextLogger := logger.WithField("server", "test.example.com").WithField("env", "test")

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			contextLogger.Info("Test INFO #", i)
			contextLogger.Warn("Test WARN #", i)
			contextLogger.Error("Test ERROR #", i)
			contextLogger.Debug("Test DEBUG #", i)
			time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
		}(i)
	}
	wg.Wait()
}
