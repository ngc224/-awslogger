package awslogger

import (
	"bufio"
	"encoding/json"
	"io"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
)

type AwsLogger struct {
	client                   *cloudwatchlogs.CloudWatchLogs
	eventsInputs             []eventsInput
	eventsInput              eventsInput
	logGroupName             *string
	logStreamName            *string
	sequenceToken            *string
	EventsInputLimitByteSize int
	EventsInputLimitNumber   int
}

type eventsInput struct {
	events   []*cloudwatchlogs.InputLogEvent
	byteSize int
	number   int
}

var (
	eventsInputMaxByteSize = 1048576 - 26
	eventsInputMaxNumber   = 10000
)

func New(logGroupName, logStreamName string, cfgs ...*aws.Config) (*AwsLogger, error) {
	logger := &AwsLogger{
		logGroupName:             &logGroupName,
		logStreamName:            &logStreamName,
		EventsInputLimitByteSize: eventsInputMaxByteSize,
		EventsInputLimitNumber:   eventsInputMaxNumber,
	}

	sess, err := session.NewSession()

	if err != nil {
		return nil, err
	}

	logger.client = cloudwatchlogs.New(sess, cfgs...)
	groups, err := logger.client.DescribeLogGroups(&cloudwatchlogs.DescribeLogGroupsInput{})

	if err != nil {
		return nil, err
	}

	var isGroupExist bool

	for _, v := range groups.LogGroups {
		if *v.LogGroupName == *logger.logGroupName {
			isGroupExist = true
			break
		}
	}

	if !isGroupExist {
		_, err := logger.client.CreateLogGroup(&cloudwatchlogs.CreateLogGroupInput{
			LogGroupName: logger.logGroupName,
		})

		if err != nil {
			return nil, err
		}
	}

	streams, err := logger.client.DescribeLogStreams(&cloudwatchlogs.DescribeLogStreamsInput{
		LogGroupName: logger.logGroupName,
	})

	if err != nil {
		return nil, err
	}

	var isStreamExist bool

	for _, v := range streams.LogStreams {
		if *v.LogStreamName == *logger.logStreamName {
			isStreamExist = true
			logger.sequenceToken = v.UploadSequenceToken
			break
		}
	}

	if !isStreamExist {
		_, err := logger.client.CreateLogStream(&cloudwatchlogs.CreateLogStreamInput{
			LogGroupName:  aws.String(logGroupName),
			LogStreamName: aws.String(logStreamName),
		})

		if err != nil {
			return nil, err
		}
	}

	return logger, nil
}

func (logger *AwsLogger) switchEventsInput() {
	logger.eventsInputs = append(logger.eventsInputs, logger.eventsInput)
	logger.eventsInput = eventsInput{}
}

func (logger *AwsLogger) IsLimit() bool {
	return len(logger.eventsInputs) > 0
}

func (logger *AwsLogger) Put(message string, timestamps ...int64) *AwsLogger {
	if logger.eventsInput.number >= logger.EventsInputLimitNumber {
		logger.switchEventsInput()
	}

	if (logger.eventsInput.byteSize + len(message)) > logger.EventsInputLimitByteSize {
		logger.switchEventsInput()
	}

	timestamp := time.Now().UnixNano() / int64(time.Millisecond)

	if len(timestamps) > 0 {
		timestamp = timestamps[0]
	}

	logger.eventsInput.byteSize += len(message)
	logger.eventsInput.number++
	logger.eventsInput.events = append(logger.eventsInput.events, &cloudwatchlogs.InputLogEvent{
		Message:   aws.String(message),
		Timestamp: aws.Int64(timestamp),
	})

	return logger
}

func (logger *AwsLogger) Write() error {
	eventsInputs := logger.eventsInputs

	if len(logger.eventsInput.events) > 0 {
		eventsInputs = append(eventsInputs, logger.eventsInput)
	}

	logger.eventsInputs = []eventsInput{}
	logger.eventsInput = eventsInput{}

	var writeErr error

	for _, v := range eventsInputs {
		resp, err := logger.client.PutLogEvents(
			&cloudwatchlogs.PutLogEventsInput{
				LogEvents:     v.events,
				LogGroupName:  logger.logGroupName,
				LogStreamName: logger.logStreamName,
				SequenceToken: logger.sequenceToken,
			},
		)

		if err != nil {
			writeErr = err
			logger.eventsInputs = append(logger.eventsInputs, v)
		}

		logger.sequenceToken = resp.NextSequenceToken
	}

	return writeErr
}

func (logger *AwsLogger) WriteFile(filename string) error {
	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)

	if err != nil {
		return err
	}

	defer file.Close()

	writer := bufio.NewWriter(file)
	newLine := []byte("\n")

	events := []*cloudwatchlogs.InputLogEvent{}

	for _, v := range logger.eventsInputs {
		events = append(events, v.events...)
	}

	events = append(events, logger.eventsInput.events...)
	data, err := json.Marshal(events)

	if err != nil {
		return err
	}

	writer.Write(append(data, newLine...))

	if err := writer.Flush(); err != nil {
		return err
	}

	logger.eventsInputs = []eventsInput{}
	logger.eventsInput = eventsInput{}

	return nil
}

func (logger *AwsLogger) ReadFile(filename string) error {
	fp, err := os.Open(filename)

	if err != nil {
		return err
	}

	d := json.NewDecoder(fp)
	events := []*cloudwatchlogs.InputLogEvent{}

	for {
		err := d.Decode(&events)

		if err != nil {
			if err == io.EOF {
				break
			} else {
				return err
			}
		}

		for _, v := range events {
			logger.Put(*v.Message, *v.Timestamp)
		}
	}

	return nil
}
