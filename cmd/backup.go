package cmd

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"sync"
	"time"

	"github.com/RingierIMU/timestream-travel/helpers"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/timestreamquery"
	"github.com/jinzhu/now"
	"github.com/urfave/cli/v2"
	"go.uber.org/zap"
)

const (
	queryTimeFormat    = "2006-01-02 15:04:05"
	dirTimeFormat      = "20060102"
	filenameTimeFormat = "20060102T150405Z"
)

var Backup = &cli.Command{
	Name:    "backup",
	Aliases: []string{"b"},
	Usage:   "backup time-series data from Amazon Timestream",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:    "region",
			Aliases: []string{"r"},
			Usage:   "AWS `REGION`",
			Value:   "eu-west-1",
		},
		&cli.StringFlag{
			Name:    "database",
			Aliases: []string{"db"},
			Usage:   "Amazon Timestream `DATABASE`",
			Value:   "TestDB",
		},
		&cli.StringFlag{
			Name:    "table",
			Aliases: []string{"tbl"},
			Usage:   "Amazon Timestream `TABLE`",
			Value:   "IoT",
		},
		&cli.StringFlag{
			Name:    "bucket",
			Aliases: []string{"b"},
			Usage:   "Amazon S3 `BUCKET_NAME`",
			Value:   "test-playday-bucket",
		},
		&cli.StringFlag{
			Name:    "column",
			Aliases: []string{"c"},
			Usage:   "Amazon Timestream `COLUMN` to partition time-series data",
			Value:   "measure_name",
		},
		&cli.StringFlag{
			Name:  "from",
			Usage: "Amazon Timestream `FROM` time value",
			Value: "2021-06-09 00:00:00",
		},
		&cli.StringFlag{
			Name:  "to",
			Usage: "Amazon Timestream `TO` time value",
			Value: "2021-06-12 00:00:00",
		},
		&cli.Int64Flag{
			Name:  "rows",
			Usage: "Number of `ROWS` per chunk",
			Value: 1000,
		},
		&cli.BoolFlag{
			Name:    "debug",
			Aliases: []string{"d"},
			Usage:   "To `DEBUG` or NOT",
			Value:   false,
		},
	},
	Action: func(c *cli.Context) error {
		logger, _ := zap.NewProduction()
		if c.Bool("debug") {
			logger, _ = zap.NewDevelopment()
		}
		defer logger.Sync()
		sugar := logger.Sugar()

		region := c.String("region")

		sugar.Debugw("creating new aws session...", "region", region)

		sess, err := session.NewSession(&aws.Config{
			Region: aws.String(region),
		})
		if err != nil {
			sugar.Errorw("create new aws session", "error", err, "region", region)
			return fmt.Errorf("create new aws session: %v", err)
		}

		// AWS Services
		sugar.Debug("creating timestreamquery and s3uploader services...")
		querySvc := timestreamquery.New(sess)
		uploader := s3manager.NewUploader(sess)

		database := c.String("database")
		table := c.String("table")
		partitionColumn := c.String("column")

		// TODO(mike): investigate how to implement relative string time (e.g "now", "+2h", "-2d", etc) 🧐
		from, err := now.Parse(c.String("from"))
		if err != nil {
			sugar.Errorw("time format from", "error", err, "from", c.String("from"))
			return fmt.Errorf("format from: %v", err)
		}

		to, err := now.Parse(c.String("to"))
		if err != nil {
			sugar.Errorw("time format to", "error", err, "to", c.String("to"))
			return fmt.Errorf("format to: %v", err)
		}

		sql := fmt.Sprintf(
			"SELECT %s FROM \"%s\".\"%s\" WHERE time >= '%s' and time <= '%s' GROUP BY %s",
			partitionColumn,
			database,
			table,
			from.Format(queryTimeFormat),
			to.Format(queryTimeFormat),
			partitionColumn,
		)

		sugar.Debugw("retrieving partition values...", "sql", sql)

		partitionOutput, err := querySvc.QueryWithContext(c.Context, &timestreamquery.QueryInput{
			QueryString: aws.String(sql),
			MaxRows:     aws.Int64(100),
		})
		if err != nil {
			sugar.Errorw("retrieve partition values", "error", err, "sql", sql)
			return fmt.Errorf("retrieve partition values: %v", err)
		}

		var partitionValues []string
		for _, row := range partitionOutput.Rows {
			if row.Data[0].ScalarValue == nil {
				continue
			}
			partitionValues = append(partitionValues, *row.Data[0].ScalarValue)
		}

		sugar.Debugw("partition", "column", partitionColumn, "values", partitionValues)

		maxRows := c.Int64("rows")
		if maxRows > 1000 {
			sugar.Warnw("maxRows cannot exceed 1000. maxRows set to 1000.", "maxRows given", maxRows)
			maxRows = 1000
		}

		errorsCh := make(chan error, 1000)

		var mu sync.Mutex
		totalRowsPerPartitions := make(map[string]int)

		sugar.Infow("backing up all data...", "from", from.Format(queryTimeFormat), "to", to.Format(queryTimeFormat), "partitions", partitionValues)

		startTimeAllPartition := time.Now()

		var wg sync.WaitGroup
		for _, partitionValue := range partitionValues {
			wg.Add(1)
			go func(partitionValue string) {
				defer wg.Done()

				mu.Lock()
				totalRowsPerPartitions[partitionValue] = 0
				mu.Unlock()

				startTimeSinglePartition := time.Now()

				sql := fmt.Sprintf(
					"SELECT * FROM \"%s\".\"%s\" WHERE time >= '%s' and time <= '%s' AND %s = '%s' ORDER BY time DESC",
					database,
					table,
					from.Format(queryTimeFormat),
					to.Format(queryTimeFormat),
					partitionColumn,
					partitionValue,
				)

				sugar.Debugw("query", "partition", partitionValue, "sql", sql)
				sugar.Infow("backing up data...", "partition", partitionValue)

				err = querySvc.QueryPagesWithContext(c.Context,
					&timestreamquery.QueryInput{
						QueryString: aws.String(sql),
						MaxRows:     aws.Int64(maxRows),
					},
					func(page *timestreamquery.QueryOutput, lastPage bool) bool {
						if len(page.Rows) == 0 {
							return true
						}

						inMemoryStore := bytes.NewBuffer([]byte{})
						gzipWriter, _ := gzip.NewWriterLevel(inMemoryStore, gzip.BestCompression)

						var from, to time.Time
						var processedRows int
						for i, row := range page.Rows {
							processedRow := helpers.ProcessRowType(row.Data, page.ColumnInfo)

							if i == 0 {
								to, err = now.Parse(processedRow["time"].(string))
								if err != nil {
									sugar.Warnw("time format to", "error", err, "to", c.String("to"), "partition", partitionValue)
								}
							} else if i == len(page.Rows)-1 {
								from, err = now.Parse(processedRow["time"].(string))
								if err != nil {
									sugar.Warnw("time format from", "error", err, "from", c.String("from"), "partition", partitionValue)
								}
							}

							// Convert to JSON
							marshaledRow, err := json.Marshal(processedRow)
							if err != nil {
								sugar.Errorw("marshal data", "error", err, "partition", partitionValue, "from", from.Format(queryTimeFormat), "to", to.Format(queryTimeFormat))
								errorsCh <- fmt.Errorf("marshal data: %v", err)
								continue
							}

							// Write to inMemoryStore
							fmt.Fprintf(gzipWriter, "%s\n", marshaledRow)

							processedRows++
						}

						gzipWriter.Close()

						mu.Lock()
						totalRowsPerPartitions[partitionValue] += processedRows
						mu.Unlock()

						bucket := c.String("bucket")
						checksum := crc32.ChecksumIEEE(inMemoryStore.Bytes())

						// Key format: <partition>/<date>/<range_timestamp>.json.gz
						// Example: TestDB/IoT/20210223/20210223T150426Z_20210223T150456Z_0ffab703.json
						key := fmt.Sprintf("%s/%s/%s/%s/%s_%s_%08x.log.gz", database, table, partitionValue, from.Format(dirTimeFormat), from.Format(filenameTimeFormat), to.Format(filenameTimeFormat), checksum)

						sugar.Debugw("uploading data...", "bucket", bucket, "key", key, "partition", partitionValue, "from", from.Format(queryTimeFormat), "to", to.Format(queryTimeFormat), "rows", processedRows)

						result, err := uploader.UploadWithContext(c.Context, &s3manager.UploadInput{
							Bucket: aws.String(bucket),
							Key:    aws.String(key),
							Body:   inMemoryStore,
						})
						if err != nil {
							sugar.Errorw("upload data", "error", err, "bucket", bucket, "key", key, "partition", partitionValue, "from", from.Format(queryTimeFormat), "to", to.Format(queryTimeFormat))
							errorsCh <- fmt.Errorf("upload data: %v", err)
							return true
						}

						sugar.Debugw("uploaded data", "path", result.Location, "partition", partitionValue, "rows", processedRows)

						return true
					},
				)
				if err != nil {
					sugar.Errorw("query with partition value", "error", err, "partition", partitionValue, "from", from.Format(queryTimeFormat), "to", to.Format(queryTimeFormat))
					errorsCh <- fmt.Errorf("query with partition value: %v", err)
					return
				}
				sugar.Infow("finished backing up data", "partition", partitionValue, "rows", totalRowsPerPartitions[partitionValue], "time taken", time.Since(startTimeSinglePartition).String())

			}(partitionValue)
		}

		wg.Wait()

		close(errorsCh)

		for err := range errorsCh {
			if err != nil {
				return errors.New("encountered errors")
			}
		}

		sugar.Infow("finished backing up all data", "from", from.Format(queryTimeFormat), "to", to.Format(queryTimeFormat), "rows/partition", totalRowsPerPartitions, "time taken", time.Since(startTimeAllPartition).String())

		return nil
	},
}
