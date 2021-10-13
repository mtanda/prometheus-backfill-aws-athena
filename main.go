package main

import (
	"context"
	"flag"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"time"

	prometheus_backfill "github.com/aleskandro/go-prometheus-backfiller"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/athena"
	"github.com/mtanda/prometheus-backfill-aws-athena/models"
	yaml "gopkg.in/yaml.v2"
)

var (
	blockDuration     = int64(2 * time.Hour / time.Millisecond)
	maxPerAppender    = int64(100e6)
	storeBstThreshold = int64(1e3)
	bufferedChanCap   = 128
	semaphoreWeight   = int64(32)
)

type query struct {
	Region    string
	Query     string
	Workgroup string
	Interval  string
}

type config []query

func main() {
	ctx := context.Background()

	var configFile string
	flag.StringVar(&configFile, "config.file", "./backfill.yml", "Configuration file path.")
	var dstPath string
	flag.StringVar(&dstPath, "tsdb.path", "", "Prometheus TSDB path")
	flag.Parse()

	buf, err := ioutil.ReadFile(configFile)
	if err != nil {
		log.Fatal(err)
	}

	var cfg config
	err = yaml.Unmarshal(buf, &cfg)
	if err != nil {
		log.Fatal(err)
	}

	LaunchPrometheusBackfill(ctx, cfg, dstPath)
}

func LaunchPrometheusBackfill(ctx context.Context, cfg config, dstPath string) {
	interval, err := time.ParseDuration(cfg[0].Interval)
	if err != nil {
		log.Fatal(err)
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for ; true; <-ticker.C {
		now := time.Now().UTC().Truncate(interval)
		srcPath := "./data/" + now.Format("20060102_150405")
		totalNumberOfMessagesWillBeSent := int64(2e4)
		ch := make(chan interface{}, bufferedChanCap)
		bh := prometheus_backfill.NewPrometheusBackfillHandler(blockDuration, maxPerAppender,
			storeBstThreshold, semaphoreWeight, ch,
			totalNumberOfMessagesWillBeSent, srcPath,
		)
		go getQueryResult(ctx, ch, cfg[0])
		bh.RunJob()
		//w := tabwriter.NewWriter(os.Stdout, 1, 2, 5, ' ', tabwriter.DiscardEmptyColumns)
		//mem := runtime.MemStats{}
		//bh.PrintStats(w, mem)
		err := importTSDB(srcPath, dstPath)
		if err != nil {
			log.Fatal(err)
		}
	}
}

func getQueryResult(ctx context.Context, ch chan interface{}, query query) {
	cfg := &aws.Config{
		Region: aws.String(query.Region),
	}
	sess, err := session.NewSession(cfg)
	if err != nil {
		log.Fatal(err)
		return
	}

	client := athena.New(sess, cfg)
	q, err := client.StartQueryExecutionWithContext(ctx, &athena.StartQueryExecutionInput{
		QueryString: aws.String(query.Query),
		WorkGroup:   aws.String(query.Workgroup),
	})
	if err != nil {
		log.Fatal(err)
		return
	}

	completed := false
	for !completed {
		qe, err := client.GetQueryExecution(&athena.GetQueryExecutionInput{
			QueryExecutionId: q.QueryExecutionId,
		})
		if err != nil {
			time.Sleep(1 * time.Second)
			continue
		}
		if *qe.QueryExecution.Status.State == "QUEUED" || *qe.QueryExecution.Status.State == "RUNNING" {
			time.Sleep(1 * time.Second)
			continue
		}
		if *qe.QueryExecution.Status.State == "SUCCEEDED" {
			completed = true
		} else {
			log.Fatal(err)
			return
		}
	}

	if err := client.GetQueryResultsPagesWithContext(
		ctx,
		&athena.GetQueryResultsInput{
			QueryExecutionId: q.QueryExecutionId,
			MaxResults:       aws.Int64(1000),
		},
		func(page *athena.GetQueryResultsOutput, lastPage bool) bool {
			if m, err := parse(page); err != nil {
				log.Fatal(err)
				return false
			} else {
				ch <- m
			}
			return !lastPage
		}); err != nil {
		log.Fatal(err)
		return
	}

	close(ch)
}

func parse(results *athena.GetQueryResultsOutput) ([]*models.AthenaRow, error) {
	ms := make([]*models.AthenaRow, 0)
	columnInfo := results.ResultSet.ResultSetMetadata.ColumnInfo
	for _, row := range results.ResultSet.Rows[1:] {
		m := models.AthenaRow{}
		m.Labels = make(map[string]string)
		for columnIdx, cell := range row.Data {
			columnName := columnInfo[columnIdx].Name
			switch *columnName {
			case "timestamp":
				if cval, err := strconv.ParseInt(*cell.VarCharValue, 10, 64); err != nil {
					return nil, err
				} else {
					m.Timestamp = cval
				}
			case "value":
				if cval, err := strconv.ParseFloat(*cell.VarCharValue, 64); err != nil {
					return nil, err
				} else {
					m.Value = cval
				}
			default:
				m.Labels[*columnName] = *cell.VarCharValue
			}
		}
		ms = append(ms, &m)
	}
	return ms, nil
}

func importTSDB(srcPath string, dstPath string) error {
	_, err := os.Stat(dstPath)
	if os.IsNotExist(err) {
		return err
	}

	files, err := ioutil.ReadDir(srcPath)
	if err != nil {
		return err
	}

	for _, file := range files {
		fname := file.Name()
		if fname == "wal" || fname == "chunks_head" {
			continue
		}
		err := os.Rename(srcPath+"/"+fname, dstPath+"/"+fname)
		if err != nil {
			return err
		}
	}
	err = os.RemoveAll(srcPath)
	if err != nil {
		return err
	}

	return nil
}