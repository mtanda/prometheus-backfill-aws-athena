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
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/athena"
	"github.com/aws/aws-sdk-go/service/sts"
	plog "github.com/go-kit/kit/log"
	"github.com/mtanda/prometheus-backfill-aws-athena/models"
	"github.com/prometheus/prometheus/tsdb"
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
	AssumeRoleArn string `yaml:"assumeRoleArn"`
	Region        string
	Query         string
	Workgroup     string
	Interval      string
	Offset        string
	MaxSeries     int `yaml:"maxSeries"`
}

type config struct {
	Queries []query
}

func main() {
	ctx := context.Background()

	var configFile string
	flag.StringVar(&configFile, "config.file", "./backfill.yml", "Configuration file path.")
	var dstPath string
	flag.StringVar(&dstPath, "tsdb.path", "", "Prometheus TSDB path")
	var tmpPath string
	flag.StringVar(&tmpPath, "tsdb.tmp.path", "", "Prometheus TSDB path")
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

	LaunchPrometheusBackfill(ctx, cfg, dstPath, tmpPath)
}

func LaunchPrometheusBackfill(ctx context.Context, cfg config, dstPath string, tmpPath string) {
	interval, err := time.ParseDuration(cfg.Queries[0].Interval)
	if err != nil {
		log.Fatal(err)
	}
	if cfg.Queries[0].Offset == "" {
		cfg.Queries[0].Offset = "0s"
	}
	offset, err := time.ParseDuration(cfg.Queries[0].Offset)
	if err != nil {
		log.Fatal(err)
	}
	timer := time.NewTimer(getTimerDuration(interval, offset))

	for range timer.C {
		now := time.Now().UTC().Truncate(interval)
		srcPath := tmpPath + now.Format("20060102_150405")
		totalNumberOfMessagesWillBeSent := int64(2e4)
		ch := make(chan interface{}, bufferedChanCap)
		bh := prometheus_backfill.NewPrometheusBackfillHandler(blockDuration, maxPerAppender,
			storeBstThreshold, semaphoreWeight, ch,
			totalNumberOfMessagesWillBeSent, srcPath,
		)
		go getQueryResult(ctx, ch, cfg.Queries[0])
		bh.RunJob()
		//w := tabwriter.NewWriter(os.Stdout, 1, 2, 5, ' ', tabwriter.DiscardEmptyColumns)
		//mem := runtime.MemStats{}
		//bh.PrintStats(w, mem)

		logger := plog.NewNopLogger()
		db, err := tsdb.OpenDBReadOnly(
			srcPath,
			logger,
		)
		if err != nil {
			log.Fatal(err)
		}
		blocks, err := db.Blocks()
		if err != nil {
			log.Fatal(err)
		}
		skipImport := false
		for _, block := range blocks {
			if cfg.Queries[0].MaxSeries > 0 && block.Meta().Stats.NumSeries > uint64(cfg.Queries[0].MaxSeries) {
				skipImport = true
				break
			}
		}
		db.Close()

		if !skipImport {
			err = importTSDB(srcPath, dstPath)
			if err != nil {
				log.Fatal(err)
			}
		} else {
			log.Printf("exceed max series limit, path = %s", srcPath)
		}

		timer.Reset(getTimerDuration(interval, offset))
	}
}

func getTimerDuration(interval time.Duration, offset time.Duration) time.Duration {
	now := time.Now().UTC()
	t := now.Truncate(interval).Add(interval).Add(offset)
	if t.Before(now) {
		t = t.Add(interval)
	}
	d := t.Sub(now)
	if d > 24*time.Hour {
		d -= 24 * time.Hour
	}
	return d
}

func getQueryResult(ctx context.Context, ch chan interface{}, query query) {
	cfg := aws.NewConfig().WithRegion(query.Region)
	sess := session.Must(session.NewSession())
	if query.AssumeRoleArn != "" {
		assumeRole := sts.New(sess)
		creds := stscreds.NewCredentialsWithClient(assumeRole, query.AssumeRoleArn)
		cfg = cfg.WithCredentials(creds)
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

	isFirstPage := true
	if err := client.GetQueryResultsPagesWithContext(
		ctx,
		&athena.GetQueryResultsInput{
			QueryExecutionId: q.QueryExecutionId,
			MaxResults:       aws.Int64(1000),
		},
		func(page *athena.GetQueryResultsOutput, lastPage bool) bool {
			if m, err := parse(page, isFirstPage); err != nil {
				log.Fatal(err)
				return false
			} else {
				ch <- m
			}
			isFirstPage = false
			return !lastPage
		}); err != nil {
		log.Fatal(err)
		return
	}

	close(ch)
}

func parse(results *athena.GetQueryResultsOutput, isFirstPage bool) ([]*models.AthenaRow, error) {
	ms := make([]*models.AthenaRow, 0)
	columnInfo := results.ResultSet.ResultSetMetadata.ColumnInfo
	offset := 0
	if isFirstPage {
		offset = 1
	}
	for _, row := range results.ResultSet.Rows[offset:] {
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
