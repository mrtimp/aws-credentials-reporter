package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/jessevdk/go-flags"
	"golang.org/x/sync/errgroup"
)

type profileResult struct {
	idx     int
	profile string
	header  []string
	rows    [][]string
}

var opts struct {
	Profiles    []string `short:"p" long:"profile" description:"AWS profiles to include (can be repeated)" required:"true"`
	Output      string   `short:"o" long:"output" description:"CSV file to save the credentials report to" default:"aws-credentials-report.csv"`
	ExcludeRoot bool     `long:"exclude-root" description:"Exclude accounts root users from the credentials report"`
}

func generateCredentialReport(ctx context.Context, profile string) ([]string, [][]string, error) {
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithSharedConfigProfile(profile),
		config.WithRegion("us-east-1"),
	)

	if err != nil {
		return nil, nil, fmt.Errorf("loading profile %q: %w", profile, err)
	}

	client := iam.NewFromConfig(cfg)

	if _, err := client.GenerateCredentialReport(ctx, &iam.GenerateCredentialReportInput{}); err != nil {
		return nil, nil, fmt.Errorf("generate credential report for %q: %w", profile, err)
	}

	var report *iam.GetCredentialReportOutput
	for i := 0; i < 10; i++ {
		report, err = client.GetCredentialReport(ctx, &iam.GetCredentialReportInput{})

		if err == nil {
			break
		}

		time.Sleep(1 * time.Second)
	}

	if err != nil {
		return nil, nil, fmt.Errorf("fetch credential report for %q: %w", profile, err)
	}

	records, err := csv.NewReader(bytes.NewReader(report.Content)).ReadAll()

	if err != nil {
		return nil, nil, fmt.Errorf("parse CSV for %q: %w", profile, err)
	}

	if len(records) == 0 {
		return nil, nil, nil
	}

	return records[0], records[1:], nil
}

func main() {
	if _, err := flags.Parse(&opts); err != nil {
		os.Exit(1)
	}

	// deduplicate profiles, preserving order
	unique := make([]string, 0, len(opts.Profiles))
	processed := make(map[string]struct{})

	for _, profile := range opts.Profiles {
		if _, exists := processed[profile]; !exists {
			processed[profile] = struct{}{}
			unique = append(unique, profile)
		}
	}

	opts.Profiles = unique

	ctx := context.Background()
	var eg errgroup.Group
	sem := make(chan struct{}, 4) // fetch 4 at once
	results := make([]profileResult, len(opts.Profiles))

	for i, profile := range opts.Profiles {
		i, profile := i, profile

		eg.Go(func() error {
			sem <- struct{}{}
			defer func() { <-sem }()

			header, rows, err := generateCredentialReport(ctx, profile)

			if err != nil {
				return err
			}

			results[i] = profileResult{idx: i, profile: profile, header: header, rows: rows}

			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		log.Fatalf("error fetching reports: %v", err)
	}

	// sort results
	sort.Slice(results, func(i, j int) bool {
		return results[i].idx < results[j].idx
	})

	// look for the arn column for root user filtering
	arnIdx := -1
	if len(results) > 0 {
		for i, h := range results[0].header {
			if h == "arn" {
				arnIdx = i
				break
			}
		}

		if arnIdx < 0 {
			log.Fatalf("unable to locate arn column in report header")
		}
	}

	allResults := make([][]string, 0, 1000)

	if len(results) > 0 {
		allResults = append(allResults, append([]string{"profile"}, results[0].header...))
	}

	for _, res := range results {
		for _, row := range res.rows {
			if opts.ExcludeRoot {
				if strings.HasSuffix(row[arnIdx], ":root") {
					continue
				}
			}

			allResults = append(allResults, append([]string{res.profile}, row...))
		}
	}

	file, err := os.Create(opts.Output)

	if err != nil {
		log.Fatalf("unable to create CSV %s: %v", opts.Output, err)
	}

	defer file.Close()

	writer := csv.NewWriter(file)

	if err := writer.WriteAll(allResults); err != nil {
		log.Fatalf("error writing CSV: %v", err)
	}

	writer.Flush()
}
