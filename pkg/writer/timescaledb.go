package writer

import (
	"bufio"
	"fmt"
	"github.com/prometheus/prometheus/pkg/labels"
	"os"
	"strings"
	"time"
	"path"
)

type TimescaleDBWriter struct {
}

func NewTimescaleDBWriter() (*TimescaleDBWriter, error) {
	return &TimescaleDBWriter{}, nil
}

func (w *TimescaleDBWriter) Write(labels *labels.Labels, timestamps []int64, values []float64) error {
	var metrics []string
	var filename string
	outputDir := "./output"

	if _, err := os.Stat(outputDir); os.IsNotExist(err) {
		os.Mkdir(outputDir, 0700)
	}

	for _, l := range *labels {
		metrics = append(metrics, l.Value)

		if (l.Name) == "__name__" {
			filename = l.Value + ".csv"
		}
	}

	if filename != "" {
		file, _ := os.Create(path.Join(outputDir, filename))
		defer file.Close()

		writer := bufio.NewWriter(file)
		for i, _ := range timestamps {
			ts := time.Unix(0, timestamps[i]*int64(time.Millisecond)).Format(time.RFC3339Nano)
			fmt.Fprintf(writer, "%s,%s,%g\n", ts, strings.Join(metrics, ","), values[i])
		}
		writer.Flush()
	}
	return nil
}
