package cmd

import (
	"io"
	"log"
	"time"

	"github.com/pilosa/pdk/usecase/retention"
	"github.com/spf13/cobra"
)

var RetentionMain *retention.Main

func NewRetentionCommand(stdin io.Reader, stdout, stderr io.Writer) *cobra.Command {
	RetentionMain = retention.NewMain()
	retentionCommand := &cobra.Command{
		Use:   "retention",
		Short: "retention - import retention data to pilosa",
		Long:  `TODO`,
		RunE: func(cmd *cobra.Command, args []string) error {
			start := time.Now()
			err := RetentionMain.Run()
			if err != nil {
				return err
			}
			log.Println("Done: ", time.Since(start))
			select {}
		},
	}
	flags := retentionCommand.Flags()
	flags.IntVarP(&RetentionMain.Concurrency, "concurrency", "c", 1, "Number of goroutines fetching and parsing")
	flags.IntVarP(&RetentionMain.BufferSize, "buffer-size", "b", 10000000, "Size of buffer for importers - heavily affects memory usage")
	flags.StringVarP(&RetentionMain.PilosaHost, "pilosa", "p", "localhost:10101", "Pilosa host")
	flags.StringVarP(&RetentionMain.Index, "index", "i", "retention", "Pilosa db to write to")
	flags.StringVarP(&RetentionMain.URLFile, "url-file", "f", "usecase/retention/urls.txt", "File to get raw data urls from. Urls may be http or local files.")
	flags.StringVarP(&Net.BindAddr, "bind-addr", "a", "localhost:10102", "Address which mapping proxy will bind to")

	return retentionCommand
}

func init() {
	subcommandFns["retention"] = NewRetentionCommand
}
