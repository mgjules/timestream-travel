package main

import (
	"log"
	"os"
	"time"

	"github.com/mgjules/timestream-travel/cmd"
	"github.com/urfave/cli/v2"
)

func main() {
	app := &cli.App{
		Name:     "TimestreamTravel",
		Version:  "v0.1.1",
		Compiled: time.Now(),
		Authors: []*cli.Author{
			{
				Name:  "Dylan Harbour",
				Email: "dylanh@ringier.co.za",
			},
			{
				Name:  "Michael Jules",
				Email: "michaelj@ringier.co.za",
			},
		},
		Copyright: "(c) 2021 Ringier SA",
		HelpName:  "timestream-travel",
		Usage:     "backup and restore time-series data from Amazon Timestream",
		Commands: []*cli.Command{
			cmd.Backup,
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}
