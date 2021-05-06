package cmd

import (
	"github.com/spf13/cobra"
	"nats_vs_kafka/utils"
)

var (
	middleware string
	pullput    string
	count      uint64
)

func init() {
	workCmd.Flags().StringVarP(&middleware, "middleware", "m", "nats", "middleware [nats,kafka]")
	workCmd.Flags().StringVarP(&pullput, "pullput", "t", "consume", "cosume or producer")
	workCmd.Flags().Uint64VarP(&count, "count", "c", 100000, "number of records")
}

var workCmd = &cobra.Command{
	Short: "work",
	Long:  "work",
	Use:   "work",
	Run: func(cmd *cobra.Command, args []string) {
		utils.Logger.Infoln("Begin work")
	},
}
