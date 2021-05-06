package cmd

import (
	"nats_vs_kafka/utils"
	"os"

	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(workCmd)
}

var rootCmd = &cobra.Command{
	Short: "nats_go",
	Long:  "nats_go",
	Use:   "nats_go",
	Run: func(cmd *cobra.Command, args []string) {
		cmd.Help()
		os.Exit(-1)
	},
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		utils.Logger.Errorln(err)
		os.Exit(-2)
	}
}
