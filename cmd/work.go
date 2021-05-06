package cmd

import (
	"fmt"
	"nats_vs_kafka/pkg/executor"
	"nats_vs_kafka/utils"
	"sync"

	"github.com/spf13/cobra"
)

var (
	middleware string
	pullput    string
	count      uint64
	goroutine  uint8
)

func init() {
	workCmd.Flags().StringVarP(&middleware, "middleware", "m", "nats", "middleware [nats,kafka]")
	workCmd.Flags().StringVarP(&pullput, "pullput", "t", "consume", "consume or producer")
	workCmd.Flags().Uint64VarP(&count, "count", "c", 1000000, "number of records")
	workCmd.Flags().Uint8VarP(&goroutine, "goroutine", "g", 1, "number of goroutines")
}

var workCmd = &cobra.Command{
	Short: "work",
	Long:  "work",
	Use:   "work",
	Run: func(cmd *cobra.Command, args []string) {
		parsingConfig()
		readConfig()

		utils.Logger.Infoln("Begin work")

		if middleware != "nats" && middleware != "kafka" {
			panic(fmt.Errorf("Wrong middleware type[%v]", middleware))
		}
		if pullput != "produce" && pullput != "consume" {
			panic(fmt.Errorf("Wrong pullput type[%v]", pullput))
		}

		wg := &sync.WaitGroup{}
		for i := uint8(0); i < goroutine; i++ {
			wg.Add(1)
			go func(id uint8) {
				if middleware == "nats" {
					if pullput == "produce" {
						executor.Test_Producer_Nats(id, utils.Addrs["nats"], count)
					} else {
						executor.Test_Consumer_Nats(id, utils.Addrs["nats"], count/2)
					}
				} else {
					if pullput == "produce" {
						executor.Test_Producer_Kafka(id, utils.Addrs["kafka"], count)
					} else {
						executor.Test_Consumer_Kafka(id, utils.Addrs["kafka"], count/2)
					}
				}
				wg.Done()
			}(i)
		}
		wg.Wait()

		utils.Logger.Infoln("Finish work")
	},
}
