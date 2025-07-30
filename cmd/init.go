package cmd

import (
	"os"

	"github.com/spf13/cobra"
	"port_scanning/cmd/port"
	"port_scanning/cmd/server"
)

var rootCmd = &cobra.Command{
	Use:          "app",
	Short:        "port scanning",
	SilenceUsage: true,
	Long:         `App for port scanning`,
}

func init() {
	rootCmd.AddCommand(port.StartCmd)
	rootCmd.AddCommand(server.StartCmd)
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(-1)
	}
}
