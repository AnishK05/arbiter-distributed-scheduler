// Command arbiterctl is the Arbiter CLI ("kubectl for Arbiter"). Phase 0
// only scaffolds the command tree; `submit`, `get nodes/jobs/tasks`,
// `describe`, and `logs` are added in Phase 8 once the client-facing gRPC
// API has real implementations to talk to.
package main

import (
	"fmt"
	"os"

	"github.com/AnishK05/arbiter-distributed-scheduler/internal/buildinfo"

	"github.com/spf13/cobra"
)

func main() {
	if err := newRootCmd().Execute(); err != nil {
		os.Exit(1)
	}
}

func newRootCmd() *cobra.Command {
	var schedulerAddr string

	root := &cobra.Command{
		Use:           "arbiterctl",
		Short:         "arbiterctl is the command-line client for the Arbiter cluster scheduler",
		SilenceUsage:  true,
		SilenceErrors: false,
	}
	root.PersistentFlags().StringVar(&schedulerAddr, "scheduler-addr", "localhost:7000", "gRPC address of the Arbiter scheduler's SchedulerAPI service")

	root.AddCommand(newVersionCmd())

	return root
}

func newVersionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print the arbiterctl version",
		RunE: func(cmd *cobra.Command, args []string) error {
			_, err := fmt.Fprintf(cmd.OutOrStdout(), "arbiterctl %s (commit %s)\n", buildinfo.Version, buildinfo.Commit)
			return err
		},
	}
}
