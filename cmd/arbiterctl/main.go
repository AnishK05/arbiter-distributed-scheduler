// Command arbiterctl is the Arbiter CLI ("kubectl for Arbiter"). Phase 3
// adds submit / get jobs / get tasks / get nodes so the Phase 3 DoD
// (submit a 5-replica job and watch it succeed) is exercisable without
// raw grpcurl. Richer commands (describe, logs) land in Phase 8.
package main

import (
	"context"
	"fmt"
	"os"
	"text/tabwriter"
	"time"

	arbiterv1 "github.com/AnishK05/arbiter-distributed-scheduler/gen/arbiter/v1"
	"github.com/AnishK05/arbiter-distributed-scheduler/internal/buildinfo"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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
	root.PersistentFlags().StringVar(&schedulerAddr, "scheduler-addr", envOr("ARBITER_SCHEDULER_ADDR", "localhost:7000"), "gRPC address of the Arbiter scheduler's SchedulerAPI service")

	root.AddCommand(newVersionCmd())
	root.AddCommand(newSubmitCmd(&schedulerAddr))
	root.AddCommand(newGetCmd(&schedulerAddr))

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

func newSubmitCmd(schedulerAddr *string) *cobra.Command {
	var (
		image    string
		command  []string
		replicas int32
		cpuMC    int64
		memMB    int64
		policy   string
		wait     bool
		waitFor  time.Duration
	)

	cmd := &cobra.Command{
		Use:   "submit <job-name>",
		Short: "Submit a job to the cluster",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := context.WithTimeout(cmd.Context(), 30*time.Second)
			defer cancel()

			client, conn, err := dialAPI(ctx, *schedulerAddr)
			if err != nil {
				return err
			}
			defer func() { _ = conn.Close() }()

			job, err := client.SubmitJob(ctx, &arbiterv1.SubmitJobRequest{
				Name:    args[0],
				Image:   image,
				Command: command,
				Request: &arbiterv1.NodeResources{
					CpuMillicores: cpuMC,
					MemoryMb:      memMB,
				},
				Replicas:         replicas,
				SchedulingPolicy: policy,
			})
			if err != nil {
				return fmt.Errorf("submit job: %w", err)
			}
			if _, err := fmt.Fprintf(cmd.OutOrStdout(), "submitted job %s (%s) replicas=%d image=%s\n",
				job.GetName(), job.GetId(), job.GetReplicas(), job.GetImage()); err != nil {
				return err
			}

			if !wait {
				return nil
			}
			return waitForJob(cmd, client, job.GetId(), waitFor)
		},
	}

	cmd.Flags().StringVar(&image, "image", "arbiter-workload:latest", "container image for each task")
	cmd.Flags().StringArrayVar(&command, "command", nil, "container command (repeatable); default is the image ENTRYPOINT")
	cmd.Flags().Int32Var(&replicas, "replicas", 1, "number of task replicas")
	cmd.Flags().Int64Var(&cpuMC, "cpu-millicores", 100, "CPU request per task, in millicores")
	cmd.Flags().Int64Var(&memMB, "memory-mb", 64, "memory request per task, in megabytes")
	cmd.Flags().StringVar(&policy, "scheduling-policy", "bin_pack", "scheduling policy (bin_pack|spread); Phase 3 uses first-fit regardless")
	cmd.Flags().BoolVar(&wait, "wait", false, "block until all tasks reach a terminal status")
	cmd.Flags().DurationVar(&waitFor, "wait-timeout", 2*time.Minute, "max time to wait when --wait is set")
	return cmd
}

func newGetCmd(schedulerAddr *string) *cobra.Command {
	get := &cobra.Command{
		Use:   "get",
		Short: "Display one or many resources",
	}
	get.AddCommand(newGetNodesCmd(schedulerAddr))
	get.AddCommand(newGetJobsCmd(schedulerAddr))
	get.AddCommand(newGetTasksCmd(schedulerAddr))
	return get
}

func newGetNodesCmd(schedulerAddr *string) *cobra.Command {
	return &cobra.Command{
		Use:   "nodes",
		Short: "List cluster nodes",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := context.WithTimeout(cmd.Context(), 10*time.Second)
			defer cancel()
			client, conn, err := dialAPI(ctx, *schedulerAddr)
			if err != nil {
				return err
			}
			defer func() { _ = conn.Close() }()

			resp, err := client.ListNodes(ctx, &arbiterv1.ListNodesRequest{})
			if err != nil {
				return err
			}
			w := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 4, 2, ' ', 0)
			if _, err := fmt.Fprintln(w, "ID\tHOSTNAME\tSTATUS\tEPOCH\tCPU(alloc/cap)\tMEM(alloc/cap)"); err != nil {
				return err
			}
			for _, n := range resp.GetNodes() {
				if _, err := fmt.Fprintf(w, "%s\t%s\t%s\t%d\t%d/%d\t%d/%d\n",
					shortID(n.GetId()), n.GetHostname(), n.GetStatus(), n.GetEpoch(),
					n.GetAllocated().GetCpuMillicores(), n.GetCapacity().GetCpuMillicores(),
					n.GetAllocated().GetMemoryMb(), n.GetCapacity().GetMemoryMb(),
				); err != nil {
					return err
				}
			}
			return w.Flush()
		},
	}
}

func newGetJobsCmd(schedulerAddr *string) *cobra.Command {
	return &cobra.Command{
		Use:   "jobs",
		Short: "List submitted jobs",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := context.WithTimeout(cmd.Context(), 10*time.Second)
			defer cancel()
			client, conn, err := dialAPI(ctx, *schedulerAddr)
			if err != nil {
				return err
			}
			defer func() { _ = conn.Close() }()

			resp, err := client.ListJobs(ctx, &arbiterv1.ListJobsRequest{})
			if err != nil {
				return err
			}
			w := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 4, 2, ' ', 0)
			if _, err := fmt.Fprintln(w, "ID\tNAME\tREPLICAS\tIMAGE\tCPU\tMEM\tPOLICY"); err != nil {
				return err
			}
			for _, j := range resp.GetJobs() {
				if _, err := fmt.Fprintf(w, "%s\t%s\t%d\t%s\t%d\t%d\t%s\n",
					shortID(j.GetId()), j.GetName(), j.GetReplicas(), j.GetImage(),
					j.GetRequest().GetCpuMillicores(), j.GetRequest().GetMemoryMb(),
					j.GetSchedulingPolicy(),
				); err != nil {
					return err
				}
			}
			return w.Flush()
		},
	}
}

func newGetTasksCmd(schedulerAddr *string) *cobra.Command {
	var jobID string
	cmd := &cobra.Command{
		Use:   "tasks",
		Short: "List tasks (optionally filtered by --job)",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := context.WithTimeout(cmd.Context(), 10*time.Second)
			defer cancel()
			client, conn, err := dialAPI(ctx, *schedulerAddr)
			if err != nil {
				return err
			}
			defer func() { _ = conn.Close() }()

			resp, err := client.ListTasks(ctx, &arbiterv1.ListTasksRequest{JobId: jobID})
			if err != nil {
				return err
			}
			w := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 4, 2, ' ', 0)
			if _, err := fmt.Fprintln(w, "ID\tJOB\tSTATUS\tNODE\tEXIT\tERROR"); err != nil {
				return err
			}
			for _, t := range resp.GetTasks() {
				if _, err := fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%v\t%s\n",
					shortID(t.GetId()), shortID(t.GetJobId()), t.GetStatus(),
					shortID(t.GetAssignedNodeId()), t.GetExitCode(), t.GetLastError(),
				); err != nil {
					return err
				}
			}
			return w.Flush()
		},
	}
	cmd.Flags().StringVar(&jobID, "job", "", "filter tasks to this job ID")
	return cmd
}

func waitForJob(cmd *cobra.Command, client arbiterv1.SchedulerAPIClient, jobID string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		ctx, cancel := context.WithTimeout(cmd.Context(), 5*time.Second)
		resp, err := client.ListTasks(ctx, &arbiterv1.ListTasksRequest{JobId: jobID})
		cancel()
		if err != nil {
			return err
		}
		tasks := resp.GetTasks()
		if len(tasks) == 0 {
			return fmt.Errorf("job %s has no tasks", jobID)
		}
		pending := 0
		failed := 0
		succeeded := 0
		for _, t := range tasks {
			switch t.GetStatus() {
			case "succeeded":
				succeeded++
			case "failed", "cancelled":
				failed++
			default:
				pending++
			}
		}
		if _, err := fmt.Fprintf(cmd.OutOrStdout(), "job %s: succeeded=%d failed=%d inflight=%d\n",
			shortID(jobID), succeeded, failed, pending); err != nil {
			return err
		}
		if pending == 0 {
			if failed > 0 {
				return fmt.Errorf("job finished with %d failed task(s)", failed)
			}
			if _, err := fmt.Fprintln(cmd.OutOrStdout(), "all tasks succeeded"); err != nil {
				return err
			}
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out waiting for job %s (%d still inflight)", jobID, pending)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func dialAPI(ctx context.Context, addr string) (arbiterv1.SchedulerAPIClient, *grpc.ClientConn, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, fmt.Errorf("dial scheduler at %s: %w", addr, err)
	}
	// NewClient is lazy; poke the connection so a down scheduler fails fast.
	arbiterv1.NewSchedulerAPIClient(conn)
	_ = ctx
	return arbiterv1.NewSchedulerAPIClient(conn), conn, nil
}

func shortID(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
