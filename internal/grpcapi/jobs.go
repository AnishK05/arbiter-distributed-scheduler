package grpcapi

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	arbiterv1 "github.com/AnishK05/arbiter-distributed-scheduler/gen/arbiter/v1"
	"github.com/AnishK05/arbiter-distributed-scheduler/internal/store"
)

const (
	defaultReplicas   int32 = 1
	defaultRetryLimit int32 = 3
)

// SubmitJob implements arbiterv1.SchedulerAPIServer: validates the request,
// persists a job, and expands it into N pending task rows.
func (s *Server) SubmitJob(ctx context.Context, req *arbiterv1.SubmitJobRequest) (*arbiterv1.Job, error) {
	if err := s.requireLeader(); err != nil {
		return nil, err
	}
	if req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "name is required")
	}
	if req.GetImage() == "" {
		return nil, status.Error(codes.InvalidArgument, "image is required")
	}
	capacity := req.GetRequest()
	if capacity.GetCpuMillicores() <= 0 || capacity.GetMemoryMb() <= 0 {
		return nil, status.Error(codes.InvalidArgument, "request.cpu_millicores and request.memory_mb must both be positive")
	}

	replicas := req.GetReplicas()
	if replicas <= 0 {
		replicas = defaultReplicas
	}
	retryLimit := req.GetRetryLimit()
	if retryLimit <= 0 {
		// Proto3 has no presence bit for int32; treat 0 / unset as the default.
		retryLimit = defaultRetryLimit
	}
	policy := req.GetSchedulingPolicy()
	if policy == "" {
		policy = store.SchedulingPolicyBinPack
	}
	if policy != store.SchedulingPolicyBinPack && policy != store.SchedulingPolicySpread {
		return nil, status.Errorf(codes.InvalidArgument, "unsupported scheduling_policy %q", policy)
	}

	job, err := s.store.CreateJob(ctx, store.CreateJobParams{
		Name:                 req.GetName(),
		Image:                req.GetImage(),
		Command:              req.GetCommand(),
		CPURequestMillicores: capacity.GetCpuMillicores(),
		MemRequestMB:         capacity.GetMemoryMb(),
		Replicas:             replicas,
		RetryLimit:           retryLimit,
		SchedulingPolicy:     policy,
		Constraints:          req.GetConstraints(),
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "submit job: %v", err)
	}
	return toProtoJob(job), nil
}

// GetJob implements arbiterv1.SchedulerAPIServer.
func (s *Server) GetJob(ctx context.Context, req *arbiterv1.GetJobRequest) (*arbiterv1.Job, error) {
	if req.GetJobId() == "" {
		return nil, status.Error(codes.InvalidArgument, "job_id is required")
	}
	job, err := s.store.GetJob(ctx, req.GetJobId())
	if err == store.ErrJobNotFound {
		return nil, status.Error(codes.NotFound, "job not found")
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get job: %v", err)
	}
	return toProtoJob(job), nil
}

// GetTask implements arbiterv1.SchedulerAPIServer.
func (s *Server) GetTask(ctx context.Context, req *arbiterv1.GetTaskRequest) (*arbiterv1.Task, error) {
	if req.GetTaskId() == "" {
		return nil, status.Error(codes.InvalidArgument, "task_id is required")
	}
	task, err := s.store.GetTask(ctx, req.GetTaskId())
	if err == store.ErrTaskNotFound {
		return nil, status.Error(codes.NotFound, "task not found")
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get task: %v", err)
	}
	return toProtoTask(task), nil
}

// ListJobs implements arbiterv1.SchedulerAPIServer.
func (s *Server) ListJobs(ctx context.Context, _ *arbiterv1.ListJobsRequest) (*arbiterv1.ListJobsResponse, error) {
	jobs, err := s.store.ListJobs(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list jobs: %v", err)
	}
	resp := &arbiterv1.ListJobsResponse{Jobs: make([]*arbiterv1.Job, 0, len(jobs))}
	for i := range jobs {
		resp.Jobs = append(resp.Jobs, toProtoJob(&jobs[i]))
	}
	return resp, nil
}

// ListTasks implements arbiterv1.SchedulerAPIServer.
func (s *Server) ListTasks(ctx context.Context, req *arbiterv1.ListTasksRequest) (*arbiterv1.ListTasksResponse, error) {
	tasks, err := s.store.ListTasks(ctx, req.GetJobId())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list tasks: %v", err)
	}
	resp := &arbiterv1.ListTasksResponse{Tasks: make([]*arbiterv1.Task, 0, len(tasks))}
	for i := range tasks {
		resp.Tasks = append(resp.Tasks, toProtoTask(&tasks[i]))
	}
	return resp, nil
}

func toProtoJob(j *store.Job) *arbiterv1.Job {
	return &arbiterv1.Job{
		Id:      j.ID,
		Name:    j.Name,
		Image:   j.Image,
		Command: j.Command,
		Request: &arbiterv1.NodeResources{
			CpuMillicores: j.CPURequestMillicores,
			MemoryMb:      j.MemRequestMB,
		},
		Replicas:         j.Replicas,
		RetryLimit:       j.RetryLimit,
		SchedulingPolicy: j.SchedulingPolicy,
		Constraints:      j.Constraints,
	}
}

func toProtoTask(t *store.Task) *arbiterv1.Task {
	out := &arbiterv1.Task{
		Id:          t.ID,
		JobId:       t.JobID,
		Status:      t.Status,
		RetriesUsed: t.RetriesUsed,
	}
	if t.AssignedNodeID != nil {
		out.AssignedNodeId = *t.AssignedNodeID
	}
	if t.ExitCode != nil {
		out.ExitCode = *t.ExitCode
	}
	if t.LastError != nil {
		out.LastError = *t.LastError
	}
	return out
}
