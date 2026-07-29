// Package metrics defines Arbiter's Prometheus collectors (Phase 7).
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Names match IMPLEMENTATION_PLAN.md Section 8 Phase 7.
const (
	Namespace = "arbiter"
)

// Registry holds the process-local collectors. Call MustRegister once at
// process start (scheduler or worker).
type Registry struct {
	reg      prometheus.Registerer
	gatherer prometheus.Gatherer

	TasksTotal           *prometheus.CounterVec
	TasksRunning         prometheus.Gauge
	SchedulingLatency    prometheus.Histogram
	NodesTotal           *prometheus.GaugeVec
	HeartbeatMissesTotal prometheus.Counter
	FailoverSeconds      prometheus.Histogram
	LeaderElectionsTotal prometheus.Counter
	QueueDepth           prometheus.Gauge
	IsLeader             prometheus.Gauge
	NodeCPUCapacity      *prometheus.GaugeVec
	NodeCPUAllocated     *prometheus.GaugeVec
	NodeMemCapacity      *prometheus.GaugeVec
	NodeMemAllocated     *prometheus.GaugeVec
	WorkerTasksRunning   prometheus.Gauge
}

// New builds collectors on a private registry (avoids global default clashes
// in tests) and registers Go/process collectors for free host signals.
func New() *Registry {
	reg := prometheus.NewRegistry()
	r := &Registry{
		reg:      reg,
		gatherer: reg,
		TasksTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "tasks_total",
			Help:      "Total tasks observed entering a status.",
		}, []string{"status"}),
		TasksRunning: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: Namespace,
			Name:      "tasks_running",
			Help:      "Number of tasks currently in running status (cluster view on leader).",
		}),
		SchedulingLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: Namespace,
			Name:      "scheduling_latency_seconds",
			Help:      "Time from task creation to successful schedule placement.",
			Buckets:   []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5},
		}),
		NodesTotal: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: Namespace,
			Name:      "nodes_total",
			Help:      "Number of nodes by status.",
		}, []string{"status"}),
		HeartbeatMissesTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "heartbeat_misses_total",
			Help:      "Times the failure detector downgraded a node for missed heartbeats.",
		}),
		FailoverSeconds: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: Namespace,
			Name:      "failover_seconds",
			Help:      "Observed last-seen age when a node is marked dead (detection latency).",
			Buckets:   []float64{0.25, 0.5, 0.75, 1, 1.25, 1.5, 2, 3, 5},
		}),
		LeaderElectionsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "leader_elections_total",
			Help:      "Times this scheduler replica became the elected leader.",
		}),
		QueueDepth: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: Namespace,
			Name:      "queue_depth",
			Help:      "Number of pending tasks waiting to be scheduled.",
		}),
		IsLeader: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: Namespace,
			Name:      "is_leader",
			Help:      "1 if this scheduler replica currently holds the leader lease, else 0.",
		}),
		NodeCPUCapacity: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: Namespace,
			Name:      "node_cpu_capacity_millicores",
			Help:      "Advertised CPU capacity per node.",
		}, []string{"node_id", "hostname"}),
		NodeCPUAllocated: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: Namespace,
			Name:      "node_cpu_allocated_millicores",
			Help:      "CPU allocated to scheduled/running tasks per node.",
		}, []string{"node_id", "hostname"}),
		NodeMemCapacity: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: Namespace,
			Name:      "node_memory_capacity_mb",
			Help:      "Advertised memory capacity per node in MB.",
		}, []string{"node_id", "hostname"}),
		NodeMemAllocated: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: Namespace,
			Name:      "node_memory_allocated_mb",
			Help:      "Memory allocated to scheduled/running tasks per node in MB.",
		}, []string{"node_id", "hostname"}),
		WorkerTasksRunning: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: Namespace,
			Name:      "worker_tasks_running",
			Help:      "Tasks currently executing on this worker process.",
		}),
	}

	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		r.TasksTotal,
		r.TasksRunning,
		r.SchedulingLatency,
		r.NodesTotal,
		r.HeartbeatMissesTotal,
		r.FailoverSeconds,
		r.LeaderElectionsTotal,
		r.QueueDepth,
		r.IsLeader,
		r.NodeCPUCapacity,
		r.NodeCPUAllocated,
		r.NodeMemCapacity,
		r.NodeMemAllocated,
		r.WorkerTasksRunning,
	)
	return r
}

// Handler serves Prometheus text exposition on /metrics.
func (r *Registry) Handler() http.Handler {
	return promhttp.HandlerFor(r.gatherer, promhttp.HandlerOpts{})
}

// ObserveTaskStatus increments tasks_total for the given status label.
func (r *Registry) ObserveTaskStatus(status string) {
	r.TasksTotal.WithLabelValues(status).Inc()
}
