"use client";

import { FormEvent, useEffect, useMemo, useState } from "react";
import {
  ClusterEvent,
  Job,
  Node,
  Task,
  eventsStreamURL,
  fetchEvents,
  fetchJobs,
  fetchNodes,
  fetchTasks,
  shortId,
  submitJob,
} from "@/lib/api";

function Badge({ status }: { status: string }) {
  const cls = status.replace(/[^a-z_]/gi, "").toLowerCase();
  return <span className={`badge ${cls}`}>{status}</span>;
}

export default function HomePage() {
  const [nodes, setNodes] = useState<Node[]>([]);
  const [jobs, setJobs] = useState<Job[]>([]);
  const [tasks, setTasks] = useState<Task[]>([]);
  const [events, setEvents] = useState<ClusterEvent[]>([]);
  const [error, setError] = useState<string>("");
  const [submitMsg, setSubmitMsg] = useState<string>("");
  const [submitting, setSubmitting] = useState(false);

  const [name, setName] = useState("dashboard-job");
  const [image, setImage] = useState("arbiter-workload:latest");
  const [command, setCommand] = useState("8");
  const [replicas, setReplicas] = useState(3);
  const [cpu, setCpu] = useState(50);
  const [mem, setMem] = useState(32);
  const [policy, setPolicy] = useState("bin_pack");

  const refresh = async () => {
    try {
      const [n, j, t, e] = await Promise.all([
        fetchNodes(),
        fetchJobs(),
        fetchTasks(),
        fetchEvents(40),
      ]);
      setNodes(n);
      setJobs(j);
      setTasks(t);
      setEvents(e);
      setError("");
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    }
  };

  useEffect(() => {
    void refresh();
    const id = setInterval(() => void refresh(), 2000);
    return () => clearInterval(id);
  }, []);

  useEffect(() => {
    let es: EventSource | null = null;
    try {
      es = new EventSource(eventsStreamURL());
      es.onmessage = (msg) => {
        try {
          const ev = JSON.parse(msg.data) as ClusterEvent;
          setEvents((prev) => {
            if (prev.some((p) => p.ID === ev.ID)) return prev;
            return [...prev.slice(-80), ev];
          });
          // Light refresh when task/job events arrive so tables catch up quickly.
          if (ev.EntityType === "task" || ev.EntityType === "job" || ev.EntityType === "node") {
            void Promise.all([fetchTasks(), fetchJobs(), fetchNodes()]).then(([t, j, n]) => {
              setTasks(t);
              setJobs(j);
              setNodes(n);
            });
          }
        } catch {
          /* ignore malformed */
        }
      };
    } catch {
      /* EventSource unavailable */
    }
    return () => es?.close();
  }, []);

  const onSubmit = async (e: FormEvent) => {
    e.preventDefault();
    setSubmitting(true);
    setSubmitMsg("");
    try {
      const cmd = command.trim() ? [command.trim()] : [];
      const job = await submitJob({
        name,
        image,
        command: cmd,
        cpu_millicores: cpu,
        memory_mb: mem,
        replicas,
        scheduling_policy: policy,
      });
      setSubmitMsg(`submitted ${job.Name || name} (${shortId(job.ID)})`);
      await refresh();
    } catch (err) {
      setSubmitMsg(err instanceof Error ? err.message : String(err));
    } finally {
      setSubmitting(false);
    }
  };

  const readyNodes = useMemo(() => nodes.filter((n) => n.status === "ready"), [nodes]);

  return (
    <main className="shell">
      <div className="brand">
        <h1>Arbiter</h1>
        <span>cluster dashboard · live</span>
      </div>

      {error ? <p className="error">API: {error}</p> : null}

      <div className="grid grid-main">
        <section className="panel">
          <h2>Nodes</h2>
          {nodes.length === 0 ? (
            <p className="muted">No nodes registered yet.</p>
          ) : (
            nodes.map((n) => {
              const cpuPct = n.capacity.cpu_millicores
                ? n.allocated.cpu_millicores / n.capacity.cpu_millicores
                : 0;
              const memPct = n.capacity.memory_mb
                ? n.allocated.memory_mb / n.capacity.memory_mb
                : 0;
              return (
                <div key={n.id} style={{ marginBottom: "1rem" }}>
                  <div style={{ display: "flex", justifyContent: "space-between", gap: "1rem" }}>
                    <strong>{n.hostname}</strong>
                    <Badge status={n.status} />
                  </div>
                  <div className="muted" style={{ fontSize: "0.8rem", marginTop: "0.25rem" }}>
                    CPU {n.allocated.cpu_millicores}/{n.capacity.cpu_millicores}m · MEM{" "}
                    {n.allocated.memory_mb}/{n.capacity.memory_mb}MB · epoch {n.epoch}
                  </div>
                  <div className="util" title={`CPU ${(cpuPct * 100).toFixed(0)}%`}>
                    <span style={{ width: `${Math.min(100, cpuPct * 100)}%` }} />
                  </div>
                  <div className="util" title={`MEM ${(memPct * 100).toFixed(0)}%`}>
                    <span style={{ width: `${Math.min(100, memPct * 100)}%` }} />
                  </div>
                </div>
              );
            })
          )}
          <p className="muted" style={{ marginTop: "0.75rem", fontSize: "0.8rem" }}>
            {readyNodes.length} ready / {nodes.length} total
          </p>
        </section>

        <section className="panel">
          <h2>Submit job</h2>
          <form className="form" onSubmit={onSubmit}>
            <input value={name} onChange={(e) => setName(e.target.value)} placeholder="job name" required />
            <input value={image} onChange={(e) => setImage(e.target.value)} placeholder="image" required />
            <input
              value={command}
              onChange={(e) => setCommand(e.target.value)}
              placeholder="command arg (e.g. sleep seconds)"
            />
            <div className="form-row">
              <input
                type="number"
                min={1}
                value={replicas}
                onChange={(e) => setReplicas(Number(e.target.value))}
                placeholder="replicas"
              />
              <select value={policy} onChange={(e) => setPolicy(e.target.value)}>
                <option value="bin_pack">bin_pack</option>
                <option value="spread">spread</option>
              </select>
            </div>
            <div className="form-row">
              <input
                type="number"
                min={1}
                value={cpu}
                onChange={(e) => setCpu(Number(e.target.value))}
                placeholder="cpu millicores"
              />
              <input
                type="number"
                min={1}
                value={mem}
                onChange={(e) => setMem(Number(e.target.value))}
                placeholder="memory mb"
              />
            </div>
            <button type="submit" disabled={submitting}>
              {submitting ? "Submitting…" : "Submit"}
            </button>
            {submitMsg ? (
              <p className={submitMsg.startsWith("submitted") ? "ok" : "error"}>{submitMsg}</p>
            ) : null}
          </form>
        </section>
      </div>

      <div className="grid grid-main" style={{ marginTop: "1.25rem" }}>
        <section className="panel">
          <h2>Jobs</h2>
          <table className="table">
            <thead>
              <tr>
                <th>ID</th>
                <th>Name</th>
                <th>Replicas</th>
                <th>Policy</th>
              </tr>
            </thead>
            <tbody>
              {[...jobs].reverse().slice(0, 12).map((j) => (
                <tr key={j.ID}>
                  <td>{shortId(j.ID)}</td>
                  <td>{j.Name}</td>
                  <td>{j.Replicas}</td>
                  <td>{j.SchedulingPolicy}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </section>

        <section className="panel">
          <h2>Tasks</h2>
          <table className="table">
            <thead>
              <tr>
                <th>ID</th>
                <th>Job</th>
                <th>Status</th>
                <th>Node</th>
              </tr>
            </thead>
            <tbody>
              {[...tasks].reverse().slice(0, 20).map((t) => (
                <tr key={t.ID}>
                  <td>{shortId(t.ID)}</td>
                  <td>{shortId(t.JobID)}</td>
                  <td>
                    <Badge status={t.Status} />
                  </td>
                  <td>{shortId(t.AssignedNodeID || "")}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </section>
      </div>

      <section className="panel" style={{ marginTop: "1.25rem" }}>
        <h2>Live events</h2>
        <div className="events">
          {[...events].reverse().map((ev) => (
            <div className="event-line" key={ev.ID}>
              <strong>{ev.EventType}</strong> · {ev.EntityType}/{shortId(ev.EntityID)} · {ev.Message}
            </div>
          ))}
        </div>
      </section>
    </main>
  );
}
