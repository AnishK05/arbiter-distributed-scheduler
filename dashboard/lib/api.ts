export type NodeResources = {
  cpu_millicores: number;
  memory_mb: number;
};

export type Node = {
  id: string;
  hostname: string;
  address: string;
  status: string;
  epoch: number;
  capacity: NodeResources;
  allocated: NodeResources;
};

export type Job = {
  ID: string;
  Name: string;
  Image: string;
  Replicas: number;
  SchedulingPolicy: string;
  CPURequestMillicores: number;
  MemRequestMB: number;
};

export type Task = {
  ID: string;
  JobID: string;
  Status: string;
  AssignedNodeID?: string | null;
  ExitCode?: number | null;
  LastError?: string | null;
};

export type ClusterEvent = {
  ID: number;
  EntityType: string;
  EntityID: string;
  EventType: string;
  Message: string;
  CreatedAt: string;
};

function apiBase(): string {
  if (typeof window !== "undefined") {
    return process.env.NEXT_PUBLIC_API_BASE || "http://localhost:8080";
  }
  return process.env.NEXT_PUBLIC_API_BASE || "http://localhost:8080";
}

async function getJSON<T>(path: string): Promise<T> {
  const res = await fetch(`${apiBase()}${path}`, { cache: "no-store" });
  if (!res.ok) {
    throw new Error(`${path}: ${res.status}`);
  }
  return res.json() as Promise<T>;
}

export async function fetchNodes(): Promise<Node[]> {
  const data = await getJSON<{ nodes: Node[] }>("/api/v1/nodes");
  return data.nodes || [];
}

export async function fetchJobs(): Promise<Job[]> {
  const data = await getJSON<{ jobs: Job[] }>("/api/v1/jobs");
  return data.jobs || [];
}

export async function fetchTasks(jobId?: string): Promise<Task[]> {
  const q = jobId ? `?job_id=${encodeURIComponent(jobId)}` : "";
  const data = await getJSON<{ tasks: Task[] }>(`/api/v1/tasks${q}`);
  return data.tasks || [];
}

export async function fetchEvents(limit = 40): Promise<ClusterEvent[]> {
  const data = await getJSON<{ events: ClusterEvent[] }>(`/api/v1/events?limit=${limit}`);
  return data.events || [];
}

export async function submitJob(body: {
  name: string;
  image: string;
  command?: string[];
  cpu_millicores: number;
  memory_mb: number;
  replicas: number;
  scheduling_policy: string;
}): Promise<Job> {
  let base = apiBase();
  for (let attempt = 0; attempt < 4; attempt++) {
    const res = await fetch(`${base}/api/v1/jobs`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    const data = await res.json();
    if (res.ok) {
      return data as Job;
    }
    if (res.status === 409 && data.error === "NOT_LEADER" && data.leader_addr) {
      base = leaderHTTPBase(String(data.leader_addr));
      continue;
    }
    throw new Error(data.error || `submit failed: ${res.status}`);
  }
  throw new Error("submit failed: could not reach leader");
}

function leaderHTTPBase(grpcAdvertise: string): string {
  // host.docker.internal:7001 → http://localhost:8086
  const cleaned = grpcAdvertise.replace("host.docker.internal", "localhost");
  const [host, port] = cleaned.split(":");
  let httpPort = "8080";
  if (port === "7001") httpPort = "8086";
  if (port === "7002") httpPort = "8087";
  return `http://${host || "localhost"}:${httpPort}`;
}


export function eventsStreamURL(): string {
  return `${apiBase()}/api/v1/events/stream`;
}

export function shortId(id: string): string {
  return id?.length > 8 ? id.slice(0, 8) : id || "";
}
