use anyhow::Result;
use bollard::container::{ListContainersOptions, LogOutput, LogsOptions, StatsOptions};
use bollard::Docker;
use chrono::{DateTime, Utc};
use futures_util::StreamExt;
use log::{error, info};
use regex::Regex;
use std::collections::HashMap;
use std::env;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Mutex;

#[derive(Debug, Clone)]
struct ContainerInfo {
    name: String,
    #[allow(dead_code)]
    state: String,
    #[allow(dead_code)]
    status: String,
}

#[derive(Debug, Clone)]
struct BaseMetrics {
    id: String,
    cpu_total: f64,
    cpu_user: f64,
    cpu_kernel: f64,
    mem_total_bytes: u64,
    mem_usage_bytes: u64,
    net_receive_bytes: u64,
    net_receive_packets: u64,
    net_transmit_bytes: u64,
    net_transmit_packets: u64,
    io_read_bytes: u64,
    io_write_bytes: u64,
    pids: u64,
}

#[derive(Debug, Clone)]
struct LogMetrics {
    stdout: i64,
    stderr: i64,
    std_all: i64,
    stderr_custom: i64,
    stdout_custom: i64,
    std_custom: i64,
}

#[derive(Debug)]
struct LogMetric {
    id: String,
    stdout: bool,
    stderr: bool,
    value: i64,
    custom_value: i64,
}

#[derive(Debug)]
struct InspectMetric {
    id: String,
    started_date: f64,
}

#[derive(Debug, Clone)]
struct Metrics {
    containers_up: i32,
    containers_down: i32,
    ids: Vec<String>,
    info: HashMap<String, ContainerInfo>,
    base_metrics: HashMap<String, BaseMetrics>,
    get_log_metrics: bool,
    get_log_custom_metrics: bool,
    log_regex: Option<Regex>,
    log_metrics: HashMap<String, LogMetrics>,
    inspect_metrics: HashMap<String, f64>,
}

impl Metrics {
    async fn get_containers(&mut self, docker: &Docker, all: bool) -> Result<()> {
        let options = Some(ListContainersOptions::<String> {
            all,
            ..Default::default()
        });

        let containers = docker.list_containers(options).await?;

        self.containers_up = 0;
        self.containers_down = 0;
        self.info.clear();
        self.ids.clear();

        for container in containers {
            let state = container.state.unwrap_or_default();
            let id = container.id.unwrap_or_default();

            if state == "running" {
                self.containers_up += 1;
            } else {
                self.containers_down += 1;
                continue;
            }

            let names = container.names.unwrap_or_default();
            let name = names
                .first()
                .map(|n| n.trim_start_matches('/').to_string())
                .unwrap_or_default();

            let status = container.status.unwrap_or_default();

            self.info.insert(
                id.clone(),
                ContainerInfo { name, state, status },
            );
            self.ids.push(id);
        }

        Ok(())
    }

    async fn get_base_metrics(docker: &Docker, id: &str) -> Result<BaseMetrics> {
        let options = Some(StatsOptions {
            stream: false,
            one_shot: true,
        });

        let mut stats_stream = docker.stats(id, options);

        if let Some(stats_result) = stats_stream.next().await {
            let stats = stats_result?;

            let cpu_total = stats.cpu_stats.cpu_usage.total_usage as f64 / 1e9;
            let cpu_user = stats.cpu_stats.cpu_usage.usage_in_usermode as f64 / 1e9;
            let cpu_kernel = stats.cpu_stats.cpu_usage.usage_in_kernelmode as f64 / 1e9;

            let mem_total = stats.memory_stats.limit.unwrap_or(0);
            let mem_usage = stats.memory_stats.usage.unwrap_or(0);

            let mut net_receive_bytes = 0u64;
            let mut net_receive_packets = 0u64;
            let mut net_transmit_bytes = 0u64;
            let mut net_transmit_packets = 0u64;

            if let Some(networks) = &stats.networks {
                for (_, network) in networks {
                    net_receive_bytes += network.rx_bytes;
                    net_receive_packets += network.rx_packets;
                    net_transmit_bytes += network.tx_bytes;
                    net_transmit_packets += network.tx_packets;
                }
            }

            let mut io_read_bytes = 0u64;
            let mut io_write_bytes = 0u64;

            if let Some(blkio_stats) = &stats.blkio_stats.io_service_bytes_recursive {
                for entry in blkio_stats {
                    match entry.op.as_str() {
                        "read" => io_read_bytes += entry.value,
                        "write" => io_write_bytes += entry.value,
                        _ => {}
                    }
                }
            }

            let pids = stats.pids_stats.current.unwrap_or(0);

            Ok(BaseMetrics {
                id: id.to_string(),
                cpu_total,
                cpu_user,
                cpu_kernel,
                mem_total_bytes: mem_total,
                mem_usage_bytes: mem_usage,
                net_receive_bytes,
                net_receive_packets,
                net_transmit_bytes,
                net_transmit_packets,
                io_read_bytes,
                io_write_bytes,
                pids,
            })
        } else {
            Err(anyhow::anyhow!("No stats available for container {}", id))
        }
    }

    async fn get_logs_count(
        docker: &Docker,
        id: &str,
        stdout: bool,
        stderr: bool,
        log_regex: &Option<Regex>,
        get_custom: bool,
    ) -> Result<LogMetric> {
        let options = Some(LogsOptions {
            stdout,
            stderr,
            follow: false,
            timestamps: false,
            tail: "all",
            ..Default::default()
        });

        let mut logs_stream = docker.logs(id, options);
        let mut content = String::new();

        while let Some(log_result) = logs_stream.next().await {
            match log_result {
                Ok(log_output) => match log_output {
                    LogOutput::StdOut { message } => {
                        content.push_str(&String::from_utf8_lossy(&message));
                    }
                    LogOutput::StdErr { message } => {
                        content.push_str(&String::from_utf8_lossy(&message));
                    }
                    _ => {}
                },
                Err(e) => {
                    error!("Failed to read logs for container {}: {}", id, e);
                    break;
                }
            }
        }

        let lines: Vec<&str> = content.lines().collect();
        let count_logs = lines.len() as i64;

        let mut err_counter = 0i64;
        if get_custom {
            if let Some(regex) = log_regex {
                for line in &lines {
                    if regex.is_match(line) {
                        err_counter += 1;
                    }
                }
            }
        }

        Ok(LogMetric {
            id: id.to_string(),
            stdout,
            stderr,
            value: count_logs,
            custom_value: err_counter,
        })
    }

    async fn get_inspect(docker: &Docker, id: &str) -> Result<InspectMetric> {
        let inspect = docker.inspect_container(id, None).await?;

        let state = inspect.state.ok_or_else(|| anyhow::anyhow!("No state info"))?;
        let started_at = state.started_at.unwrap_or_default();
        let started_time: DateTime<Utc> = started_at.parse()?;
        let started_timestamp = started_time.timestamp() as f64;

        Ok(InspectMetric {
            id: id.to_string(),
            started_date: started_timestamp,
        })
    }

    fn prometheus_format(
        metric_name: &str,
        help_text: &str,
        type_data: &str,
        id: &str,
        container_name: &str,
        hostname: &str,
        value: impl std::fmt::Display,
    ) -> Vec<String> {
        vec![
            format!("# HELP {} {}", metric_name, help_text),
            format!("# TYPE {} {}", metric_name, type_data),
            format!(
                "{}{{containerId=\"{}\",containerName=\"{}\",hostname=\"{}\"}} {}",
                metric_name, id, container_name, hostname, value
            ),
        ]
    }

    fn prometheus_metrics(&self, id: &str, hostname: &str) -> Vec<String> {
        let mut data = Vec::new();

        if let Some(info) = self.info.get(id) {
            if let Some(base_metrics) = self.base_metrics.get(id) {
                data.extend(Self::prometheus_format(
                    "docker_cpu_usage_total",
                    "Total CPU usage (user and kernel) in seconds",
                    "counter", id, &info.name, hostname, base_metrics.cpu_total,
                ));
                data.extend(Self::prometheus_format(
                    "docker_cpu_usage_user",
                    "User CPU usage in seconds",
                    "counter", id, &info.name, hostname, base_metrics.cpu_user,
                ));
                data.extend(Self::prometheus_format(
                    "docker_cpu_usage_kernel",
                    "Kernel CPU usage in seconds",
                    "counter", id, &info.name, hostname, base_metrics.cpu_kernel,
                ));
                data.extend(Self::prometheus_format(
                    "docker_memory_total",
                    "Total memory size in bytes",
                    "gauge", id, &info.name, hostname, base_metrics.mem_total_bytes,
                ));
                data.extend(Self::prometheus_format(
                    "docker_memory_usage",
                    "Usage memory size in bytes",
                    "gauge", id, &info.name, hostname, base_metrics.mem_usage_bytes,
                ));
                data.extend(Self::prometheus_format(
                    "docker_network_received_bytes",
                    "Number of bytes received on the network",
                    "counter", id, &info.name, hostname, base_metrics.net_receive_bytes,
                ));
                data.extend(Self::prometheus_format(
                    "docker_network_received_packages",
                    "Number of packages received on the network",
                    "counter", id, &info.name, hostname, base_metrics.net_receive_packets,
                ));
                data.extend(Self::prometheus_format(
                    "docker_network_transmit_bytes",
                    "Number of bytes transmitted on the network",
                    "counter", id, &info.name, hostname, base_metrics.net_transmit_bytes,
                ));
                data.extend(Self::prometheus_format(
                    "docker_network_transmit_packages",
                    "Number of packages transmitted on the network",
                    "counter", id, &info.name, hostname, base_metrics.net_transmit_packets,
                ));
                data.extend(Self::prometheus_format(
                    "docker_io_read_bytes",
                    "Number of bytes read by the block device",
                    "counter", id, &info.name, hostname, base_metrics.io_read_bytes,
                ));
                data.extend(Self::prometheus_format(
                    "docker_io_write_bytes",
                    "Number of bytes write by the block device",
                    "counter", id, &info.name, hostname, base_metrics.io_write_bytes,
                ));
                data.extend(Self::prometheus_format(
                    "docker_process_pids_count",
                    "Number of running processes and threads",
                    "gauge", id, &info.name, hostname, base_metrics.pids,
                ));
            }

            if self.get_log_metrics {
                if let Some(log_metrics) = self.log_metrics.get(id) {
                    data.extend(Self::prometheus_format(
                        "docker_logs_stdout_count",
                        "Number of logs from stdout stream",
                        "counter", id, &info.name, hostname, log_metrics.stdout,
                    ));
                    data.extend(Self::prometheus_format(
                        "docker_logs_stderr_count",
                        "Number of logs from stderr stream",
                        "counter", id, &info.name, hostname, log_metrics.stderr,
                    ));
                    data.extend(Self::prometheus_format(
                        "docker_logs_all_count",
                        "Number of logs from all stream",
                        "counter", id, &info.name, hostname, log_metrics.std_all,
                    ));
                    if self.get_log_custom_metrics {
                        data.extend(Self::prometheus_format(
                            "docker_logs_custom_count",
                            "Number of logs containing custom regular expression from all streams (by default, containing the error level)",
                            "counter", id, &info.name, hostname, log_metrics.std_custom,
                        ));
                    }
                }
            }

            if let Some(started_date) = self.inspect_metrics.get(id) {
                data.extend(Self::prometheus_format(
                    "docker_started_time",
                    "Container started time",
                    "gauge", id, &info.name, hostname, started_date,
                ));
            }
        }

        data.push(String::new());
        data
    }

    async fn get_metrics(&mut self, docker: &Docker, hostname: &str) -> Result<Vec<String>> {
        self.get_containers(docker, true).await?;

        let mut handles = Vec::new();
        for id in &self.ids {
            let docker = docker.clone();
            let id = id.clone();
            handles.push(tokio::spawn(async move {
                Self::get_base_metrics(&docker, &id).await
            }));
        }

        let mut base_metrics = HashMap::new();
        for handle in handles {
            match handle.await? {
                Ok(metrics) => { base_metrics.insert(metrics.id.clone(), metrics); }
                Err(e) => { error!("Failed to get base metrics: {}", e); }
            }
        }
        self.base_metrics = base_metrics;

        if self.get_log_metrics {
            let mut log_handles = Vec::new();

            for id in &self.ids {
                let docker_clone = docker.clone();
                let id_clone = id.clone();
                let log_regex = self.log_regex.clone();
                let get_custom = self.get_log_custom_metrics;
                log_handles.push(tokio::spawn(async move {
                    Self::get_logs_count(&docker_clone, &id_clone, true, false, &log_regex, get_custom).await
                }));

                let docker_clone = docker.clone();
                let id_clone = id.clone();
                let log_regex = self.log_regex.clone();
                let get_custom = self.get_log_custom_metrics;
                log_handles.push(tokio::spawn(async move {
                    Self::get_logs_count(&docker_clone, &id_clone, false, true, &log_regex, get_custom).await
                }));
            }

            let mut log_metrics = HashMap::new();
            for handle in log_handles {
                match handle.await? {
                    Ok(log_metric) => {
                        let entry = log_metrics.entry(log_metric.id.clone()).or_insert(LogMetrics {
                            stdout: 0, stderr: 0, std_all: 0,
                            stderr_custom: 0, stdout_custom: 0, std_custom: 0,
                        });
                        if log_metric.stdout {
                            entry.stdout = log_metric.value;
                            if self.get_log_custom_metrics { entry.stderr_custom = log_metric.custom_value; }
                        } else if log_metric.stderr {
                            entry.stderr = log_metric.value;
                            if self.get_log_custom_metrics { entry.stdout_custom = log_metric.custom_value; }
                        }
                    }
                    Err(e) => { error!("Failed to get log metrics: {}", e); }
                }
            }

            for (_, m) in &mut log_metrics {
                m.std_all = m.stdout + m.stderr;
                if self.get_log_custom_metrics { m.std_custom = m.stderr_custom + m.stdout_custom; }
            }
            self.log_metrics = log_metrics;
        }

        let mut inspect_handles = Vec::new();
        for id in &self.ids {
            let docker = docker.clone();
            let id = id.clone();
            inspect_handles.push(tokio::spawn(async move {
                Self::get_inspect(&docker, &id).await
            }));
        }

        let mut inspect_metrics = HashMap::new();
        for handle in inspect_handles {
            match handle.await? {
                Ok(im) => { inspect_metrics.insert(im.id.clone(), im.started_date); }
                Err(e) => { error!("Failed to get inspect metrics: {}", e); }
            }
        }
        self.inspect_metrics = inspect_metrics;

        let mut data = Vec::new();
        data.push("# HELP docker_containers_up_count Number of running containers".to_string());
        data.push("# TYPE docker_containers_up_count gauge".to_string());
        data.push(format!("docker_containers_up_count{{hostname=\"{}\"}} {}", hostname, self.containers_up));
        data.push("# HELP docker_containers_down_count Number of stopped containers".to_string());
        data.push("# TYPE docker_containers_down_count gauge".to_string());
        data.push(format!("docker_containers_down_count{{hostname=\"{}\"}} {}", hostname, self.containers_down));
        data.push(String::new());

        for id in &self.ids {
            data.extend(self.prometheus_metrics(id, hostname));
        }

        Ok(data)
    }
}

fn build_http_response(status: &str, content_type: &str, body: &str) -> String {
    format!(
        "HTTP/1.1 {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        status, content_type, body.len(), body
    )
}

async fn handle_client(
    mut stream: TcpStream,
    docker: Docker,
    hostname: String,
    metrics_state: Arc<Mutex<Metrics>>,
) -> Result<()> {
    let start = Instant::now();
    let mut buffer = [0u8; 4096];
    let bytes_read = stream.read(&mut buffer)?;
    let request = String::from_utf8_lossy(&buffer[..bytes_read]);

    let path = request.lines().next()
        .and_then(|line| {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() >= 2 && parts[0] == "GET" { Some(parts[1].to_string()) } else { None }
        })
        .unwrap_or_default();

    info!("GET request on {} from client", path);

    let response = if path == "/metrics" {
        let mut metrics = metrics_state.lock().await;
        match metrics.get_metrics(&docker, &hostname).await {
            Ok(metrics_data) => {
                let body = metrics_data.join("\n");
                build_http_response("200 OK", "text/plain; version=0.0.4", &body)
            }
            Err(e) => {
                error!("Failed to get metrics: {}", e);
                build_http_response("500 Internal Server Error", "text/plain", "Internal Server Error")
            }
        }
    } else {
        build_http_response("404 Not Found", "text/plain", "Not Found")
    };

    let _ = stream.write_all(response.as_bytes());
    let _ = stream.flush();

    let duration = start.elapsed();
    info!("Response time {:?} from client", duration);

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let get_log_metrics = env::var("DOCKER_LOG_METRICS")
        .map(|v| v.to_lowercase() == "true")
        .unwrap_or(false);

    let get_log_custom_metrics = env::var("DOCKER_LOG_CUSTOM_METRICS")
        .map(|v| v.to_lowercase() == "true")
        .unwrap_or(false);

    let log_regex = if get_log_custom_metrics {
        let query = env::var("DOCKER_LOG_CUSTOM_QUERY")
            .unwrap_or_else(|_| r#"(err|error|ERR|ERROR)"#.to_string());
        Some(Regex::new(&query)?)
    } else {
        None
    };

    let docker = Docker::connect_with_local_defaults()?;

    let info = docker.info().await?;
    let hostname = info.name.unwrap_or_else(|| "unknown".to_string());

    let metrics_state = Arc::new(Mutex::new(Metrics {
        containers_up: 0,
        containers_down: 0,
        ids: Vec::new(),
        info: HashMap::new(),
        base_metrics: HashMap::new(),
        get_log_metrics,
        get_log_custom_metrics,
        log_regex,
        log_metrics: HashMap::new(),
        inspect_metrics: HashMap::new(),
    }));

    let port = "9333";
    let addr = format!("0.0.0.0:{}", port);
    let listener = TcpListener::bind(&addr)?;

    info!("Exporter started on {} port.", port);

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let docker = docker.clone();
                let hostname = hostname.clone();
                let metrics_state = metrics_state.clone();

                tokio::spawn(async move {
                    if let Err(e) = handle_client(stream, docker, hostname, metrics_state).await {
                        error!("Error handling client: {}", e);
                    }
                });
            }
            Err(e) => { error!("Error accepting connection: {}", e); }
        }
    }

    Ok(())
}
