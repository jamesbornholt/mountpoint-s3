#[derive(Debug)]
pub struct ResourceMetric {
    pub name: String,
    pub value: String,
}

#[cfg(target_os = "linux")]
mod linux {
    use std::time::Duration;

    use procfs::process::Process;
    use procfs::{ticks_per_second, Current, CurrentSI, KernelStats, Meminfo, WithCurrentSystemInfo};

    use super::*;

    /// A monitor for system- and process-level resource metrics that can emit to our metrics
    /// infrastructure.
    #[derive(Debug)]
    pub struct ResourceMetrics {
        system: SystemMetrics,
        process: ProcessMetrics,
    }

    impl ResourceMetrics {
        pub fn new() -> anyhow::Result<Self> {
            Ok(Self {
                system: SystemMetrics::new()?,
                process: ProcessMetrics::new()?,
            })
        }

        /// Update the resource metrics and return an iterator over metric key-value pairs
        pub fn update_and_fmt(&mut self) -> anyhow::Result<impl Iterator<Item = ResourceMetric>> {
            let (system, cpu_time, total_memory) = self.system.update()?;
            let process = self.process.update(cpu_time, total_memory)?;

            Ok(vec![
                system.cpu_time.as_metric("resource.system.cpu"),
                system.used_memory.as_metric("resource.system.memory.used"),
                system.cached_memory.as_metric("resource.system.memory.cached"),
                process.cpu_time.as_metric("resource.process.cpu"),
                process.memory_usage.as_metric("resource.process.memory.used"),
                process.virtual_memory_size.as_metric("resource.process.memory.virtual"),
            ]
            .into_iter())
        }
    }

    #[derive(Debug)]
    struct SystemMetrics {
        last_total_time: Duration,
        last_active_time: Duration,
    }

    impl SystemMetrics {
        fn new() -> anyhow::Result<Self> {
            let mut ret = Self {
                last_total_time: Duration::from_secs(0),
                last_active_time: Duration::from_secs(0),
            };
            ret.update()?;
            Ok(ret)
        }

        /// Returns a metric snapshot, as well as metrics needed by [ProcessMetrics]: the total CPU time
        /// since the last update, and the total memory in bytes
        fn update(&mut self) -> anyhow::Result<(SystemMetricsSnapshot, Duration, u64)> {
            // CPU usage math is borrowed from htop:
            // https://github.com/htop-dev/htop/blob/4abe9f4ce5102d029f65d26e73b03b00005a6096/linux/LinuxMachine.c#L448-L484
            let stat = KernelStats::current()?;
            let idle_time = stat
                .total
                .idle_duration()
                .saturating_add(stat.total.iowait_duration().unwrap_or_default());
            let active_time =
                // user time
                stat.total.user_duration()
                // nice time
                .saturating_add(stat.total.nice_duration())
                // system time
                .saturating_add(stat.total.system_duration())
                .saturating_add(stat.total.irq_duration().unwrap_or_default())
                .saturating_add(stat.total.softirq_duration().unwrap_or_default());
            let total_time = idle_time.saturating_add(active_time);

            let total_time_diff = total_time.saturating_sub(self.last_total_time);
            let active_time_diff = active_time.saturating_sub(self.last_active_time);
            self.last_total_time = total_time;
            self.last_active_time = active_time;

            let cpu_time = Percentage(active_time_diff.as_secs_f64() / total_time_diff.as_secs_f64());

            // Meminfo: https://access.redhat.com/solutions/406773
            let meminfo = Meminfo::current()?;
            let total_memory = meminfo.mem_total;
            let used_memory = meminfo.mem_total.saturating_sub(meminfo.mem_free);
            let cached_memory = meminfo
                .buffers
                .saturating_add(meminfo.cached)
                .saturating_add(meminfo.slab);
            let used_memory = used_memory.saturating_sub(cached_memory);

            let used_memory = Percentage(used_memory as f64 / total_memory as f64);
            let cached_memory = Percentage(cached_memory as f64 / total_memory as f64);

            let snapshot = SystemMetricsSnapshot {
                cpu_time,
                used_memory,
                cached_memory,
            };

            Ok((snapshot, total_time_diff, total_memory))
        }
    }

    #[derive(Debug)]
    struct SystemMetricsSnapshot {
        /// Total active CPU time (0-100%), including kernel time
        pub cpu_time: Percentage,
        /// Memory used, excluding kernel caches
        pub used_memory: Percentage,
        /// Memory used by kernel caches
        pub cached_memory: Percentage,
    }

    #[derive(Debug)]
    struct ProcessMetrics {
        last_user_time: Duration,
        last_system_time: Duration,
    }

    impl ProcessMetrics {
        fn new() -> anyhow::Result<Self> {
            let mut ret = Self {
                last_user_time: Duration::from_secs(0),
                last_system_time: Duration::from_secs(0),
            };
            // The actual values don't matter here since we won't use the snapshot, they just need to be
            // non-zero so we can get our fields initialized
            ret.update(Duration::from_secs(1), 1)?;
            Ok(ret)
        }

        fn update(&mut self, total_time_diff: Duration, total_memory: u64) -> anyhow::Result<ProcessMetricsSnapshot> {
            let process = Process::myself()?;
            let tps = ticks_per_second() as f64;

            let stat = process.stat()?;
            let user_time = Duration::from_secs_f64(stat.utime as f64 / tps);
            let system_time = Duration::from_secs_f64(stat.stime as f64 / tps);

            let user_time_diff = user_time.saturating_sub(self.last_user_time);
            let system_time_diff = system_time.saturating_sub(self.last_system_time);
            let active_time_diff = user_time_diff.saturating_add(system_time_diff);
            self.last_user_time = user_time;
            self.last_system_time = system_time;

            let cpu_time = Percentage(active_time_diff.as_secs_f64() / total_time_diff.as_secs_f64());

            let virtual_memory_size = Bytes(stat.vsize);
            let resident_set_size = stat.rss_bytes().get();

            let memory_usage = Percentage(resident_set_size as f64 / total_memory as f64);

            Ok(ProcessMetricsSnapshot {
                cpu_time,
                memory_usage,
                virtual_memory_size,
            })
        }
    }

    #[derive(Debug)]
    struct ProcessMetricsSnapshot {
        /// Total active CPU time (0-100%), including kernel time
        pub cpu_time: Percentage,
        /// Resident memory
        pub memory_usage: Percentage,
        /// Virtual memory size in bytes
        pub virtual_memory_size: Bytes,
    }

    #[derive(Debug, Copy, Clone)]
    struct Percentage(f64);

    impl Percentage {
        fn as_metric(&self, name: &str) -> ResourceMetric {
            ResourceMetric {
                name: name.to_owned(),
                value: format!("{:.1}%", self.0 * 100.0),
            }
        }
    }

    #[derive(Debug, Copy, Clone)]
    struct Bytes(u64);

    impl Bytes {
        fn as_metric(&self, name: &str) -> ResourceMetric {
            ResourceMetric {
                name: format!("{name}_mib"),
                value: format!("{}", self.0 / (1024 * 1024)),
            }
        }
    }
}

#[cfg(target_os = "linux")]
pub use linux::ResourceMetrics;

#[cfg(not(target_os = "linux"))]
mod other {
    use super::*;

    /// A monitor for system- and process-level resource metrics that can emit to our metrics
    /// infrastructure. On non-Linux OSes, this is a no-op
    #[derive(Debug)]
    pub struct ResourceMetrics;

    impl ResourceMetrics {
        pub fn new() -> anyhow::Result<Self> {
            Err(anyhow::anyhow!("resource metrics not implemented on this platform"))
        }

        /// Update the resource metrics and return an iterator over metric key-value pairs
        pub fn update_and_fmt(&mut self) -> anyhow::Result<impl Iterator<Item = ResourceMetric>> {
            Err::<std::iter::Empty<_>, _>(anyhow::anyhow!("resource metrics not implemented on this platform"))
        }
    }
}

#[cfg(not(target_os = "linux"))]
pub use other::ResourceMetrics;
