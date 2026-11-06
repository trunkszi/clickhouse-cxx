//! Isolated ClickHouse server with readable TLS mounts and RAII cleanup.
use std::{
    io::{Read, Write},
    net::{IpAddr, Ipv4Addr, SocketAddr, TcpStream},
    path::{Path, PathBuf},
    process::Command,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use rcgen::{BasicConstraints, CertificateParams, DnType, IsCa, KeyPair, SanType};
pub struct Ports {
    pub tcp: u16,
    pub http: u16,
    pub tcps: u16,
    pub https: u16,
}

pub struct Server {
    pub name: String,
    dir: PathBuf,
    pub ca_pem: String,
    pub ports: Ports,
    cleaned: bool,
}

impl Server {
    pub fn start() -> Result<Self, String> {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|err| err.to_string())?
            .as_nanos();
        let name = format!("chcxx06-{}-{nanos}", std::process::id());
        let dir = std::env::temp_dir().join(&name);
        let cert_dir = dir.join("certs");
        std::fs::create_dir_all(&cert_dir).map_err(|err| err.to_string())?;
        let issued = issue_certs()?;
        write_materials(&dir, &cert_dir, &issued)?;
        let mut server = Self {
            name,
            dir,
            ca_pem: issued.ca_pem,
            ports: Ports {
                tcp: 0,
                http: 0,
                tcps: 0,
                https: 0,
            },
            cleaned: false,
        };
        if let Err(err) = server.launch() {
            let report = server_report(&server.name);
            return Err(format!("{err}\n{report}"));
        }
        Ok(server)
    }

    fn launch(&mut self) -> Result<(), String> {
        let tls_xml = self.dir.join("tls.xml");
        let certs = self.dir.join("certs");
        let users = self.dir.join("users.xml");
        let output = Command::new("docker")
            .args([
                "run",
                "-d",
                "--name",
                &self.name,
                "-e",
                "CLICKHOUSE_SKIP_USER_SETUP=1",
                "-p",
                "0:9000",
                "-p",
                "0:8123",
                "-p",
                "0:9440",
                "-p",
                "0:8443",
                "-v",
                &format!(
                    "{}:/etc/clickhouse-server/config.d/tls.xml:ro",
                    tls_xml.display()
                ),
                "-v",
                &format!("{}:/etc/clickhouse-server/certs:ro", certs.display()),
                "-v",
                &format!(
                    "{}:/etc/clickhouse-server/users.d/allow.xml:ro",
                    users.display()
                ),
                "clickhouse/clickhouse-server:26.8.2.7",
            ])
            .output()
            .map_err(|err| err.to_string())?;
        if !output.status.success() {
            return Err(format!(
                "docker run failed: {}",
                String::from_utf8_lossy(&output.stderr)
            ));
        }
        self.ports = Ports {
            tcp: mapped_port(&self.name, 9000)?,
            http: mapped_port(&self.name, 8123)?,
            tcps: mapped_port(&self.name, 9440)?,
            https: mapped_port(&self.name, 8443)?,
        };
        wait_ready(&self.ports)
    }

    pub fn cleanup(&mut self) -> Result<(), String> {
        if self.cleaned {
            return Ok(());
        }
        remove_container(&self.name)?;
        match std::fs::remove_dir_all(&self.dir) {
            Ok(()) => {
                self.cleaned = true;
                Ok(())
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                self.cleaned = true;
                Ok(())
            }
            Err(err) => Err(format!("remove {} failed: {err}", self.dir.display())),
        }
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        if self.cleaned {
            return;
        }
        self.cleaned = true;
        match remove_container(&self.name) {
            Ok(()) => {}
            Err(err) => eprintln!("docker cleanup for {} failed: {err}", self.name),
        }
        match std::fs::remove_dir_all(&self.dir) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => eprintln!("remove {} failed: {err}", self.dir.display()),
        }
    }
}

pub struct Issued {
    pub ca_pem: String,
    cert_pem: String,
    key_pem: String,
}

pub fn issue_certs() -> Result<Issued, String> {
    let ca_key = KeyPair::generate().map_err(|err| err.to_string())?;
    let mut ca_params =
        CertificateParams::new(Vec::<String>::new()).map_err(|err| err.to_string())?;
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params
        .distinguished_name
        .push(DnType::CommonName, "clickhouse-live-ca");
    let ca_cert = ca_params
        .self_signed(&ca_key)
        .map_err(|err| err.to_string())?;
    let server_key = KeyPair::generate().map_err(|err| err.to_string())?;
    let mut server_params =
        CertificateParams::new(vec!["localhost".to_owned()]).map_err(|err| err.to_string())?;
    server_params
        .distinguished_name
        .push(DnType::CommonName, "localhost");
    server_params
        .subject_alt_names
        .push(SanType::IpAddress(IpAddr::V4(Ipv4Addr::LOCALHOST)));
    let server_cert = server_params
        .signed_by(&server_key, &ca_cert, &ca_key)
        .map_err(|err| err.to_string())?;
    Ok(Issued {
        ca_pem: ca_cert.pem(),
        cert_pem: server_cert.pem(),
        key_pem: server_key.serialize_pem(),
    })
}

fn write_materials(dir: &Path, cert_dir: &Path, issued: &Issued) -> Result<(), String> {
    // Published connections require IPv4 listening and a non-localhost user network.
    let tls_xml = r#"<clickhouse>
    <listen_host>0.0.0.0</listen_host>
    <tcp_port_secure>9440</tcp_port_secure>
    <https_port>8443</https_port>
    <openSSL>
        <server>
            <certificateFile>/etc/clickhouse-server/certs/server.crt</certificateFile>
            <privateKeyFile>/etc/clickhouse-server/certs/server.key</privateKeyFile>
            <verificationMode>none</verificationMode>
            <loadDefaultCAFile>false</loadDefaultCAFile>
            <cacheSessions>false</cacheSessions>
            <disableProtocols>sslv2,sslv3</disableProtocols>
            <preferServerCiphers>true</preferServerCiphers>
        </server>
    </openSSL>
</clickhouse>
"#;
    let users_xml = r#"<clickhouse>
    <users>
        <default>
            <networks replace="replace">
                <ip>0.0.0.0/0</ip>
                <ip>::/0</ip>
            </networks>
        </default>
    </users>
</clickhouse>
"#;
    std::fs::write(dir.join("tls.xml"), tls_xml).map_err(|err| err.to_string())?;
    std::fs::write(dir.join("users.xml"), users_xml).map_err(|err| err.to_string())?;
    std::fs::write(cert_dir.join("server.crt"), &issued.cert_pem).map_err(|err| err.to_string())?;
    let key_path = cert_dir.join("server.key");
    std::fs::write(&key_path, &issued.key_pem).map_err(|err| err.to_string())?;
    set_mode(&cert_dir.join("server.crt"), 0o644)?;
    set_mode(&key_path, 0o644)?;
    set_mode(&dir.join("tls.xml"), 0o644)?;
    set_mode(&dir.join("users.xml"), 0o644)?;
    set_mode(dir, 0o755)?;
    set_mode(cert_dir, 0o755)?;
    Ok(())
}

fn set_mode(path: &Path, mode: u32) -> Result<(), String> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))
        .map_err(|err| format!("chmod {} failed: {err}", path.display()))
}

fn mapped_port(name: &str, container_port: u16) -> Result<u16, String> {
    let output = Command::new("docker")
        .args(["port", name, &container_port.to_string()])
        .output()
        .map_err(|err| err.to_string())?;
    if !output.status.success() {
        return Err(format!(
            "docker port {container_port} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    let text = String::from_utf8_lossy(&output.stdout);
    let line = text
        .lines()
        .next()
        .ok_or_else(|| format!("docker port {container_port} returned no mapping"))?;
    let port = line
        .rsplit(':')
        .next()
        .ok_or_else(|| format!("docker port line has no host port: {line}"))?;
    port.trim().parse::<u16>().map_err(|err| err.to_string())
}

fn wait_ready(ports: &Ports) -> Result<(), String> {
    let deadline = Instant::now() + Duration::from_secs(120);
    let mut last = String::from("not checked");
    while Instant::now() < deadline {
        let tcp_ok = tcp_open(ports.tcp) && tcp_open(ports.tcps) && tcp_open(ports.https);
        let http_ok = http_ping(ports.http);
        if tcp_ok && http_ok {
            return Ok(());
        }
        last = format!("tcp={tcp_ok} http={http_ok}");
        std::thread::sleep(Duration::from_millis(500));
    }
    Err(format!("ClickHouse did not become ready: {last}"))
}

fn tcp_open(port: u16) -> bool {
    let address = SocketAddr::from(([127, 0, 0, 1], port));
    TcpStream::connect_timeout(&address, Duration::from_millis(400)).is_ok()
}

fn http_ping(port: u16) -> bool {
    let address = SocketAddr::from(([127, 0, 0, 1], port));
    let Ok(mut stream) = TcpStream::connect_timeout(&address, Duration::from_millis(400)) else {
        return false;
    };
    if stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .is_err()
    {
        return false;
    }
    if stream
        .write_all(b"GET /ping HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n")
        .is_err()
    {
        return false;
    }
    let mut buf = Vec::new();
    let read = stream.read_to_end(&mut buf);
    if read.is_err() && buf.is_empty() {
        return false;
    }
    String::from_utf8_lossy(&buf).contains("Ok")
}

fn remove_container(name: &str) -> Result<(), String> {
    let output = Command::new("docker")
        .args(["rm", "-f", name])
        .output()
        .map_err(|err| err.to_string())?;
    if output.status.success() {
        return Ok(());
    }
    let stderr = String::from_utf8_lossy(&output.stderr);
    if stderr.contains("No such container") {
        return Ok(());
    }
    Err(format!("docker rm -f {name} failed: {stderr}"))
}

pub fn server_report(name: &str) -> String {
    let state = container_state(name);
    let exited = state.starts_with("exited") || state.starts_with("dead");
    let state_line = if exited {
        format!("{state} (container has exited)")
    } else {
        state
    };
    format!(
        "container state: {state_line}\n--- docker logs ---\n{}\n--- clickhouse-server.err.log \
         ---\n{}",
        docker_logs(name),
        server_err_log(name, exited)
    )
}

fn container_state(name: &str) -> String {
    match Command::new("docker")
        .args([
            "inspect",
            "-f",
            "{{.State.Status}} exit={{.State.ExitCode}}",
            name,
        ])
        .output()
    {
        Ok(output) if output.status.success() => {
            String::from_utf8_lossy(&output.stdout).trim().to_owned()
        }
        Ok(output) => format!(
            "inspect failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ),
        Err(err) => format!("inspect failed: {err}"),
    }
}

fn server_err_log(name: &str, exited: bool) -> String {
    match Command::new("docker")
        .args([
            "exec",
            name,
            "cat",
            "/var/log/clickhouse-server/clickhouse-server.err.log",
        ])
        .output()
    {
        Ok(output) if output.status.success() => {
            String::from_utf8_lossy(&output.stdout).into_owned()
        }
        Ok(output) => {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let note = if exited || stderr.contains("is not running") {
                "container has exited; docker exec failed"
            } else {
                "docker exec failed"
            };
            format!("{note}: {stderr}\n{}", copied_err_log(name))
        }
        Err(err) => format!("docker exec failed: {err}\n{}", copied_err_log(name)),
    }
}

fn copied_err_log(name: &str) -> String {
    let dest = std::env::temp_dir().join(format!("{name}-err.log"));
    let copied = Command::new("docker")
        .args([
            "cp",
            &format!("{name}:/var/log/clickhouse-server/clickhouse-server.err.log"),
        ])
        .arg(&dest)
        .output();
    let text = match copied {
        Ok(output) if output.status.success() => std::fs::read_to_string(&dest)
            .unwrap_or_else(|err| format!("read copied err log failed: {err}")),
        Ok(output) => format!(
            "docker cp failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ),
        Err(err) => format!("docker cp failed: {err}"),
    };
    if dest.exists() {
        match std::fs::remove_file(&dest) {
            Ok(()) => {}
            Err(err) => eprintln!("remove {} failed: {err}", dest.display()),
        }
    }
    text
}

fn docker_logs(name: &str) -> String {
    match Command::new("docker").args(["logs", name]).output() {
        Ok(output) => {
            let mut text = String::from_utf8_lossy(&output.stdout).into_owned();
            text.push('\n');
            text.push_str(&String::from_utf8_lossy(&output.stderr));
            text
        }
        Err(err) => format!("docker logs failed: {err}"),
    }
}
