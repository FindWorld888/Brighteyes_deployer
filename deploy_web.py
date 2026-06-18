#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
明眸系统批量部署工具 - Web 界面版

功能:
  - 选择部署模式（明眸程序 / 明眸工具 / 全部）
  - 配置 MQTT URI
  - 扫描局域网主机并勾选目标
  - 批量远程部署，SSE 实时进度推送

用法:
  python3 deploy_web.py
  浏览器打开 http://localhost:9090
"""

import json
import os
import re
import shlex
import subprocess
import sys
import threading
import shutil
import ipaddress
import time
import queue
import concurrent.futures
from urllib.parse import urlparse
from pathlib import Path
from dataclasses import dataclass, field
from typing import List, Optional, Tuple

from flask import Flask, jsonify, request, Response, stream_with_context

# =============================================================================
# 常量
# =============================================================================
if getattr(sys, "frozen", False):
  # PyInstaller onefile: 使用可执行文件所在目录，便于读取同级资源
  SCRIPT_DIR = Path(sys.executable).resolve().parent
else:
  SCRIPT_DIR = Path(__file__).resolve().parent
INSTALL_DIR = Path("/usr/src/bevp6.3")
GLOB_CONFIG = INSTALL_DIR / "config" / "glob_config.json"
TOOLS_DIR = SCRIPT_DIR / "brighteyes_tools"
DEFAULT_REMOTE_DIR = "/tmp/bevp_deploy"

WEB_PORT = 9090

# =============================================================================
# Flask App
# =============================================================================
app = Flask(__name__)

# 部署状态 (SSE 事件队列)
_deploy_lock = threading.Lock()
_deploying = False
_sse_queues: List[queue.Queue] = []
_sse_lock = threading.Lock()


def _broadcast_sse(event: str, data: dict):
    """向所有 SSE 客户端广播事件"""
    msg = f"event: {event}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"
    with _sse_lock:
        dead = []
        for q in _sse_queues:
            try:
                q.put_nowait(msg)
            except queue.Full:
                dead.append(q)
        for q in dead:
            _sse_queues.remove(q)


# =============================================================================
# SSH 操作
# =============================================================================
class RemoteHost:
  def __init__(self, host: str, user: str = "visionnav", port: str = "22", password: str = ""):
    self.host = host
    self.user = user
    self.port = port
    self.password = password or ""
    self.last_error = ""
    self.ssh_opts = [
      "-o", "StrictHostKeyChecking=no",
      "-o", "ConnectTimeout=10",
      "-p", port,
    ]

  @property
  def target(self) -> str:
    return f"{self.user}@{self.host}"

  def _with_password(self, base_cmd: List[str]) -> Tuple[List[str], Optional[dict]]:
    """当填写了 SSH 密码时，使用 sshpass 注入密码。"""
    if not self.password:
      return base_cmd, None
    if not shutil.which("sshpass"):
      raise RuntimeError("已填写 SSH 密码，但本机未安装 sshpass")
    env = os.environ.copy()
    env["SSHPASS"] = self.password
    return ["sshpass", "-e"] + base_cmd, env

  def test_connection(self) -> bool:
    self.last_error = ""
    try:
      cmd, env = self._with_password(["ssh"] + self.ssh_opts + [self.target, "echo ok"])
      r = subprocess.run(
        cmd,
        capture_output=True, text=True, timeout=15, env=env
      )
      if r.returncode != 0:
        self.last_error = (r.stderr or r.stdout or "").strip()
      return r.returncode == 0
    except Exception as e:
      self.last_error = str(e) or "连接测试异常"
      return False

  def run(self, cmd: str, check: bool = True) -> subprocess.CompletedProcess:
    run_cmd, env = self._with_password(["ssh"] + self.ssh_opts + [self.target, cmd])
    r = subprocess.run(
      run_cmd,
      capture_output=True, text=True, timeout=300, env=env
    )
    if check and r.returncode != 0:
      raise RuntimeError(f"远程命令失败: {r.stderr.strip()}")
    return r

  def run_as_root(self, cmd: str, check: bool = True) -> subprocess.CompletedProcess:
    """以 root 权限执行远程命令。非 root 用户时优先使用已填写密码自动 sudo。"""
    if self.user == "root":
      return self.run(cmd, check=check)

    quoted_cmd = shlex.quote(cmd)
    if self.password:
      quoted_pwd = shlex.quote(self.password)
      sudo_cmd = f"printf '%s\\n' {quoted_pwd} | sudo -S -p '' bash -lc {quoted_cmd}"
      return self.run(sudo_cmd, check=check)

    # 未填写密码时尝试无密码 sudo（如 NOPASSWD 场景）
    return self.run(f"sudo -n bash -lc {quoted_cmd}", check=check)

  def upload(self, local_paths: List[str], remote_dir: str, recursive: bool = False):
    scp_opts = ["-P", self.port, "-o", "StrictHostKeyChecking=no"]
    if recursive:
      scp_opts.append("-r")
    upload_cmd, env = self._with_password(["scp"] + scp_opts + local_paths + [f"{self.target}:{remote_dir}/"])
    r = subprocess.run(upload_cmd, capture_output=True, text=True, timeout=600, env=env)
    if r.returncode != 0:
      raise RuntimeError(f"SCP 传输失败: {r.stderr.strip()}")


# =============================================================================
# 网络扫描
# =============================================================================
def get_all_local_subnets() -> List[str]:
    """获取本机所有网络接口的子网"""
    subnets = []
    try:
        r = subprocess.run(["ip", "-4", "route", "show", "scope", "link"],
                           capture_output=True, text=True)
        for line in r.stdout.splitlines():
            parts = line.split()
            if parts and "/" in parts[0]:
                subnets.append(parts[0])
    except Exception:
        pass
    return subnets


def get_local_subnet() -> Optional[str]:
    subnets = get_all_local_subnets()
    return subnets[0] if subnets else None


def get_local_ip() -> str:
    try:
        r = subprocess.run(["hostname", "-I"], capture_output=True, text=True)
        return r.stdout.split()[0]
    except Exception:
        return ""


def _pick_subnet_for_broker(broker_ip: str) -> Optional[str]:
    """
    根据 broker IP 选择扫描网段。
    使用 broker IP 的前两段作为 /16 网段进行广域扫描。
    例如: 10.20.24.63 -> 10.20.0.0/16
    """
    try:
        ipaddress.IPv4Address(broker_ip)
    except ValueError:
        subnets = get_all_local_subnets()
        return subnets[0] if subnets else None

    parts = broker_ip.split('.')
    return f"{parts[0]}.{parts[1]}.0.0/16"


def _normalize_mqtt_uri(raw: str) -> str:
    """规范化 MQTT 输入：支持 IP/主机名、host:port、mqtt://host:port。"""
    value = (raw or "").strip()
    if not value:
        return ""

    # 若输入为 mqtt://host:port 等 URI，提取主机部分，与现有配置格式保持一致。
    if "://" in value:
        parsed = urlparse(value)
        if parsed.hostname:
            return parsed.hostname

    # 输入 host:port 时仅保留 host。
    if ":" in value:
        host_part = value.rsplit(":", 1)[0].strip()
        if host_part:
            return host_part

    return value


def scan_lan_hosts(target_subnet: Optional[str] = None, progress_callback=None) -> List[str]:
    subnet = target_subnet or get_local_subnet()
    local_ip = get_local_ip()
    if not subnet:
        return []

    hosts = []

    if shutil.which("nmap"):
        if progress_callback:
            progress_callback(f"使用 nmap 扫描 {subnet} ...")
        try:
            # /16 扫描需要更长超时
            timeout = 300 if '/16' in subnet else 90
            r = subprocess.run(["nmap", "-sn", subnet],
                               capture_output=True, text=True, timeout=timeout)
            for m in re.finditer(r"Nmap scan report for.*?(\d+\.\d+\.\d+\.\d+)", r.stdout):
                ip = m.group(1)
                if ip != local_ip:
                    hosts.append(ip)
        except Exception:
            pass
    elif shutil.which("arp-scan"):
        if progress_callback:
            progress_callback("使用 arp-scan 扫描中...")
        try:
            r = subprocess.run(["arp-scan", "--localnet"],
                               capture_output=True, text=True, timeout=30)
            for m in re.finditer(r"^(\d+\.\d+\.\d+\.\d+)", r.stdout, re.MULTILINE):
                ip = m.group(1)
                if ip != local_ip:
                    hosts.append(ip)
        except Exception:
            pass
    else:
        if progress_callback:
            progress_callback(f"ping 扫描 {subnet} 中 (较慢)...")

        try:
            net = ipaddress.IPv4Network(subnet, strict=False)
        except ValueError:
            return []
        all_ips = [str(ip) for ip in net.hosts() if str(ip) != local_ip]

        def ping_host(ip):
            try:
                r = subprocess.run(["ping", "-c", "1", "-W", "1", ip],
                                   capture_output=True, timeout=3)
                return ip if r.returncode == 0 else None
            except Exception:
                return None

        max_workers = min(256, len(all_ips))
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(ping_host, ip): ip for ip in all_ips}
            for f in concurrent.futures.as_completed(futures):
                result = f.result()
                if result:
                    hosts.append(result)

    try:
        hosts.sort(key=lambda x: ipaddress.IPv4Address(x))
    except Exception:
        hosts.sort()
    return hosts


# =============================================================================
# 部署逻辑
# =============================================================================
def find_deb_file() -> Optional[Path]:
    for f in SCRIPT_DIR.iterdir():
        if f.suffix == ".deb":
            return f
    return None


def resolve_tool_upload_items() -> List[str]:
    """
    解析要上传的明眸工具文件。
    优先使用 brighteyes_tools 目录；若不存在，则回退到 deploy_web.py 同目录。
    """
    if TOOLS_DIR.is_dir():
        items = [str(p) for p in TOOLS_DIR.iterdir()]
        if not items:
            raise RuntimeError("brighteyes_tools 目录为空")
        return items

    required = [
        SCRIPT_DIR / "control_panel",
        SCRIPT_DIR / "control_panel_config.json",
        SCRIPT_DIR / "install_control_panel_service.sh",
    ]
    missing = [p.name for p in required if not p.exists()]
    if missing:
        raise RuntimeError(f"未找到 brighteyes_tools，且当前目录缺少工具文件: {', '.join(missing)}")
    return [str(p) for p in required]


def chmod_uploaded_items(remote: RemoteHost, remote_tmp: str):
    """为已上传的可执行程序、shell 脚本、deb 包赋予可执行权限。"""
    quoted_dir = shlex.quote(remote_tmp)
    remote.run_as_root(
        f"find {quoted_dir} -type f \\( -name '*.sh' -o -name '*.deb' -o -name 'control_panel' -o -name 'VP_SERVER' \\) "
        f"-exec chmod +x {{}} + 2>/dev/null || true",
        check=False,
    )


def make_remote_paths(remote_base_dir: str, host: str) -> Tuple[str, str]:
    """为当前主机生成本次部署专用目录，避免覆盖历史上传内容。"""
    remote_base_dir = (remote_base_dir or DEFAULT_REMOTE_DIR).strip() or DEFAULT_REMOTE_DIR
    safe_host = re.sub(r"[^0-9A-Za-z_.-]", "_", host)
    token = f"{int(time.time() * 1000)}_{safe_host}_{os.getpid()}"
    remote_tmp = f"{remote_base_dir.rstrip('/')}/_bevp_deploy_{token}"
    remote_backup = f"{remote_base_dir.rstrip('/')}/_bevp_config_backup_{token}"
    return remote_tmp, remote_backup


def cleanup_old_upload_dirs(remote: RemoteHost, remote_base_dir: str, keep_dirs: List[str]):
    """清理历史上传目录，保留 keep_dirs 中的目录。"""
    remote_base_dir = (remote_base_dir or DEFAULT_REMOTE_DIR).strip() or DEFAULT_REMOTE_DIR
    keep_checks = " || ".join([f'[ "$d" = {shlex.quote(k)} ]' for k in keep_dirs]) or "false"
    remote.run_as_root(f"""
mkdir -p {shlex.quote(remote_base_dir)}
for pattern in _bevp_deploy_ _bevp_config_backup_; do
  for d in {shlex.quote(remote_base_dir.rstrip('/'))}/$pattern*; do
    [ -d "$d" ] || continue
    if {keep_checks}; then
      continue
    fi
    rm -rf "$d"
  done
done
""", check=False)


def deploy_program_remote(remote: RemoteHost, host: str, mqtt_uri: str, remote_tmp: str, remote_backup: str):
    config_dir = f"{INSTALL_DIR}/config"

    deb_file = find_deb_file()
    if not deb_file:
        raise RuntimeError("未找到 .deb 安装包")

    # ---- 备份远程配置 ----
    _broadcast_sse("step", {"host": host, "msg": "备份远程配置文件...", "icon": "▶"})
    backup_result = remote.run_as_root(f"""
mkdir -p {shlex.quote(remote_backup)}
BACKED_UP=0
if [ -d {shlex.quote(config_dir)} ]; then
  for f in camera_config.json linestorage_config.json storage_config.json controlled_area.json; do
    if [ -f {shlex.quote(config_dir)}/$f ]; then
      cp -f {shlex.quote(config_dir)}/$f {shlex.quote(remote_backup)}/ && BACKED_UP=$((BACKED_UP + 1))
    fi
  done
  if [ -f {shlex.quote(str(GLOB_CONFIG))} ]; then
    NODE_ID=$(grep -o '"node_id"[[:space:]]*:[[:space:]]*"[^"]*"' {shlex.quote(str(GLOB_CONFIG))} | head -1 | grep -o '"[^"]*"$' | tr -d '"')
    if [ -n "$NODE_ID" ]; then
      printf '%s' "$NODE_ID" > {shlex.quote(remote_backup)}/node_id.txt
    fi
  fi
fi
echo "BACKED_UP=$BACKED_UP"
""", check=False)
    _broadcast_sse("step", {"host": host, "msg": f"配置备份完成 (备份了 {backup_result.stdout.strip().split('=')[-1] if backup_result.stdout else '0'} 个文件)", "icon": "✓"})

    # 确认是否有备份内容需要恢复（检查任一配置文件或 node_id.txt 存在）
    backup_check = remote.run_as_root(
        f"find {shlex.quote(remote_backup)} -type f | wc -l",
        check=False
    )
    has_backup = int((backup_check.stdout or "0").strip()) > 0

    # ---- 传输并安装 deb ----
    size_mb = deb_file.stat().st_size // 1024 // 1024
    _broadcast_sse("step", {"host": host, "msg": f"传输 deb 包 ({size_mb}MB)...", "icon": "▶"})
    remote.run_as_root(f"mkdir -p {shlex.quote(remote_tmp)} && chown {shlex.quote(remote.user)}:{shlex.quote(remote.user)} {shlex.quote(remote_tmp)}", check=False)
    remote.run(f"mkdir -p {shlex.quote(remote_tmp)}", check=False)
    remote.upload([str(deb_file)], remote_tmp)
    _broadcast_sse("step", {"host": host, "msg": "设置上传文件执行权限...", "icon": "▶"})
    chmod_uploaded_items(remote, remote_tmp)
    _broadcast_sse("step", {"host": host, "msg": "deb 包传输完成", "icon": "✓"})

    _broadcast_sse("step", {"host": host, "msg": "安装明眸程序...", "icon": "▶"})
    install_result = remote.run_as_root(
        f"dpkg -i {shlex.quote(remote_tmp)}/*.deb || apt-get install -f -y",
        check=False
    )
    if install_result.returncode != 0:
        err_msg = (install_result.stderr or install_result.stdout or "").strip()
        raise RuntimeError(f"deb 安装失败: {err_msg}")
    remote.run_as_root(
        f"chmod +x {shlex.quote(str(INSTALL_DIR))}/VP_SERVER 2>/dev/null || true; "
        f"chmod +x {shlex.quote(str(INSTALL_DIR))}/run.sh 2>/dev/null || true; "
        f"chmod -R a+rwX {shlex.quote(str(INSTALL_DIR))} 2>/dev/null || true",
        check=False,
    )
    _broadcast_sse("step", {"host": host, "msg": "明眸程序安装完成", "icon": "✓"})

    # ---- 恢复配置文件 ----
    if has_backup:
        _broadcast_sse("step", {"host": host, "msg": "恢复配置文件...", "icon": "▶"})
        restore_result = remote.run_as_root(f"""
RESTORED=0
if [ -d {shlex.quote(remote_backup)} ]; then
  for f in camera_config.json linestorage_config.json storage_config.json controlled_area.json; do
    if [ -f {shlex.quote(remote_backup)}/$f ]; then
      cp -f {shlex.quote(remote_backup)}/$f {shlex.quote(config_dir)}/ && RESTORED=$((RESTORED + 1))
    fi
  done
  if [ -f {shlex.quote(remote_backup)}/node_id.txt ] && [ -f {shlex.quote(str(GLOB_CONFIG))} ]; then
    NODE_ID=$(cat {shlex.quote(remote_backup)}/node_id.txt)
    if [ -n "$NODE_ID" ]; then
      sed -i "s/\\"node_id\\"[[:space:]]*:[[:space:]]*\\"[^\\"]*\\"/\\"node_id\\": \\"$NODE_ID\\"/" {shlex.quote(str(GLOB_CONFIG))}
      RESTORED=$((RESTORED + 1))
    fi
  fi
  rm -rf {shlex.quote(remote_backup)}
fi
echo "RESTORED=$RESTORED"
""", check=False)
        restored_count = (restore_result.stdout or "").strip()
        _broadcast_sse("step", {"host": host, "msg": f"配置文件恢复完成 (恢复了 {restored_count.split('=')[-1] if '=' in restored_count else '0'} 个文件)", "icon": "✓"})
    else:
        # 首次安装，无需恢复，清理空备份目录
        remote.run_as_root(f"rm -rf {shlex.quote(remote_backup)}", check=False)
        _broadcast_sse("step", {"host": host, "msg": "首次安装，使用默认配置", "icon": "✓"})

    # ---- 更新 MQTT URI ----
    _broadcast_sse("step", {"host": host, "msg": "更新 MQTT URI 配置...", "icon": "▶"})
    remote.run_as_root(f"""
if [ ! -f {shlex.quote(str(GLOB_CONFIG))} ]; then
  echo 'glob_config.json 不存在' >&2
  exit 1
fi
MQTT_URI={shlex.quote(mqtt_uri)}
if grep -q '"URI"' {shlex.quote(str(GLOB_CONFIG))}; then
  sed -i "s/\\"URI\\"[[:space:]]*:[[:space:]]*\\"[^\\"]*\\"/\\"URI\\": \\"$MQTT_URI\\"/" {shlex.quote(str(GLOB_CONFIG))}
else
  echo 'glob_config.json 中未找到 URI 字段' >&2
  exit 1
fi
""")
    _broadcast_sse("step", {"host": host, "msg": "MQTT URI 更新完成", "icon": "✓"})


def deploy_tool_remote(remote: RemoteHost, host: str, remote_tmp: str):
    upload_items = resolve_tool_upload_items()

    _broadcast_sse("step", {"host": host, "msg": "传输明眸工具文件...", "icon": "▶"})
    remote.run_as_root(f"mkdir -p {remote_tmp} && chown {remote.user}:{remote.user} {remote_tmp}", check=False)
    remote.run(f"mkdir -p {remote_tmp}", check=False)
    remote.upload(upload_items, remote_tmp, recursive=True)
    _broadcast_sse("step", {"host": host, "msg": "设置上传文件执行权限...", "icon": "▶"})
    chmod_uploaded_items(remote, remote_tmp)
    remote.run(f"test -f {remote_tmp}/install_control_panel_service.sh")
    _broadcast_sse("step", {"host": host, "msg": "工具文件传输完成", "icon": "✓"})

    _broadcast_sse("step", {"host": host, "msg": "卸载旧版明眸工具...", "icon": "▶"})
    remote.run_as_root(
        f"chmod +x {remote_tmp}/install_control_panel_service.sh && "
        f"cd {remote_tmp} && ./install_control_panel_service.sh uninstall 2>/dev/null || true",
        check=False
    )
    _broadcast_sse("step", {"host": host, "msg": "旧版卸载完成", "icon": "✓"})

    _broadcast_sse("step", {"host": host, "msg": "安装明眸工具...", "icon": "▶"})
    remote.run_as_root(f"cd {remote_tmp} && ./install_control_panel_service.sh install")
    _broadcast_sse("step", {"host": host, "msg": "明眸工具安装完成", "icon": "✓"})


def _deploy_single_host(host: str, idx: int, total: int, mode: str, mqtt_uri: str,
                        ssh_user: str, ssh_port: str, ssh_password: str,
                        remote_dir: str, cleanup_old_uploads: bool) -> bool:
    """部署单台主机，返回 True 表示成功"""
    _broadcast_sse("host_start", {"host": host, "index": idx, "total": total})
    try:
        remote = RemoteHost(host, ssh_user, ssh_port, ssh_password)
        _broadcast_sse("step", {"host": host, "msg": "连接测试...", "icon": "▶"})
        if not remote.test_connection():
            err = remote.last_error or "无法连接到主机"
            raise RuntimeError(err)
        _broadcast_sse("step", {"host": host, "msg": "连接成功", "icon": "✓"})

        remote_tmp, remote_backup = make_remote_paths(remote_dir, host)
        if cleanup_old_uploads:
            _broadcast_sse("step", {"host": host, "msg": "清理历史上传目录（保留本次）...", "icon": "▶"})
            cleanup_old_upload_dirs(remote, remote_dir, [remote_tmp, remote_backup])
            _broadcast_sse("step", {"host": host, "msg": "历史上传目录清理完成", "icon": "✓"})

        if mode in ("program", "all"):
            deploy_program_remote(remote, host, mqtt_uri, remote_tmp, remote_backup)
        if mode in ("tool", "all"):
            deploy_tool_remote(remote, host, remote_tmp)

        _broadcast_sse("step", {"host": host, "msg": f"本次上传目录保留: {remote_tmp}", "icon": "✓"})
        _broadcast_sse("host_done", {"host": host, "index": idx, "ok": True, "msg": ""})
        return True
    except Exception as e:
        _broadcast_sse("step", {"host": host, "msg": str(e), "icon": "✗"})
        _broadcast_sse("host_done", {"host": host, "index": idx, "ok": False, "msg": str(e)})
        return False


# 最大并发部署线程数
MAX_DEPLOY_WORKERS = 5


def _deploy_worker(hosts: List[str], mode: str, mqtt_uri: str, ssh_user: str, ssh_port: str,
                   ssh_password: str, remote_dir: str, cleanup_old_uploads: bool = True):
    """后台部署线程（多主机并发）"""
    global _deploying
    total = len(hosts)

    if ssh_password and not shutil.which("sshpass"):
        _broadcast_sse("deploy_done", {"success": 0, "fail": total, "total": total})
        _broadcast_sse("step", {
            "host": "系统",
            "msg": "已填写 SSH 密码，但本机未安装 sshpass，请先安装：sudo apt-get install -y sshpass",
            "icon": "✗"
        })
        with _deploy_lock:
            _deploying = False
        return

    _broadcast_sse("deploy_start", {"total": total})

    workers = min(MAX_DEPLOY_WORKERS, total)
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(
                _deploy_single_host, host, idx, total, mode, mqtt_uri,
                ssh_user, ssh_port, ssh_password, remote_dir, cleanup_old_uploads
            ): host
            for idx, host in enumerate(hosts)
        }
        success = sum(1 for f in concurrent.futures.as_completed(futures) if f.result())
    fail = total - success

    _broadcast_sse("deploy_done", {"success": success, "fail": fail, "total": total})
    with _deploy_lock:
        _deploying = False


# =============================================================================
# API 路由
# =============================================================================
@app.route('/api/scan', methods=['POST'])
def api_scan():
    """扫描局域网主机（根据 broker IP 确定扫描网段）"""
    data = request.get_json(force=True) if request.is_json else {}
    broker_ip = _normalize_mqtt_uri(data.get('mqtt_uri') or '')

    if broker_ip:
        subnet = _pick_subnet_for_broker(broker_ip)
    else:
        subnet = get_local_subnet()

    hosts = scan_lan_hosts(target_subnet=subnet)
    return jsonify({'ok': True, 'hosts': hosts, 'local_ip': get_local_ip(), 'scanned_subnet': subnet})


@app.route('/api/scan_subnet', methods=['POST'])
def api_scan_subnet():
    """扫描指定网段的主机"""
    data = request.get_json(force=True) if request.is_json else {}
    subnet = (data.get('subnet') or '').strip()
    if not subnet:
        return jsonify({'ok': False, 'msg': '请输入目标网段'}), 400

    # 支持用户输入简写格式:
    #   "10.20"     -> "10.20.0.0/16"  (2段 → /16)
    #   "10.20.20"  -> "10.20.20.0/24" (3段 → /24)
    #   "10.20.20.1" -> "10.20.20.0/24" (4段无掩码 → /24)
    if '/' not in subnet:
        parts = subnet.split('.')
        if len(parts) <= 2:
            subnet = '.'.join(parts + ['0'] * (4 - len(parts))) + '/16'
        elif len(parts) == 3:
            subnet = subnet + '.0/24'
        else:
            subnet = subnet.rsplit('.', 1)[0] + '.0/24'

    try:
        ipaddress.IPv4Network(subnet, strict=False)
    except ValueError:
        return jsonify({'ok': False, 'msg': f'无效网段格式: {subnet}'}), 400

    hosts = scan_lan_hosts(target_subnet=subnet)
    return jsonify({'ok': True, 'hosts': hosts, 'scanned_subnet': subnet})


@app.route('/api/deploy', methods=['POST'])
def api_deploy():
    """启动部署"""
    global _deploying
    with _deploy_lock:
        if _deploying:
            return jsonify({'ok': False, 'msg': '部署正在进行中'}), 409
        _deploying = True

    data = request.get_json(force=True)
    hosts = data.get('hosts', [])
    mode = data.get('mode', 'all')
    mqtt_uri = _normalize_mqtt_uri(data.get('mqtt_uri', '10.20.24.63'))
    ssh_user = data.get('ssh_user', 'visionnav')
    ssh_port = data.get('ssh_port', '22')
    ssh_password = data.get('ssh_password', '')
    remote_dir = (data.get('remote_dir') or DEFAULT_REMOTE_DIR).strip() or DEFAULT_REMOTE_DIR
    cleanup_old_uploads_raw = data.get('cleanup_old_uploads', True)
    if isinstance(cleanup_old_uploads_raw, str):
      cleanup_old_uploads = cleanup_old_uploads_raw.strip().lower() in ('1', 'true', 'yes', 'on')
    else:
      cleanup_old_uploads = bool(cleanup_old_uploads_raw)

    if not hosts:
        with _deploy_lock:
            _deploying = False
        return jsonify({'ok': False, 'msg': '未选择目标主机'}), 400

    if not mqtt_uri:
      with _deploy_lock:
        _deploying = False
      return jsonify({'ok': False, 'msg': 'MQTT 地址不能为空'}), 400

    t = threading.Thread(target=_deploy_worker,
                         args=(hosts, mode, mqtt_uri, ssh_user, ssh_port, ssh_password, remote_dir, cleanup_old_uploads),
                         daemon=True)
    t.start()
    return jsonify({'ok': True, 'msg': f'部署已启动，共 {len(hosts)} 台主机'})


@app.route('/api/status', methods=['GET'])
def api_status():
    """获取当前部署状态"""
    deb = find_deb_file()
    return jsonify({
        'deploying': _deploying,
        'deb_file': deb.name if deb else None,
        'tools_dir': TOOLS_DIR.is_dir(),
        'local_ip': get_local_ip(),
    })


@app.route('/api/events')
def api_events():
    """SSE 实时事件流"""
    q = queue.Queue(maxsize=200)
    with _sse_lock:
        _sse_queues.append(q)

    def generate():
        try:
            yield "event: connected\ndata: {}\n\n"
            while True:
                try:
                    msg = q.get(timeout=30)
                    yield msg
                except queue.Empty:
                    yield ": keepalive\n\n"
        except GeneratorExit:
            pass
        finally:
            with _sse_lock:
                if q in _sse_queues:
                    _sse_queues.remove(q)

    return Response(
        stream_with_context(generate()),
        mimetype='text/event-stream',
        headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no'}
    )


# =============================================================================
# 前端页面
# =============================================================================
@app.route('/')
def index():
    return HTML_PAGE


HTML_PAGE = '''<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>明眸系统批量部署工具</title>
<style>
* { margin: 0; padding: 0; box-sizing: border-box; }
body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", "PingFang SC", "Microsoft YaHei", sans-serif; background: #f0f2f5; color: #333; min-height: 100vh; }
.header { background: linear-gradient(135deg, #1890ff, #096dd9); color: #fff; padding: 20px 30px; box-shadow: 0 2px 8px rgba(0,0,0,.15); }
.header h1 { font-size: 22px; font-weight: 600; }
.header p { opacity: .8; margin-top: 4px; font-size: 13px; }
.container { max-width: 1200px; margin: 20px auto; padding: 0 20px; display: grid; grid-template-columns: 320px 1fr; gap: 20px; }
@media(max-width:900px){ .container { grid-template-columns: 1fr; } }
.card { background: #fff; border-radius: 8px; box-shadow: 0 1px 4px rgba(0,0,0,.08); padding: 20px; }
.card h3 { font-size: 15px; color: #333; margin-bottom: 12px; padding-bottom: 8px; border-bottom: 1px solid #f0f0f0; }
.form-group { margin-bottom: 14px; }
.form-group label { display: block; font-size: 13px; color: #666; margin-bottom: 4px; }
.form-group input, .form-group select { width: 100%; padding: 8px 10px; border: 1px solid #d9d9d9; border-radius: 4px; font-size: 13px; transition: border-color .2s; }
.form-group input:focus, .form-group select:focus { outline: none; border-color: #1890ff; box-shadow: 0 0 0 2px rgba(24,144,255,.1); }
.radio-group { display: flex; flex-direction: column; gap: 8px; }
.radio-group label { font-size: 13px; color: #333; cursor: pointer; display: flex; align-items: center; gap: 6px; }
.btn { display: inline-flex; align-items: center; justify-content: center; gap: 6px; padding: 9px 16px; border: none; border-radius: 4px; font-size: 13px; font-weight: 500; cursor: pointer; transition: all .2s; width: 100%; }
.btn-primary { background: #1890ff; color: #fff; }
.btn-primary:hover { background: #40a9ff; }
.btn-primary:disabled { background: #bbb; cursor: not-allowed; }
.btn-success { background: #52c41a; color: #fff; }
.btn-success:hover { background: #73d13d; }
.btn-danger { background: #ff4d4f; color: #fff; }
.btn-sm { padding: 5px 12px; font-size: 12px; width: auto; }
.btn-outline { background: #fff; color: #666; border: 1px solid #d9d9d9; }
.btn-outline:hover { color: #1890ff; border-color: #1890ff; }
.right-panel { display: flex; flex-direction: column; gap: 16px; }
.host-toolbar { display: flex; gap: 8px; margin-bottom: 10px; flex-wrap: wrap; }
.host-toolbar input { flex: 1; min-width: 120px; padding: 7px 10px; border: 1px solid #d9d9d9; border-radius: 4px; font-size: 13px; }
.host-table { width: 100%; border-collapse: collapse; font-size: 13px; }
.host-table th { background: #fafafa; padding: 10px 12px; text-align: left; border-bottom: 1px solid #eee; font-weight: 500; }
.host-table td { padding: 8px 12px; border-bottom: 1px solid #f5f5f5; }
.host-table tr:hover td { background: #f0f7ff; }
.host-table .cb { width: 30px; text-align: center; }
.status-tag { display: inline-block; padding: 2px 8px; border-radius: 10px; font-size: 11px; }
.status-ready { background: #f0f0f0; color: #666; }
.status-ok { background: #f6ffed; color: #52c41a; }
.status-fail { background: #fff2f0; color: #ff4d4f; }
.status-wait { background: #fffbe6; color: #faad14; }
.log-box { background: #1e1e1e; color: #d4d4d4; border-radius: 6px; padding: 14px; font-family: "JetBrains Mono", "Fira Code", monospace; font-size: 12px; line-height: 1.7; height: 300px; overflow-y: auto; white-space: pre-wrap; word-break: break-all; }
.log-box .ok { color: #52c41a; }
.log-box .err { color: #ff4d4f; }
.log-box .info { color: #1890ff; }
.log-box .dim { color: #888; }
.progress-bar { height: 6px; background: #f0f0f0; border-radius: 3px; overflow: hidden; margin-top: 10px; }
.progress-fill { height: 100%; background: linear-gradient(90deg, #1890ff, #52c41a); transition: width .3s; width: 0; }
.summary { margin-top: 10px; font-size: 13px; color: #666; }
.empty-state { text-align: center; padding: 40px 20px; color: #bbb; }
</style>
</head>
<body>
<div class="header">
  <h1>明眸系统批量部署工具</h1>
  <p id="statusLine">就绪</p>
</div>
<div class="container">
  <!-- 左侧配置 -->
  <div>
    <div class="card" style="margin-bottom:16px">
      <h3>部署模式</h3>
      <div class="radio-group">
        <label><input type="radio" name="mode" value="all" checked> 全部安装（明眸程序 + 明眸工具）</label>
        <label><input type="radio" name="mode" value="program"> 仅安装明眸程序</label>
        <label><input type="radio" name="mode" value="tool"> 仅安装明眸工具</label>
      </div>
    </div>
    <div class="card" style="margin-bottom:16px">
      <h3>MQTT 配置</h3>
      <div class="form-group">
        <label>Broker IP 地址</label>
        <input type="text" id="mqttUri" value="10.20.24.63" placeholder="MQTT Broker IP">
      </div>
    </div>
    <div class="card" style="margin-bottom:16px">
      <h3>SSH 配置</h3>
      <div style="display:flex;gap:10px;margin-bottom:10px">
        <div class="form-group" style="flex:1">
          <label>用户名</label>
          <input type="text" id="sshUser" value="visionnav">
        </div>
        <div class="form-group" style="width:80px">
          <label>端口</label>
          <input type="text" id="sshPort" value="22">
        </div>
      </div>
      <div class="form-group" style="margin-bottom:10px">
        <label>目标文件夹（不存在会自动创建）</label>
        <input type="text" id="remoteDir" value="/tmp/bevp_deploy" placeholder="如 /tmp/bevp_deploy">
      </div>
      <div class="form-group" style="margin-bottom:0">
        <label>密码（可选，未填则使用免密）</label>
        <div style="display:flex;gap:8px">
          <input type="password" id="sshPassword" placeholder="SSH 密码（可留空）" autocomplete="off" style="flex:1">
          <button type="button" class="btn btn-sm btn-outline" id="btnPwdToggle" onclick="togglePassword()">显示</button>
        </div>
      </div>
      <div class="form-group" style="margin-top:10px;margin-bottom:0">
        <label style="display:flex;align-items:center;gap:8px;cursor:pointer">
          <input type="checkbox" id="cleanupOldUploads" checked>
          <span>清理历史上传目录，但保留本次上传目录</span>
        </label>
      </div>
    </div>
    <div class="card">
      <h3>操作</h3>
      <button class="btn btn-success" id="btnScan" onclick="doScan()" style="margin-bottom:10px">🔍 扫描局域网主机</button>
      <button class="btn btn-primary" id="btnDeploy" onclick="doDeploy()">🚀 开始部署</button>
    </div>
  </div>
  <!-- 右侧 -->
  <div class="right-panel">
    <div class="card">
      <h3>目标主机</h3>
      <div class="host-toolbar">
        <input type="text" id="ipInput" placeholder="输入IP, 回车或点击添加" onkeydown="if(event.key==='Enter')addHost()">
        <button class="btn btn-sm btn-outline" onclick="addHost()">➕添加</button>
        <button class="btn btn-sm btn-outline" onclick="selectAll()">全选</button>
        <button class="btn btn-sm btn-outline" onclick="deselectAll()">取消</button>
        <button class="btn btn-sm btn-outline" onclick="removeSelected()">🗑删除</button>
      </div>
      <div class="host-toolbar" style="margin-top:6px">
        <input type="text" id="subnetInput" placeholder="如 10.20(/16) 或 10.20.20(/24) 或 172.16.0.0/16" onkeydown="if(event.key==='Enter')doSubnetScan()">
        <button class="btn btn-sm btn-outline" id="btnSubnet" onclick="doSubnetScan()">🌐查询网段</button>
      </div>
      <div class="host-toolbar" style="margin-top:6px">
        <input type="text" id="filterInput" placeholder="🔍 筛选IP（输入关键字过滤）" oninput="renderTable()">
        <button class="btn btn-sm btn-outline" onclick="document.getElementById('filterInput').value='';renderTable()">清除</button>
      </div>
      <div id="hostTableWrap">
        <table class="host-table">
          <thead><tr><th class="cb"><input type="checkbox" id="cbAll" onchange="toggleAll(this)"></th><th>IP 地址</th><th>状态</th></tr></thead>
          <tbody id="hostBody"></tbody>
        </table>
      </div>
      <div class="summary" id="hostSummary">共 0 台主机，已选 0 台</div>
    </div>
    <div class="card">
      <h3>部署日志</h3>
      <div class="progress-bar"><div class="progress-fill" id="progressFill"></div></div>
      <div class="summary" id="deploySummary"></div>
      <div class="log-box" id="logBox"></div>
    </div>
  </div>
</div>
<script>
let hosts = [];  // [{ip, status, checked}]
let deploying = false;
let evtSource = null;

function getMode() { return document.querySelector('input[name=mode]:checked').value; }

function togglePassword() {
  const pwd = document.getElementById('sshPassword');
  const btn = document.getElementById('btnPwdToggle');
  if (!pwd || !btn) return;
  if (pwd.type === 'password') {
    pwd.type = 'text';
    btn.textContent = '隐藏';
  } else {
    pwd.type = 'password';
    btn.textContent = '显示';
  }
}

function renderTable() {
  const tbody = document.getElementById('hostBody');
  const filter = (document.getElementById('filterInput')?.value || '').trim();
  const filtered = filter ? hosts.filter(h => h.ip.includes(filter)) : hosts;
  if (!filtered.length) {
    tbody.innerHTML = '<tr><td colspan="3" style="text-align:center;color:#bbb;padding:30px">' + (filter ? '无匹配的主机' : '暂无主机，请扫描或手动添加') + '</td></tr>';
  } else {
    tbody.innerHTML = filtered.map((h) => {
      const i = hosts.indexOf(h);
      return `<tr>
      <td class="cb"><input type="checkbox" ${h.checked?'checked':''} onchange="hosts[${i}].checked=this.checked;updateSummary()"></td>
      <td>${h.ip}</td>
      <td><span class="status-tag status-${h.status}">${{ready:'就绪',ok:'✓成功',fail:'✗失败',wait:'等待中...'}[h.status]||h.status}</span></td>
    </tr>`;
    }).join('');
  }
  updateSummary();
}

function updateSummary() {
  const sel = hosts.filter(h=>h.checked).length;
  document.getElementById('hostSummary').textContent = `共 ${hosts.length} 台主机，已选 ${sel} 台`;
}

function addHost() {
  const input = document.getElementById('ipInput');
  const text = input.value.trim();
  if (!text) return;
  const ips = text.split(/[,\\s]+/).filter(Boolean);
  ips.forEach(ip => {
    if (!hosts.find(h => h.ip === ip)) {
      hosts.push({ip, status:'ready', checked: true});
    }
  });
  input.value = '';
  renderTable();
}

function selectAll() {
  const filter = (document.getElementById('filterInput')?.value || '').trim();
  hosts.forEach(h => { if (!filter || h.ip.includes(filter)) h.checked=true; });
  renderTable();
}
function deselectAll() {
  const filter = (document.getElementById('filterInput')?.value || '').trim();
  hosts.forEach(h => { if (!filter || h.ip.includes(filter)) h.checked=false; });
  renderTable();
}
function toggleAll(el) {
  const filter = (document.getElementById('filterInput')?.value || '').trim();
  hosts.forEach(h => { if (!filter || h.ip.includes(filter)) h.checked=el.checked; });
  renderTable();
}
function removeSelected() { hosts = hosts.filter(h => !h.checked); renderTable(); }

async function doSubnetScan() {
  const input = document.getElementById('subnetInput');
  const subnet = input.value.trim();
  if (!subnet) { alert('请输入目标网段，如 10.20.20 或 192.168.1.0/24'); return; }
  const btn = document.getElementById('btnSubnet');
  btn.disabled = true; btn.textContent = '┗ 扫描中...';
  document.getElementById('statusLine').textContent = `正在扫描网段 ${subnet} ...`;
  try {
    const r = await fetch('/api/scan_subnet', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({subnet})});
    const d = await r.json();
    if (d.ok && d.hosts.length) {
      d.hosts.forEach(ip => {
        if (!hosts.find(h => h.ip === ip)) {
          hosts.push({ip, status:'ready', checked: false});
        }
      });
      renderTable();
      document.getElementById('statusLine').textContent = `网段 ${d.scanned_subnet} 扫描完成，发现 ${d.hosts.length} 台主机`;
    } else {
      document.getElementById('statusLine').textContent = d.msg || `网段 ${subnet} 未发现在线主机`;
    }
  } catch(e) {
    document.getElementById('statusLine').textContent = '扫描失败: ' + e.message;
  }
  btn.disabled = false; btn.textContent = '🌐查询网段';
}

async function doScan() {
  const btn = document.getElementById('btnScan');
  btn.disabled = true; btn.textContent = '⏳ 扫描中...';
  document.getElementById('statusLine').textContent = '正在扫描局域网...';
  try {
    const mqttUri = document.getElementById('mqttUri').value.trim();
    const r = await fetch('/api/scan', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({mqtt_uri: mqttUri})});
    const d = await r.json();
    if (d.ok && d.hosts.length) {
      d.hosts.forEach(ip => {
        if (!hosts.find(h => h.ip === ip)) {
          hosts.push({ip, status:'ready', checked: false});
        }
      });
      renderTable();
      document.getElementById('statusLine').textContent = `扫描完成，发现 ${d.hosts.length} 台在线主机`;
    } else {
      document.getElementById('statusLine').textContent = '扫描完成，未发现新主机';
    }
  } catch(e) {
    document.getElementById('statusLine').textContent = '扫描失败: ' + e.message;
  }
  btn.disabled = false; btn.textContent = '🔍 扫描局域网主机';
}

async function doDeploy() {
  const selected = hosts.filter(h => h.checked);
  if (!selected.length) { alert('请先选择至少一台目标主机'); return; }
  const mqttUri = document.getElementById('mqttUri').value.trim();
  const remoteDir = document.getElementById('remoteDir').value.trim();
  if (!mqttUri) { alert('请输入 MQTT Broker IP'); return; }
  if (!remoteDir) { alert('请输入目标文件夹'); return; }
  const modeMap = {all:'明眸程序+明眸工具', program:'仅明眸程序', tool:'仅明眸工具'};
  if (!confirm(`确认部署？\\n\\n模式: ${modeMap[getMode()]}\\nMQTT: ${mqttUri}\\n目录: ${remoteDir}\\n目标: ${selected.length} 台主机`)) return;

  // 重置状态
  selected.forEach(h => h.status = 'wait');
  renderTable();
  document.getElementById('logBox').innerHTML = '';
  document.getElementById('progressFill').style.width = '0%';
  document.getElementById('deploySummary').textContent = '';

  // 连接 SSE
  connectSSE();

  const body = {
    hosts: selected.map(h => h.ip),
    mode: getMode(),
    mqtt_uri: mqttUri,
    ssh_user: document.getElementById('sshUser').value.trim() || 'visionnav',
    ssh_port: document.getElementById('sshPort').value.trim() || '22',
    remote_dir: remoteDir,
    ssh_password: document.getElementById('sshPassword').value,
    cleanup_old_uploads: document.getElementById('cleanupOldUploads').checked,
  };
  try {
    const r = await fetch('/api/deploy', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(body)});
    const d = await r.json();
    if (!d.ok) { alert(d.msg); return; }
    deploying = true;
    document.getElementById('btnDeploy').disabled = true;
    document.getElementById('statusLine').textContent = '部署进行中...';
  } catch(e) { alert('请求失败: ' + e.message); }
}

function connectSSE() {
  if (evtSource) evtSource.close();
  evtSource = new EventSource('/api/events');
  evtSource.addEventListener('step', e => {
    const d = JSON.parse(e.data);
    const cls = d.icon==='✓'?'ok': d.icon==='✗'?'err':'dim';
    appendLog(`<span class="${cls}">[${d.host}] ${d.icon} ${d.msg}</span>`);
  });
  evtSource.addEventListener('host_start', e => {
    const d = JSON.parse(e.data);
    appendLog(`<span class="info">━━━ [${d.index+1}/${d.total}] 部署: ${d.host} ━━━</span>`);
  });
  evtSource.addEventListener('host_done', e => {
    const d = JSON.parse(e.data);
    const h = hosts.find(x => x.ip === d.host);
    if (h) { h.status = d.ok ? 'ok' : 'fail'; renderTable(); }
    // 更新进度
    const total = hosts.filter(x => x.status==='ok'||x.status==='fail').length;
    const all = hosts.filter(x => x.checked).length;
    document.getElementById('progressFill').style.width = (total/all*100)+'%';
  });
  evtSource.addEventListener('deploy_done', e => {
    const d = JSON.parse(e.data);
    deploying = false;
    document.getElementById('btnDeploy').disabled = false;
    document.getElementById('statusLine').textContent = `部署完成: 成功 ${d.success}/${d.total}  失败 ${d.fail}/${d.total}`;
    document.getElementById('deploySummary').textContent = `成功 ${d.success} 台 / 失败 ${d.fail} 台 / 共 ${d.total} 台`;
    appendLog(`<span class="info">═══ 部署完成: 成功${d.success} 失败${d.fail} ═══</span>`);
  });
}

function appendLog(html) {
  const box = document.getElementById('logBox');
  box.innerHTML += html + '\\n';
  box.scrollTop = box.scrollHeight;
}

// 初始化
renderTable();
</script>
</body>
</html>'''


# =============================================================================
# 入口
# =============================================================================
def _kill_previous_instance(port: int):
    """启动前关闭占用同一端口的旧进程"""
    my_pid = os.getpid()
    try:
        r = subprocess.run(
            ["lsof", "-t", "-i", f":{port}"],
            capture_output=True, text=True, timeout=5
        )
        if r.returncode == 0 and r.stdout.strip():
            for pid_str in r.stdout.strip().splitlines():
                pid = int(pid_str.strip())
                if pid != my_pid:
                    print(f"[自动清理] 终止占用端口 {port} 的旧进程 (PID={pid})")
                    os.kill(pid, 9)
                    time.sleep(0.3)
    except FileNotFoundError:
        # lsof 不可用时尝试用 fuser
        try:
            r = subprocess.run(
                ["fuser", f"{port}/tcp"],
                capture_output=True, text=True, timeout=5
            )
            if r.returncode == 0 and r.stdout.strip():
                for pid_str in r.stdout.strip().split():
                    pid = int(pid_str.strip())
                    if pid != my_pid:
                        print(f"[自动清理] 终止占用端口 {port} 的旧进程 (PID={pid})")
                        os.kill(pid, 9)
                        time.sleep(0.3)
        except Exception:
            pass
    except Exception:
        pass


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='明眸系统批量部署工具 - Web版')
    parser.add_argument('-p', '--port', type=int, default=WEB_PORT, help='监听端口 (默认 9090)')
    parser.add_argument('--host', default='0.0.0.0', help='监听地址 (默认 0.0.0.0)')
    args = parser.parse_args()

    _kill_previous_instance(args.port)

    local_ip = get_local_ip()
    print(f"╔══════════════════════════════════════════╗")
    print(f"║  明眸系统批量部署工具 - Web 界面         ║")
    print(f"╠══════════════════════════════════════════╣")
    print(f"║  本地访问: http://127.0.0.1:{args.port:<5}       ║")
    if local_ip:
        print(f"║  局域网:   http://{local_ip}:{args.port:<5}  ║")
    print(f"╚══════════════════════════════════════════╝")
    app.run(host=args.host, port=args.port, debug=False, threaded=True)
