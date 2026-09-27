#!/usr/bin/env bash
# install_launchd.sh — 把 workbuddy2api 交给 macOS launchd 托管
# （开机自启 + 崩溃自动重启），并停掉手动启动的实例。
#
# 用法:
#   bash scripts/install_launchd.sh          # 安装/重装并启动
#   bash scripts/install_launchd.sh --uninstall   # 停止并卸载
#
# 安装后管理:
#   查看状态   launchctl print gui/$(id -u)/com.user.wb2api
#   停止/卸载  launchctl bootout gui/$(id -u)/com.user.wb2api
#   日志       tail -f data/wb2api.err.log（同 data/wb2api.out.log）
#
# 重要：plist 里 KeepAlive=true 表示「无条件常驻」——直接 kill 进程会被 launchd
# 立即拉起。要真正停服必须用下面的 bootout（或本脚本 --uninstall）。
#
# 与 Docker 的关系：docker-compose 绑同一端口(7863)与同一 ./data，两条路线只能选
# 一条。用本脚本前请确认 docker compose 未在跑（脚本会检查端口占用）。
set -euo pipefail
cd "$(dirname "$0")/.."   # 仓库根

REPO="$(pwd)"
LABEL="com.user.wb2api"
PLIST_SRC="$REPO/deploy/launchd/$LABEL.plist"
PLIST_DST="$HOME/Library/LaunchAgents/$LABEL.plist"
DOMAIN="gui/$(id -u)"
BIN="$REPO/wb2api"

say() { printf '\033[1;34m==>\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33mWARN:\033[0m %s\n' "$*" >&2; }
die() { printf '\033[1;31mERR:\033[0m %s\n' "$*" >&2; exit 1; }

uninstall() {
    say "卸载 $LABEL ..."
    launchctl bootout "$DOMAIN/$LABEL" 2>/dev/null && say "已停止并卸载" || warn "job 未在运行（可能已卸载）"
    if [ -f "$PLIST_DST" ]; then
        rm -f "$PLIST_DST"
        say "已删除 $PLIST_DST"
    fi
    say "完成。如需再手动启动：./wb2api -config config.json &"
}

if [ "${1:-}" = "--uninstall" ] || [ "${1:-}" = "-u" ]; then
    uninstall
    exit 0
fi

# ---- 前置检查 ----
[ -f "$PLIST_SRC" ] || die "模板不存在: $PLIST_SRC"
[ -f "$REPO/config.json" ] || die "缺少 config.json（cp config.example.json config.json 后至少设置 api_key）"
if [ ! -x "$BIN" ]; then
    die "二进制不存在: $BIN
  请先构建：cd $REPO && go build -o wb2api ./cmd/server"
fi

# 检查端口是否已被非 launchd 进程占用（典型：手动起的实例或 Docker 容器）。
if command -v lsof >/dev/null 2>&1; then
    if lsof -nP -iTCP:7863 -sTCP:LISTEN >/dev/null 2>&1; then
        holder=$(lsof -nP -iTCP:7863 -sTCP:LISTEN -Fc 2>/dev/null | sed -n 's/^c//p' | head -1)
        if [ "$holder" != "wb2api" ]; then
            die "端口 7863 被非 wb2api 进程占用（${holder}）。请先停掉它（可能是 Docker 容器：docker compose down）"
        fi
    fi
fi

# ---- 生成并安装 plist（在任何停服动作之前完成，且校验通过）----
# 顺序很关键：渲染/校验都可能在路径异常时失败。必须先把新 plist 备好并验证，
# 再去停旧服务——否则脚本中途失败会把服务置于「已停、没起」的空档。
# 注意：变量插值一律用 ${VAR} 花括号形式。macOS 自带 bash 3.2 在 `$VAR` 后紧跟
# 多字节中文时会把这些字节并入变量名（set -u 下直接报 "unbound variable"）。
say "由模板生成 plist（仓库路径: ${REPO}）"
mkdir -p "$HOME/Library/LaunchAgents" "$REPO/data"
# 用 | 作分隔符，避免路径含 / 时需要转义；& 与 \ 也一并转义以防路径含特殊字符。
escaped_repo=$(printf '%s' "${REPO}" | sed -e 's/[&\\]/\\&/g')
sed "s|__REPO__|${escaped_repo}|g" "$PLIST_SRC" > "$PLIST_DST"
say "已写入 ${PLIST_DST}"

# 校验生成的 plist 语法（存在 plutil 时）；失败直接退出，此时服务仍在原样运行。
if command -v plutil >/dev/null 2>&1; then
    plutil -lint "$PLIST_DST" >/dev/null || die "生成的 plist 语法非法（未改动正在运行的服务）"
fi

# ---- 停掉手动实例（否则会和 launchd 抢端口）----
# 从这里开始才动服务。装一个 trap：万一后续 bootstrap 失败，尝试把服务拉回，
# 避免「脚本报错 → 服务停着没人管」的空档。
restore_manual() {
    if ! curl -fsS -m 2 http://127.0.0.1:7863/healthz >/dev/null 2>&1; then
        warn "检测到服务未在运行，尝试拉起手动实例 ..."
        ( cd "$REPO" && nohup ./wb2api -config config.json >> data/wb2api.out.log 2>> data/wb2api.err.log & ) || true
    fi
}
trap 'restore_manual' EXIT

if pgrep -f 'wb2api -config' >/dev/null 2>&1; then
    say "停止手动运行的 wb2api 实例 ..."
    pkill -f 'wb2api -config' || true
    # 等端口释放（最多 5s）
    for _ in $(seq 1 50); do
        pgrep -f 'wb2api -config' >/dev/null 2>&1 || break
        sleep 0.1
    done
fi

# ---- 加载（bootout 旧的 → bootstrap 新的 → enable）----
say "重新加载 launchd job ..."
launchctl bootout "$DOMAIN/$LABEL" 2>/dev/null || true
launchctl bootstrap "$DOMAIN" "$PLIST_DST"
launchctl enable "$DOMAIN/$LABEL" 2>/dev/null || true

# ---- 等待就绪并验证 ----
say "等待服务就绪 ..."
ok=""
for _ in $(seq 1 60); do
    if curl -fsS -m 2 http://127.0.0.1:7863/healthz >/tmp/wb2a_healthz.$$ 2>/dev/null; then
        ok=1; break
    fi
    sleep 0.5
done

if [ -n "$ok" ]; then
    say "服务已就绪：$(cat /tmp/wb2a_healthz.$$)"
    rm -f /tmp/wb2a_healthz.$$
    say "状态："
    launchctl print "$DOMAIN/$LABEL" 2>/dev/null | grep -E '^\s*(state|pid|runs|last exit code)' | sed 's/^/    /'
    echo
    say "完成。崩溃/重启后 launchd 会自动拉起；停服请用：launchctl bootout $DOMAIN/$LABEL"
else
    rm -f /tmp/wb2a_healthz.$$
    warn "60s 内未就绪，请检查日志：tail -50 $REPO/data/wb2api.err.log"
    launchctl print "$DOMAIN/$LABEL" 2>/dev/null | grep -E '^\s*(state|last exit code)' | sed 's/^/    /' || true
    exit 1
fi
