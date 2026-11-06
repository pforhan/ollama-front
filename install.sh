#!/usr/bin/env bash
# install.sh — install ollama-front as a background service.
#
# Supported platforms:
#   Linux + systemd   system unit in /etc/systemd/system/ (default; needs sudo)
#                     or --user unit in ~/.config/systemd/user/ (no sudo)
#   macOS + launchd   per-user LaunchAgent in ~/Library/LaunchAgents/ (default)
#                     or --system LaunchDaemon in /Library/LaunchDaemons/
# Anything else prints manual-run instructions and exits non-zero.
#
# Usage:
#   ./install.sh                     install + enable + start
#   ./install.sh --dry-run           print what would happen, change nothing
#   ./install.sh --user              systemd user unit / launchd agent (no sudo)
#   ./install.sh --system            explicit system-wide install (default on Linux)
#   ./install.sh --no-enable         write unit/plist but don't enable/start
#   ./install.sh --status            show service status (and recent logs)
#   ./install.sh --uninstall         disable/remove the service
#   ./install.sh --help
#
# Preflight: needs python3, proxy.py, and config.json next to this script.
# If config.json is missing, it is copied from config.json.example — edit it
# (forced_model, ports, rate limit) and re-run or restart the service.
#
# The systemd unit and launchd plist are rendered inline below from the
# *current* directory/user/python, so the script works from any checkout
# path. No checked-in service templates are needed — this file is the
# single source of truth for the service definitions.
#
# TODO(expand): Add support for another platform/init system here.
#   To do it:
#     1. Extend detect_os() below to recognise the new system (e.g. Windows
#        services/NSSM, FreeBSD/OpenBSD rc.d, OpenRC, runit, supervisord,
#        or a Docker container).
#     2. Add install_<name>/uninstall_<name>/status_<name> functions next to
#        install_systemd()/install_launchd() that render the native service
#        definition and enable it.
#     3. Wire the new branch into the dispatch at the bottom of this file
#        (and update the header comment above).
#   The generic preflight (resolve_python/ensure_config/verify_listening)
#   helpers are platform-independent and can be reused.

set -euo pipefail

SERVICE_NAME="ollama-front"
SYSTEMD_UNIT="${SERVICE_NAME}.service"
LAUNCHD_LABEL="com.ollama-front.proxy"
# Test hook: OLLAMA_FRONT_FORCE_OS=darwin|linux-systemd|unsupported overrides
# detection so the unsupported path can be exercised on any machine.
OS_OVERRIDE="${OLLAMA_FRONT_FORCE_OS:-}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROXY_PY="${SCRIPT_DIR}/proxy.py"
CONFIG_JSON="${SCRIPT_DIR}/config.json"
CONFIG_EXAMPLE="${SCRIPT_DIR}/config.json.example"
API_KEYS_JSON="${SCRIPT_DIR}/api_keys.json"

DRY_RUN=0
NO_ENABLE=0
REQUESTED_SCOPE=""  # "" | "system" | "user"
ACTION="install"    # install | uninstall | status

# --- small helpers ----------------------------------------------------------

say()  { printf '%s\n' "$*"; }
warn() { printf 'warning: %s\n' "$*" >&2; }
die()  { printf 'error: %s\n' "$*" >&2; exit 1; }
have_cmd() { command -v "$1" >/dev/null 2>&1; }

# run CMD... — echo in dry-run mode, otherwise execute it.
run() {
  if [[ "$DRY_RUN" -eq 1 ]]; then
    printf '[dry-run] %s\n' "$*"
  else
    "$@"
  fi
}

usage() {
  cat <<'EOF'
Usage: ./install.sh [--system|--user] [--dry-run] [--no-enable]
                    [--uninstall|--status]

Install ollama-front as a background service.

Options:
  --system      System-wide service (default on Linux). Needs sudo.
  --user        Per-user service, no sudo (systemd user unit / launchd agent).
  --dry-run,-n  Print the unit/plist and commands, but change nothing.
  --no-enable   Write the unit/plist but do not enable/start it.
  --uninstall   Disable and remove the service.
  --status      Show whether the service is running (plus recent logs).
  --help,-h     Show this help.

Supported: Linux + systemd, macOS + launchd. Other systems are warned
about with manual instructions (see TODO(expand) in this file).
EOF
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --system) REQUESTED_SCOPE="system" ;;
      --user)   REQUESTED_SCOPE="user" ;;
      --dry-run|-n) DRY_RUN=1 ;;
      --no-enable) NO_ENABLE=1 ;;
      --uninstall|uninstall) ACTION="uninstall" ;;
      --status|status) ACTION="status" ;;
      --help|-h) usage; exit 0 ;;
      --) shift; break ;;
      -*) die "unknown option: $1 (see --help)" ;;
      *) die "unexpected argument: $1 (see --help)" ;;
    esac
    shift
  done
}

# --- preflight (platform-independent) ---------------------------------------

resolve_python() {
  if have_cmd python3; then
    command -v python3
  else
    die "python3 not found in PATH — install Python 3 and re-run."
  fi
}

ensure_config() {
  [[ -f "$PROXY_PY" ]] || die "proxy.py not found next to this script: ${PROXY_PY}"
  if [[ ! -f "$CONFIG_JSON" ]]; then
    if [[ -f "$CONFIG_EXAMPLE" ]]; then
      say "config.json not found — copying from config.json.example"
      say "  (edit forced_model / ports / rate limit, then restart the service)"
      run cp "$CONFIG_EXAMPLE" "$CONFIG_JSON"
    else
      die "config.json not found and no config.json.example to copy from."
    fi
  fi
  if [[ ! -f "$API_KEYS_JSON" ]]; then
    warn "api_keys.json not found — proxy will run with no API keys"
    warn "  (anonymous tier only). Copy api_keys.json.example to api_keys.json"
    warn "  if you want admin/unlimited/standard keys."
  fi
}

# Listen port from config.json (fall back to the default 11435).
listen_port() {
  python3 - "$CONFIG_JSON" <<'PY' 2>/dev/null || echo 11435
import json, sys
try:
    print(int(json.load(open(sys.argv[1])).get("listen_port", 11435)))
except Exception:
    print(11435)
PY
}

verify_listening() {
  local port
  port="$(listen_port)"
  say "Verifying http://127.0.0.1:${port}/ ... "
  local ok=0
  if have_cmd curl; then
    curl -fsS --max-time 5 "http://127.0.0.1:${port}/" >/dev/null 2>&1 && ok=1
  elif have_cmd python3; then
    python3 -c 'import socket,sys; socket.create_connection(("127.0.0.1", int(sys.argv[1])), timeout=5).close()' "$port" >/dev/null 2>&1 && ok=1
  else
    warn "  curl/python3 not available — skipping verification (check the service logs)"
    return 0
  fi
  if [[ "$ok" -eq 1 ]]; then
    say "  OK — proxy is answering on port ${port}"
  else
    warn "  no answer yet on port ${port} (check the service logs)"
  fi
}

# --- OS detection -----------------------------------------------------------

# Echoes one of: linux-systemd | darwin | unsupported:<detail>
detect_os() {
  if [[ -n "$OS_OVERRIDE" ]]; then
    case "$OS_OVERRIDE" in
      darwin|macos) echo darwin ;;
      linux-systemd|systemd|linux) echo linux-systemd ;;
      *) echo "unsupported:${OS_OVERRIDE}" ;;
    esac
    return
  fi
  local os
  os="$(uname -s)"
  case "$os" in
    Linux)
      if have_cmd systemctl && [[ -d /run/systemd/system ]]; then
        echo linux-systemd
      else
        echo "unsupported:Linux without systemd (init system: $(ps -p 1 -o comm= 2>/dev/null || echo unknown))"
      fi
      ;;
    Darwin)
      echo darwin
      ;;
    *)
      echo "unsupported:${os}"
      ;;
  esac
}

# --- systemd ----------------------------------------------------------------

systemd_sudo() {
  # Echo the sudo prefix for system scope ("" when root or user scope).
  if [[ "$(id -u)" -eq 0 ]]; then
    echo ""
  elif have_cmd sudo; then
    echo "sudo"
  else
    die "system scope needs sudo (or re-run with --user for a user unit)."
  fi
}

render_systemd_unit() {
  # Args: python working_dir user wanted_by
  # svc_user="" omits User= (used for --user units, which must run as the
  # invoking user — User= is not allowed to pick another user there).
  local py="$1" wd="$2" svc_user="$3" wanted_by="$4"
  printf '%s\n' \
    "[Unit]" \
    "Description=ollama-front reverse proxy" \
    "After=network.target ollama.service" \
    "Wants=network.target" \
    "" \
    "[Service]" \
    "Type=simple"
  [[ -n "$svc_user" ]] && printf 'User=%s\n' "$svc_user"
  printf '%s\n' \
    "WorkingDirectory=${wd}" \
    "ExecStart=${py} ${wd}/proxy.py" \
    "Restart=always" \
    "RestartSec=3" \
    "StandardOutput=journal" \
    "StandardError=journal" \
    "" \
    "[Install]" \
    "WantedBy=${wanted_by}"
}

install_systemd() {
  local scope="$1"  # system | user
  local py svc_user dest wanted_by sudo_cmd
  py="$(resolve_python)"
  if [[ "$scope" == "user" ]]; then
    dest="${HOME}/.config/systemd/user/${SYSTEMD_UNIT}"
    wanted_by="default.target"
    # User units run as the invoking user; omit User= (see render fn).
    svc_user=""
    sudo_cmd=""
    say "Installing systemd user unit: ${dest}"
  else
    dest="/etc/systemd/system/${SYSTEMD_UNIT}"
    wanted_by="multi-user.target"
    svc_user="${SUDO_USER:-$(id -un)}"
    sudo_cmd="$(systemd_sudo)"
    say "Installing systemd system unit: ${dest} (service user: ${svc_user})"
  fi

  local unit_content
  unit_content="$(render_systemd_unit "$py" "$SCRIPT_DIR" "$svc_user" "$wanted_by")"

  if [[ "$DRY_RUN" -eq 1 ]]; then
    printf '[dry-run] --- %s ---\n%s\n[dry-run] end of unit\n' "$SYSTEMD_UNIT" "$unit_content"
    if [[ "$scope" == "user" ]]; then
      run systemctl --user daemon-reload
      [[ "$NO_ENABLE" -eq 0 ]] && run systemctl --user enable --now "$SYSTEMD_UNIT"
      say "[dry-run] (user units may need: loginctl enable-linger $(id -un) to run after logout)"
    elif [[ -n "$sudo_cmd" ]]; then
      run sudo systemctl daemon-reload
      [[ "$NO_ENABLE" -eq 0 ]] && run sudo systemctl enable --now "$SYSTEMD_UNIT"
    else
      run systemctl daemon-reload
      [[ "$NO_ENABLE" -eq 0 ]] && run systemctl enable --now "$SYSTEMD_UNIT"
    fi
    return 0
  fi

  mkdir -p "$(dirname "$dest")"
  if [[ "$scope" == "user" ]]; then
    printf '%s\n' "$unit_content" > "$dest"
    chmod 644 "$dest"
    systemctl --user daemon-reload
    if [[ "$NO_ENABLE" -eq 0 ]]; then
      systemctl --user enable --now "$SYSTEMD_UNIT"
    fi
    say "User unit installed. If it should run after logout:"
    say "  loginctl enable-linger $(id -un)"
  else
    if [[ -n "$sudo_cmd" ]]; then
      printf '%s\n' "$unit_content" | sudo tee "$dest" >/dev/null
      sudo chmod 644 "$dest"
      sudo systemctl daemon-reload
      if [[ "$NO_ENABLE" -eq 0 ]]; then
        sudo systemctl enable --now "$SYSTEMD_UNIT"
      fi
    else
      printf '%s\n' "$unit_content" > "$dest"
      chmod 644 "$dest"
      systemctl daemon-reload
      if [[ "$NO_ENABLE" -eq 0 ]]; then
        systemctl enable --now "$SYSTEMD_UNIT"
      fi
    fi
  fi
}

uninstall_systemd() {
  local scope="$1"
  local sudo_cmd=""
  if [[ "$scope" == "user" ]]; then
    run systemctl --user disable --now "$SYSTEMD_UNIT" || true
    run rm -f "${HOME}/.config/systemd/user/${SYSTEMD_UNIT}"
    run systemctl --user daemon-reload
  else
    sudo_cmd="$(systemd_sudo)"
    if [[ "$DRY_RUN" -eq 1 ]]; then
      if [[ -n "$sudo_cmd" ]]; then
        run sudo systemctl disable --now "$SYSTEMD_UNIT"
        run sudo rm -f "/etc/systemd/system/$SYSTEMD_UNIT"
        run sudo systemctl daemon-reload
      else
        run systemctl disable --now "$SYSTEMD_UNIT"
        run rm -f "/etc/systemd/system/$SYSTEMD_UNIT"
        run systemctl daemon-reload
      fi
    elif [[ -n "$sudo_cmd" ]]; then
      sudo systemctl disable --now "$SYSTEMD_UNIT" || true
      sudo rm -f "/etc/systemd/system/${SYSTEMD_UNIT}"
      sudo systemctl daemon-reload
    else
      systemctl disable --now "$SYSTEMD_UNIT" || true
      rm -f "/etc/systemd/system/${SYSTEMD_UNIT}"
      systemctl daemon-reload
    fi
    say "Removed systemd system unit."
  fi
}

status_systemd() {
  local scope="$1"
  if [[ "$scope" == "user" ]]; then
    systemctl --user status "$SYSTEMD_UNIT" --no-pager -l || true
    systemctl --user is-active "$SYSTEMD_UNIT" >/dev/null 2>&1 \
      && say "State: active" || say "State: not active"
    journalctl --user -u "$SYSTEMD_UNIT" -n 30 --no-pager 2>/dev/null || true
  else
    systemctl status "$SYSTEMD_UNIT" --no-pager -l || true
    systemctl is-active "$SYSTEMD_UNIT" >/dev/null 2>&1 \
      && say "State: active" || say "State: not active"
    journalctl -u "$SYSTEMD_UNIT" -n 30 --no-pager 2>/dev/null || true
  fi
}

# --- macOS / launchd ----------------------------------------------------------

render_launchd_plist() {
  # Args: python working_dir stdout_log stderr_log
  local py="$1" wd="$2" out_log="$3" err_log="$4"
  cat <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>${LAUNCHD_LABEL}</string>
  <key>ProgramArguments</key>
  <array>
    <string>${py}</string>
    <string>${wd}/proxy.py</string>
  </array>
  <key>WorkingDirectory</key>
  <string>${wd}</string>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <true/>
  <key>ThrottleInterval</key>
  <integer>3</integer>
  <key>StandardOutPath</key>
  <string>${out_log}</string>
  <key>StandardErrorPath</key>
  <string>${err_log}</string>
  <key>EnvironmentVariables</key>
  <dict>
    <key>PYTHONUNBUFFERED</key>
    <string>1</string>
  </dict>
</dict>
</plist>
EOF
}

install_launchd() {
  local scope="$1"  # user | system
  local py dest out_log err_log uid
  py="$(resolve_python)"
  if [[ "$scope" == "system" ]]; then
    dest="/Library/LaunchDaemons/${LAUNCHD_LABEL}.plist"
    out_log="/var/log/ollama-front.log"
    err_log="/var/log/ollama-front.log"
    # LaunchDaemons run as root, so the proxy needs read access to
    # SCRIPT_DIR/config.json — warn that a root-run proxy may not suit
    # home-directory checkouts, and prefer --user for those.
    warn "system LaunchDaemon runs as root; a checkout under \$HOME may not"
    warn "  be readable by root. Prefer --user (default) for home checkouts."
    have_cmd sudo || die "--system on macOS needs sudo."
  else
    dest="${HOME}/Library/LaunchAgents/${LAUNCHD_LABEL}.plist"
    out_log="${HOME}/Library/Logs/ollama-front.log"
    err_log="${HOME}/Library/Logs/ollama-front.log"
  fi
  uid="$(id -u)"
  say "Installing launchd ${scope} plist: ${dest}"

  local plist
  plist="$(render_launchd_plist "$py" "$SCRIPT_DIR" "$out_log" "$err_log")"

  if [[ "$DRY_RUN" -eq 1 ]]; then
    printf '[dry-run] --- %s ---\n%s\n[dry-run] end of plist\n' "$LAUNCHD_LABEL.plist" "$plist"
    if [[ "$scope" == "system" ]]; then
      run sudo launchctl bootstrap system "$dest"
      [[ "$NO_ENABLE" -eq 0 ]] && run sudo launchctl enable "system/$LAUNCHD_LABEL"
    else
      run launchctl bootstrap "gui/$uid" "$dest"
      [[ "$NO_ENABLE" -eq 0 ]] && run launchctl enable "gui/$uid/$LAUNCHD_LABEL"
    fi
    return 0
  fi

  mkdir -p "$(dirname "$dest")" "$(dirname "$out_log")"
  if [[ "$scope" == "system" ]]; then
    printf '%s\n' "$plist" | sudo tee "$dest" >/dev/null
    sudo chmod 644 "$dest"
    sudo launchctl bootstrap system "$dest" 2>/dev/null \
      || sudo launchctl load -w "$dest" 2>/dev/null \
      || warn "launchctl bootstrap/load failed — is the plist valid? (sudo launchctl print system/$LAUNCHD_LABEL)"
    if [[ "$NO_ENABLE" -eq 0 ]]; then
      sudo launchctl enable "system/${LAUNCHD_LABEL}" 2>/dev/null || true
      sudo launchctl kickstart -k "system/${LAUNCHD_LABEL}" 2>/dev/null || true
    fi
  else
    printf '%s\n' "$plist" > "$dest"
    chmod 644 "$dest"
    # Modern macOS: bootstrap; fall back to the legacy load for old versions.
    launchctl bootstrap "gui/${uid}" "$dest" 2>/dev/null \
      || launchctl load -w "$dest" 2>/dev/null \
      || warn "launchctl bootstrap/load failed — is the plist valid? (launchctl print gui/${uid}/${LAUNCHD_LABEL})"
    if [[ "$NO_ENABLE" -eq 0 ]]; then
      launchctl enable "gui/${uid}/${LAUNCHD_LABEL}" 2>/dev/null || true
      launchctl kickstart -k "gui/${uid}/${LAUNCHD_LABEL}" 2>/dev/null || true
    fi
  fi
}

uninstall_launchd() {
  local scope="$1" uid dest
  uid="$(id -u)"
  if [[ "$scope" == "system" ]]; then
    dest="/Library/LaunchDaemons/${LAUNCHD_LABEL}.plist"
    if [[ "$DRY_RUN" -eq 1 ]]; then
      run sudo launchctl disable "system/$LAUNCHD_LABEL"
      run sudo launchctl bootout "system/$LAUNCHD_LABEL"
      run sudo rm -f "$dest"
    else
      sudo launchctl disable "system/${LAUNCHD_LABEL}" 2>/dev/null || true
      sudo launchctl bootout "system/${LAUNCHD_LABEL}" 2>/dev/null \
        || sudo launchctl unload -w "$dest" 2>/dev/null || true
      sudo rm -f "$dest"
    fi
  else
    dest="${HOME}/Library/LaunchAgents/${LAUNCHD_LABEL}.plist"
    if [[ "$DRY_RUN" -eq 1 ]]; then
      run launchctl disable "gui/$uid/$LAUNCHD_LABEL"
      run launchctl bootout "gui/$uid/$LAUNCHD_LABEL"
      run rm -f "$dest"
    else
      launchctl disable "gui/${uid}/${LAUNCHD_LABEL}" 2>/dev/null || true
      launchctl bootout "gui/${uid}/${LAUNCHD_LABEL}" 2>/dev/null \
        || launchctl unload -w "$dest" 2>/dev/null || true
      rm -f "$dest"
    fi
  fi
  say "Removed launchd plist."
}

status_launchd() {
  local scope="$1" uid
  uid="$(id -u)"
  if [[ "$scope" == "system" ]]; then
    sudo launchctl print "system/${LAUNCHD_LABEL}" 2>&1 | head -n 40 || true
    tail -n 30 /var/log/ollama-front.log 2>/dev/null || true
  else
    launchctl print "gui/${uid}/${LAUNCHD_LABEL}" 2>&1 | head -n 40 || true
    tail -n 30 "${HOME}/Library/Logs/ollama-front.log" 2>/dev/null || true
  fi
}

# --- unsupported platforms ----------------------------------------------------

# TODO(expand): Supported init systems live in detect_os() + the dispatch at
#   the bottom of this file. Anything that falls through to here should stay
#   warned-about (never silently "succeed") until a real backend is added.
install_unsupported() {
  local detected="$1"
  warn "Unsupported platform for automatic service install: ${detected}"
  warn "ollama-front currently supports Linux + systemd and macOS + launchd."
  warn "Run the proxy manually instead:"
  warn "  cd ${SCRIPT_DIR}"
  warn "  python3 proxy.py"
  warn "For other systems, create the equivalent service definition yourself"
  warn "(or add a backend here — see TODO(expand) at the top of install.sh)."
  exit 1
}

# --- main -------------------------------------------------------------------

main() {
  parse_args "$@"

  # Status/uninstall skip the config preflight so they work even if the
  # checkout was moved or config.json was deleted.
  local detected
  detected="$(detect_os)"
  say "Detected platform: ${detected} (script dir: ${SCRIPT_DIR})"

  case "$detected" in
    linux-systemd)
      local scope
      scope="${REQUESTED_SCOPE:-system}"
      case "$ACTION" in
        install)
          ensure_config
          install_systemd "$scope"
          if [[ "$DRY_RUN" -eq 1 ]]; then
            say "[dry-run] no service started, nothing verified."
          elif [[ "$NO_ENABLE" -eq 1 ]]; then
            if [[ "$scope" == "user" ]]; then
              say "Written only (--no-enable). Start with: systemctl --user enable --now $SYSTEMD_UNIT"
            else
              say "Written only (--no-enable). Start with: systemctl enable --now $SYSTEMD_UNIT"
            fi
          else
            verify_listening
            say "Check logs: journalctl -u ${SYSTEMD_UNIT} -f"
          fi
          ;;
        uninstall) uninstall_systemd "$scope" ;;
        status)    status_systemd "$scope" ;;
      esac
      ;;
    darwin)
      local scope
      scope="${REQUESTED_SCOPE:-user}"
      case "$ACTION" in
        install)
          ensure_config
          install_launchd "$scope"
          if [[ "$DRY_RUN" -eq 1 ]]; then
            say "[dry-run] no service started, nothing verified."
          elif [[ "$NO_ENABLE" -eq 1 ]]; then
            say "Written only (--no-enable)."
          else
            verify_listening
            say "Check logs: tail -f ~/Library/Logs/ollama-front.log"
          fi
          ;;
        uninstall) uninstall_launchd "$scope" ;;
        status)    status_launchd "$scope" ;;
      esac
      ;;
    unsupported:*)
      # TODO(expand): add a branch above for each new supported platform;
      # until then every other system lands here and is warned about.
      install_unsupported "${detected#unsupported:}"
      ;;
    *)
      install_unsupported "$detected"
      ;;
  esac
}

main "$@"
