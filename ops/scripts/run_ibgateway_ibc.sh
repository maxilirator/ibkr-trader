#!/usr/bin/env bash
set -euo pipefail

fail() {
    echo "ibgateway-ibc: $*" >&2
    exit 1
}

if [[ -z "${DISPLAY:-}" ]]; then
    fail "DISPLAY is not set. Capture the active desktop first with write_ibgateway_session_env.sh."
fi

XAUTHORITY_PATH="${XAUTHORITY:-${HOME}/.Xauthority}"
IBG_TWS_MAJOR_VERSION="${IBG_TWS_MAJOR_VERSION:-1045}"
IBG_IBC_PATH="${IBG_IBC_PATH:-${HOME}/IBC}"
IBG_TWS_PATH="${IBG_TWS_PATH:-${HOME}/Jts}"
IBG_IBC_INI="${IBG_IBC_INI:-${IBG_IBC_PATH}/config.ini}"
IBG_TRADING_MODE="${IBG_TRADING_MODE:-live}"
IBG_TWOFA_TIMEOUT_ACTION="${IBG_TWOFA_TIMEOUT_ACTION:-restart}"
IBG_ENSURE_RELOGIN_AFTER_TWOFA_TIMEOUT="${IBG_ENSURE_RELOGIN_AFTER_TWOFA_TIMEOUT:-yes}"
IBG_TWS_SETTINGS_PATH="${IBG_TWS_SETTINGS_PATH:-}"
IBG_JAVA_PATH="${IBG_JAVA_PATH:-}"

[[ -f "${XAUTHORITY_PATH}" ]] || fail "XAUTHORITY file not found: ${XAUTHORITY_PATH}"
[[ -d "${IBG_TWS_PATH}" ]] || fail "TWS/Gateway path not found: ${IBG_TWS_PATH}"
[[ -x "${IBG_IBC_PATH}/scripts/ibcstart.sh" ]] || fail "IBC launcher missing or not executable: ${IBG_IBC_PATH}/scripts/ibcstart.sh"
[[ -f "${IBG_IBC_INI}" ]] || fail "IBC config file not found: ${IBG_IBC_INI}"

ensure_ini_setting() {
    local key="$1"
    local value="$2"
    if grep -qE "^${key}=" "${IBG_IBC_INI}"; then
        sed -i "s|^${key}=.*|${key}=${value}|" "${IBG_IBC_INI}"
    else
        printf '\n%s=%s\n' "${key}" "${value}" >> "${IBG_IBC_INI}"
    fi
}

if [[ "${IBG_ENSURE_RELOGIN_AFTER_TWOFA_TIMEOUT,,}" != "no" ]]; then
    ensure_ini_setting "ReloginAfterSecondFactorAuthenticationTimeout" "yes"
    ensure_ini_setting "ExitAfterSecondFactorAuthenticationTimeout" "no"
fi

if pgrep -f "ibcalpha\\.ibc\\.IbcGateway.*${IBG_IBC_INI}" >/dev/null 2>&1; then
    fail "IB Gateway already appears to be running for ${IBG_IBC_INI}"
fi

declare -a args=(
    "${IBG_TWS_MAJOR_VERSION}"
    --gateway
    "--tws-path=${IBG_TWS_PATH}"
    "--ibc-path=${IBG_IBC_PATH}"
    "--ibc-ini=${IBG_IBC_INI}"
    "--mode=${IBG_TRADING_MODE}"
    "--on2fatimeout=${IBG_TWOFA_TIMEOUT_ACTION}"
)

if [[ -n "${IBG_TWS_SETTINGS_PATH}" ]]; then
    args+=("--tws-settings-path=${IBG_TWS_SETTINGS_PATH}")
fi

if [[ -n "${IBG_JAVA_PATH}" ]]; then
    args+=("--java-path=${IBG_JAVA_PATH}")
fi

export XAUTHORITY="${XAUTHORITY_PATH}"

echo "ibgateway-ibc: starting Gateway on DISPLAY=${DISPLAY} using IBC config ${IBG_IBC_INI}" >&2
exec "${IBG_IBC_PATH}/scripts/ibcstart.sh" "${args[@]}"
