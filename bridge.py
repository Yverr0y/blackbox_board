import json
import os
import sys
import time
from typing import Any

try:
    sys.stdin.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass


def emit(message_type: str, payload: dict[str, Any]) -> None:
    sys.stdout.write(json.dumps({"type": message_type, "payload": payload}, ensure_ascii=True) + "\n")
    sys.stdout.flush()


def repair_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8", errors="replace")
        except Exception:
            return value.hex()
    return str(value)


def describe_serial_port(port: Any) -> dict[str, Any]:
    return {
        "device": str(getattr(port, "device", "") or ""),
        "name": repair_text(getattr(port, "name", "") or ""),
        "description": repair_text(getattr(port, "description", "") or ""),
        "manufacturer": repair_text(getattr(port, "manufacturer", "") or ""),
        "product": repair_text(getattr(port, "product", "") or ""),
        "serialNumber": repair_text(getattr(port, "serial_number", "") or ""),
        "hwid": repair_text(getattr(port, "hwid", "") or ""),
    }


def list_serial_ports() -> list[dict[str, Any]]:
    try:
        from serial.tools import list_ports  # type: ignore
    except Exception:
        return []
    return [describe_serial_port(port) for port in list_ports.comports()]


def looks_like_meshtastic_port(port: dict[str, Any]) -> bool:
    combined = " ".join(
        str(port.get(key, "") or "")
        for key in ("description", "manufacturer", "product", "hwid", "device")
    ).lower()
    keywords = (
        "meshtastic",
        "heltec",
        "rak",
        "lora",
        "cp210",
        "ch340",
        "usb serial",
        "uart",
        "silicon labs",
        "wch",
    )
    return any(keyword in combined for keyword in keywords)


def get_local_node_num(interface: Any) -> str:
    return str(
        getattr(getattr(interface, "localNode", None), "nodeNum", None)
        or getattr(getattr(interface, "myInfo", None), "myNodeNum", None)
        or ""
    )


def snapshot_nodes(interface: Any) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    raw_nodes = getattr(interface, "nodes", {}) or {}
    for node_id, node in raw_nodes.items():
        user = node.get("user", {}) or {}
        result.append(
            {
                "id": str(node.get("num") or node_id or ""),
                "userId": str(user.get("id") or ""),
                "shortName": repair_text(user.get("shortName") or ""),
                "longName": repair_text(user.get("longName") or ""),
                "lastHeard": node.get("lastHeard"),
                "snr": node.get("snr"),
                "hopsAway": node.get("hopsAway"),
            }
        )
    return result


def get_local_identity(interface: Any) -> tuple[str, str, str]:
    local_num = get_local_node_num(interface)
    local_user_id = ""
    local_name = ""
    for node in snapshot_nodes(interface):
        if str(node.get("id") or "") == local_num:
            local_user_id = str(node.get("userId") or "")
            local_name = repair_text(node.get("longName") or node.get("shortName") or "")
            break
    return local_num, local_user_id, local_name


def emit_status(
    *,
    connected: bool,
    mode: str,
    error: str | None,
    port: str | None,
    available_ports: list[dict[str, Any]],
    interface: Any | None = None,
) -> None:
    local_node_id = ""
    local_user_id = ""
    local_display_name = ""
    if interface is not None:
        local_node_id, local_user_id, local_display_name = get_local_identity(interface)
    emit(
        "status",
        {
            "connected": connected,
            "mode": mode,
            "error": error,
            "port": port,
            "localNodeId": local_node_id or None,
            "localUserId": local_user_id or None,
            "localDisplayName": local_display_name or None,
            "availablePorts": available_ports,
        },
    )


def main() -> int:
    interface = None
    available_ports = list_serial_ports()

    try:
        from pubsub import pub  # type: ignore
        from meshtastic.serial_interface import SerialInterface  # type: ignore
        from meshtastic.util import findPorts  # type: ignore
    except Exception as exc:
        emit_status(
            connected=False,
            mode="unavailable",
            error=f"Meshtastic bridge imports unavailable: {exc}",
            port=None,
            available_ports=available_ports,
        )
        for raw in sys.stdin:
            message = raw.strip()
            if not message:
                continue
            try:
                parsed = json.loads(message)
            except Exception:
                emit("error", {"message": "invalid json from stdin"})
                continue
            if parsed.get("type") == "refresh_nodes":
                emit_status(
                    connected=False,
                    mode="unavailable",
                    error=f"Meshtastic bridge imports unavailable: {exc}",
                    port=None,
                    available_ports=list_serial_ports(),
                )
                emit("nodes", {"nodes": []})
            elif parsed.get("type") == "send_text":
                emit("error", {"message": "Meshtastic bridge is unavailable"})
        return 0

    requested_port = os.getenv("MESHTASTIC_PORT") or None
    port = requested_port
    if not port:
        try:
            detected_ports = [str(port_name) for port_name in (findPorts(True) or []) if port_name]
        except Exception:
            detected_ports = []
        available_ports = list_serial_ports()
        if not detected_ports:
            detected_ports = [
                str(item.get("device") or "")
                for item in available_ports
                if str(item.get("device") or "") and looks_like_meshtastic_port(item)
            ]
        port_index = 0
        raw_index = os.getenv("MESH_PORT_INDEX")
        if raw_index is not None:
            try:
                port_index = int(raw_index)
            except ValueError:
                pass
        port = detected_ports[port_index] if port_index < len(detected_ports) else (detected_ports[0] if detected_ports else None)

    if not port:
        emit_status(
            connected=False,
            mode="no-device",
            error="No Meshtastic USB device detected",
            port=None,
            available_ports=available_ports,
        )
        for raw in sys.stdin:
            message = raw.strip()
            if not message:
                continue
            try:
                parsed = json.loads(message)
            except Exception:
                emit("error", {"message": "invalid json from stdin"})
                continue
            if parsed.get("type") == "refresh_nodes":
                emit_status(
                    connected=False,
                    mode="no-device",
                    error="No Meshtastic USB device detected",
                    port=None,
                    available_ports=list_serial_ports(),
                )
                emit("nodes", {"nodes": []})
            elif parsed.get("type") == "send_text":
                emit("error", {"message": "No Meshtastic device connected"})
        return 0

    try:
        try:
            interface = SerialInterface(devPath=port, timeout=8)
        except TypeError:
            interface = SerialInterface(devPath=port)
    except Exception as exc:
        emit_status(
            connected=False,
            mode="error",
            error=f"serial connect failed: {exc}",
            port=port,
            available_ports=available_ports,
        )
        for raw in sys.stdin:
            message = raw.strip()
            if not message:
                continue
            try:
                parsed = json.loads(message)
            except Exception:
                emit("error", {"message": "invalid json from stdin"})
                continue
            if parsed.get("type") == "refresh_nodes":
                emit_status(
                    connected=False,
                    mode="error",
                    error=f"serial connect failed: {exc}",
                    port=port,
                    available_ports=list_serial_ports(),
                )
                emit("nodes", {"nodes": []})
            elif parsed.get("type") == "send_text":
                emit("error", {"message": f"serial connect failed: {exc}"})
        return 0

    emit_status(
        connected=True,
        mode="serial",
        error=None,
        port=port,
        available_ports=available_ports,
        interface=interface,
    )
    emit("nodes", {"nodes": snapshot_nodes(interface)})

    def on_receive(packet: dict[str, Any], interface: Any | None = None, **kwargs: Any) -> None:
        decoded = packet.get("decoded", {}) or {}
        text = decoded.get("text")
        active_interface = interface or kwargs.get("interface")
        if active_interface is not None:
            emit("nodes", {"nodes": snapshot_nodes(active_interface)})
        if text:
            emit(
                "inbound",
                {
                    "sender": str(packet.get("fromId") or packet.get("from") or "unknown"),
                    "recipient": str(packet.get("toId") or packet.get("to") or "unknown"),
                    "text": repair_text(text),
                },
            )

    pub.subscribe(on_receive, "meshtastic.receive")

    try:
        for raw in sys.stdin:
            message = raw.strip()
            if not message:
                continue
            try:
                parsed = json.loads(message)
            except Exception:
                emit("error", {"message": "invalid json from stdin"})
                continue

            if parsed.get("type") == "refresh_nodes":
                emit_status(
                    connected=True,
                    mode="serial",
                    error=None,
                    port=port,
                    available_ports=list_serial_ports(),
                    interface=interface,
                )
                emit("nodes", {"nodes": snapshot_nodes(interface)})
                continue

            if parsed.get("type") != "send_text":
                continue

            payload = parsed.get("payload", {}) or {}
            destination_id = str(payload.get("destinationId") or "")
            text = repair_text(payload.get("text") or "")
            want_ack = bool(payload.get("wantAck"))
            wait_for_ack = bool(payload.get("waitForAck"))
            retry_count = max(0, int(payload.get("retryOnAckTimeout") or 0))
            retry_delay_ms = max(0, int(payload.get("ackTimeoutRetryDelayMs") or 0))
            max_attempts = 1 + (retry_count if want_ack and wait_for_ack else 0)
            acked = None
            attempts = 0
            packet = None

            try:
                for attempt in range(max_attempts):
                    attempts = attempt + 1
                    try:
                        packet = interface.sendText(
                            text=text,
                            destinationId=destination_id,
                            wantAck=want_ack,
                        )
                    except TypeError:
                        packet = interface.sendText(text=text, destinationId=destination_id)
                    if not want_ack or not wait_for_ack:
                        break
                    try:
                        interface.waitForAckNak()
                        acked = True
                        break
                    except Exception:
                        acked = False
                        if attempt < max_attempts - 1 and retry_delay_ms > 0:
                            time.sleep(retry_delay_ms / 1000)

                if want_ack and wait_for_ack and acked is False:
                    emit("error", {
                        "message": f"ack timeout for {destination_id}",
                        "destinationId": destination_id,
                        "attempts": attempts,
                    })
                emit(
                    "sent",
                    {
                        "destinationId": destination_id,
                        "text": text,
                        "packetId": getattr(packet, "id", None),
                        "wantAck": want_ack,
                        "acked": acked,
                        "attempts": attempts,
                    },
                )
            except Exception as exc:
                emit("error", {"message": f"send failed: {exc}", "destinationId": destination_id})
    finally:
        try:
            if interface is not None:
                interface.close()
        except Exception:
            pass

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
