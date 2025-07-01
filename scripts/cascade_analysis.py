import re
import sys
from datetime import datetime

def parse_log_line(line):
    """Parses a single log line into a dictionary."""
    regex = r'(\w+)="(.*?)"|(\w+)=(\S+)'
    matches = re.findall(regex, line)
    log_data = {}
    msg_match = re.search(r'msg="(.*?)"', line)
    if msg_match:
        log_data['msg'] = msg_match.group(1)

    for key_quoted, val_quoted, key_unquoted, val_unquoted in matches:
        if key_quoted and key_quoted != 'msg':
            log_data[key_quoted] = val_quoted
        elif key_unquoted:
            log_data[key_unquoted] = val_unquoted

    time_match = re.search(r'time=([\d\-]+T[\d:\.\+]+)', line)
    if time_match:
        log_data['time'] = time_match.group(1)

    return log_data

def analyze_logs(log_file):
    """Analyzes the client log file and prints a summary."""
    print(f"Analyzing log file: {log_file} with Cascade's script")
    print("\n--- Cascade's Qwrap Client Log Analysis ---")

    lines = open(log_file, 'r').readlines()

    # --- Latency Parsing ---
    connection_attempts = {}
    connection_latencies = {}
    disk_write_starts = {}
    disk_write_latencies = []

    for line in lines:
        log_data = parse_log_line(line)
        msg = log_data.get('msg', '')
        ts = datetime.fromisoformat(log_data.get('time')) if 'time' in log_data else None
        if not ts:
            continue

        if "Attempting to dial QUIC connection" in msg:
            addr = log_data.get('address')
            if addr:
                connection_attempts[addr] = ts
        
        if "Successfully established QUIC connection" in msg:
            addr = log_data.get('address')
            if addr in connection_attempts:
                latency = (ts - connection_attempts[addr]).total_seconds() * 1000
                connection_latencies.setdefault(addr, []).append(latency)
                del connection_attempts[addr]

        if "Received expected chunk for writing" in msg or "Writing buffered chunk" in msg:
            chunk_id = log_data.get('chunk_id')
            if chunk_id:
                disk_write_starts[chunk_id] = ts
        
        if "Chunk written to disk" in msg:
            chunk_id = log_data.get('chunk_id')
            if chunk_id in disk_write_starts:
                latency = (ts - disk_write_starts[chunk_id]).total_seconds() * 1000
                disk_write_latencies.append(latency)
                del disk_write_starts[chunk_id]

    # --- Main Info Parsing ---
    init_line = next((l for l in lines if 'qwrap client starting' in l), None)
    file_id, orchestrator, destination, start_time = "Unknown", "Unknown", "Unknown", None
    if init_line:
        log_data = parse_log_line(init_line)
        file_id, orchestrator, destination = log_data.get('file_id'), log_data.get('orchestrator'), log_data.get('destination')
        start_time = datetime.fromisoformat(log_data.get('time'))

    end_time = datetime.fromisoformat(parse_log_line(lines[-1]).get('time')) if lines else None

    progress_line = next((l for l in reversed(lines) if 'msg=Progress' in l), None)
    total_chunks, completed_chunks, downloaded_bytes_str = 0, 0, "0.0 MiB"
    if progress_line:
        log_data = parse_log_line(progress_line)
        chunks_str = log_data.get('chunks_completed', '0/0')
        completed_chunks, total_chunks = map(int, chunks_str.split('/'))
        downloaded_bytes_str = log_data.get('bytes', '0.0 MiB/0.0 MiB').split('/')[0]

    # --- Status Determination ---
    transfer_status = "SUCCESS" if total_chunks > 0 and completed_chunks == total_chunks else "FAILED"
    client_exit_status, client_error_message = "SUCCESS", "None"
    failure_line = next((l for l in reversed(lines) if 'msg="Download failed"' in l), None)
    if failure_line:
        log_data = parse_log_line(failure_line)
        client_exit_status = "FAILED"
        client_error_message = log_data.get('error', 'Unknown error')

    # --- Output ---
    print("\n[Transfer Identification]")
    print(f"  File ID:           {file_id}")
    print(f"  Orchestrator:      {orchestrator}")
    print(f"  Destination:       {destination}")

    print("\n[Transfer & Client Status]")
    duration = (end_time - start_time).total_seconds() if start_time and end_time else 0
    print(f"  Duration:          {duration:.2f} seconds")
    print(f"  Data Transfer:     {transfer_status}")
    print(f"  Client Exit:       {client_exit_status}")
    if client_exit_status == 'FAILED':
        print(f"  Exit Error:        {client_error_message}")

    print("\n[Chunk & Data Details]")
    print(f"  Chunk Progress:    {completed_chunks}/{total_chunks}")
    if total_chunks > 0:
        print(f"  Chunk Success Rate: {(completed_chunks / total_chunks) * 100:.2f}%")
    print(f"  Downloaded Size:   {downloaded_bytes_str}")
    if duration > 0:
        bytes_val = float(re.search(r'[\d\.]+', downloaded_bytes_str).group())
        bytes_total = bytes_val * 1024 * 1024
        print(f"  Avg. Throughput:   {bytes_total / (duration * 1024 * 1024):.2f} MiB/s")

    print("\n[Latency Analysis]")
    print("  Network Connection Latency (ms):")
    total_conn_latencies = []
    for addr, latencies in connection_latencies.items():
        avg_latency = sum(latencies) / len(latencies)
        print(f"    - Path {addr}: Avg: {avg_latency:.2f} ms (from {len(latencies)} sample(s))")
        total_conn_latencies.extend(latencies)
    if total_conn_latencies:
        print(f"    - Overall Avg: {sum(total_conn_latencies) / len(total_conn_latencies):.2f} ms")

    print("\n  Disk Write Latency (ms):")
    if disk_write_latencies:
        print(f"    - Avg: {sum(disk_write_latencies) / len(disk_write_latencies):.2f} ms")
        print(f"    - Min: {min(disk_write_latencies):.2f} ms")
        print(f"    - Max: {max(disk_write_latencies):.2f} ms")
        print(f"    - (from {len(disk_write_latencies)} chunk writes)")
    else:
        print("    - No disk write operations found.")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python3 cascade_analysis.py <path_to_log_file>")
        sys.exit(1)
    analyze_logs(sys.argv[1])
