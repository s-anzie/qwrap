import re
import json
from datetime import datetime, timedelta
from collections import defaultdict
import argparse

# Expression régulière pour parser les lignes de log structurées (slog TextHandler)
# Exemple: time=2025-06-18T13:22:26.538+02:00 level=INFO source=/path/to/file.go:66 msg="qwrap client starting" key1=value1 key2="value with spaces"
LOG_LINE_RE = re.compile(
    r"time=(?P<timestamp>\S+)\s+"
    r"level=(?P<level>\S+)\s+"
    r"(?:source=(?P<source>[^\s=]+)\s+)?"  # source est optionnel
    r"msg=(?P<msg>\"[^\"]*\"|\S+)"
    r"(?P<attrs>.*)"
)

# Expression régulière pour parser les attributs clé-valeur
ATTR_RE = re.compile(r'(?P<key>[a-zA-Z0-9_-]+)=(?P<value>\"[^\"]*\"|\S+)')

def parse_log_attributes(attr_str):
    """Parse la chaîne d'attributs d'une ligne de log."""
    attributes = {}
    for match in ATTR_RE.finditer(attr_str):
        key = match.group("key")
        value = match.group("value")
        if value.startswith('"') and value.endswith('"'):
            value = value[1:-1]  # Enlever les guillemets
        # Essayer de convertir en int ou float si possible
        try:
            if '.' in value:
                attributes[key] = float(value)
            else:
                attributes[key] = int(value)
        except ValueError:
            attributes[key] = value
    return attributes

def parse_log_file(log_file_path):
    """Parse un fichier de log client et retourne une liste d'entrées de log structurées."""
    parsed_logs = []
    try:
        with open(log_file_path, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f):
                line = line.strip()
                if not line:
                    continue
                
                match = LOG_LINE_RE.match(line)
                if match:
                    log_entry = match.groupdict()
                    try:
                        log_entry["timestamp_dt"] = datetime.fromisoformat(log_entry["timestamp"])
                    except ValueError:
                        # Essayer un format alternatif si le premier échoue (ex: sans timezone)
                        try:
                            log_entry["timestamp_dt"] = datetime.strptime(log_entry["timestamp"], "%Y-%m-%dT%H:%M:%S.%f")
                        except ValueError as e_alt:
                            print(f"Warning: Could not parse timestamp on line {line_num+1}: {log_entry['timestamp']} - {e_alt}")
                            log_entry["timestamp_dt"] = None # Ou une date par défaut ?

                    # Nettoyer le message
                    if log_entry["msg"].startswith('"') and log_entry["msg"].endswith('"'):
                         log_entry["msg"] = log_entry["msg"][1:-1]
                    
                    if log_entry["attrs"]:
                        log_entry["attributes"] = parse_log_attributes(log_entry["attrs"])
                    else:
                        log_entry["attributes"] = {}
                    parsed_logs.append(log_entry)
                else:
                    print(f"Warning: Could not parse log line {line_num+1}: {line[:100]}...")
    except FileNotFoundError:
        print(f"Error: Log file not found at {log_file_path}")
        return None
    except Exception as e:
        print(f"Error reading or parsing log file {log_file_path}: {e}")
        return None
        
    return parsed_logs

def analyze_client_logs(parsed_logs):
    """Analyse les logs parsés pour en extraire des métriques."""
    if not parsed_logs:
        print("No logs to analyze.")
        return

    metrics = {
        "start_time": None,
        "end_time": None,
        "duration_seconds": None,
        "file_id": "N/A",
        "request_id": "N/A",
        "orchestrator_addr": "N/A",
        "destination_path": "N/A",
        "status": "UNKNOWN",
        "final_error_message": None,
        "total_chunks_from_plan": 0,
        "total_size_from_plan_bytes": 0,
        "final_progress": None, # Stocke la dernière ProgressInfo
        "chunk_download_attempts": defaultdict(int), # chunk_id -> count
        "chunk_failures_permanent": set(), # set of chunk_ids
        "chunk_successes": set(), # set of chunk_ids
        "errors_by_type": defaultdict(int),
        "initial_plan_received": False,
        "plan_updates_received": 0,
        "concurrency": 0,
    }

    # --- Extraction des informations initiales ---
    for log in parsed_logs:
        if log["msg"] == "qwrap client starting (new pipeline arch)":
            metrics["start_time"] = log.get("timestamp_dt")
            metrics["file_id"] = log["attributes"].get("file_id", "N/A")
            metrics["request_id"] = log["attributes"].get("request_id", "N/A") # Doit être ajouté au log client si ce n'est pas le cas
            metrics["orchestrator_addr"] = log["attributes"].get("orchestrator", "N/A")
            metrics["destination_path"] = log["attributes"].get("destination", "N/A")
            metrics["concurrency"] = int(log["attributes"].get("concurrency", 0))
            break # On prend la première occurrence

    # --- Analyse des événements de téléchargement ---
    last_progress_log = None
    for log in parsed_logs:
        msg = log["msg"]
        attrs = log.get("attributes", {})

        if msg == "Initial plan received successfully from orchestrator" or \
           (msg == "Initial transfer plan received" and attrs.get("op") == "manageDownloadLifecycle"): # Nouvelle version
            metrics["initial_plan_received"] = True
            metrics["total_chunks_from_plan"] = int(attrs.get("num_chunks", attrs.get("num_assignments", 0)))
            metrics["total_size_from_plan_bytes"] = int(attrs.get("total_size_bytes", 0))
            if metrics["file_id"] == "N/A" and attrs.get("file_id"): # Si pas déjà setté
                metrics["file_id"] = attrs.get("file_id")


        if msg == "Downloader received UpdatedTransferPlan":
             metrics["plan_updates_received"] +=1

        if msg.startswith("Progress") or msg.startswith("Final progress") or msg.startswith("Last progress"):
            last_progress_log = log # Garder le dernier log de progression

        if msg == "Chunk download attempt failed": # Log du ResultAggregator ou worker
            chunk_id = attrs.get("chunk_id")
            if chunk_id is not None:
                metrics["chunk_download_attempts"][int(chunk_id)] += 1
            err_msg = attrs.get("error", "unknown chunk failure")
            if "ErrChecksumMismatch" in err_msg:
                 metrics["errors_by_type"]["CHECKSUM_MISMATCH"] += 1
            elif "permanently failed" in msg or "Max total retries reached for chunk" in msg: # Log du ResultAggregator
                 if chunk_id is not None:
                    metrics["chunk_failures_permanent"].add(int(chunk_id))


        if msg == "Download completed successfully!":
            metrics["status"] = "SUCCESS"
            metrics["end_time"] = log.get("timestamp_dt")
            break # On a le statut final

        if msg == "Download failed":
            metrics["status"] = "FAILED"
            metrics["final_error_message"] = attrs.get("error", "Unknown failure reason")
            metrics["end_time"] = log.get("timestamp_dt")
            break
            
        if msg == "Download was cancelled.":
            metrics["status"] = "CANCELLED"
            metrics["final_error_message"] = "Download was cancelled by user/signal"
            metrics["end_time"] = log.get("timestamp_dt")
            break

    if metrics["status"] == "UNKNOWN" and parsed_logs: # Si on n'a pas trouvé de message de fin explicite
        metrics["end_time"] = parsed_logs[-1].get("timestamp_dt") # Prendre le timestamp du dernier log
        if metrics["start_time"] and metrics["end_time"] and (metrics["end_time"] - metrics["start_time"]) > timedelta(seconds=lifecycleTimeout - 5): # Proche du timeout global
            metrics["status"] = "TIMED_OUT_OR_INCOMPLETE"
            metrics["final_error_message"] = "Download likely timed out or did not complete cleanly."
        else:
             metrics["status"] = "INCOMPLETE" # Ou potentiellement toujours en cours si le log est partiel


    if metrics["start_time"] and metrics["end_time"]:
        metrics["duration_seconds"] = (metrics["end_time"] - metrics["start_time"]).total_seconds()

    if last_progress_log:
        attrs = last_progress_log.get("attributes", {})
        metrics["final_progress"] = {
            "completed_chunks": int("{}".format(attrs.get("chunks_completed", 0)).split('/')[0]), # Prendre le premier nombre (succès)
            "failed_perm_chunks": int(attrs.get("chunks_failed_perm", 0)),
            "total_chunks_reported": int(attrs.get("total_chunks", metrics["total_chunks_from_plan"])), # Prendre le total du plan si dispo
            "downloaded_bytes": parse_bytes_from_log(attrs.get("bytes", "0/0").split('/')[0]),
            "total_size_bytes_reported": parse_bytes_from_log(attrs.get("bytes", "0/0").split('/')[1]),
        }
        # Si le plan initial a donné une taille, utiliser cela comme référence principale
        if metrics["total_size_from_plan_bytes"] > 0:
            metrics["final_progress"]["total_size_bytes_authoritative"] = metrics["total_size_from_plan_bytes"]
        else:
            metrics["final_progress"]["total_size_bytes_authoritative"] = metrics["final_progress"]["total_size_bytes_reported"]
        
        # Remplir les succès à partir de la progression si le statut est SUCCESS
        if metrics["status"] == "SUCCESS":
            # On ne peut pas lister les chunk_id individuels ici, seulement le compte
            metrics["chunk_successes_count"] = metrics["final_progress"]["completed_chunks"]

    return metrics

def parse_bytes_from_log(byte_str):
    """Parse une chaîne comme "10.5 MiB" ou "1024 B" en octets."""
    byte_str = byte_str.strip()
    multipliers = {"B": 1, "KiB": 1024, "MiB": 1024**2, "GiB": 1024**3, "TiB": 1024**4}
    parts = byte_str.split()
    if len(parts) == 2:
        try:
            value = float(parts[0])
            unit = parts[1]
            if unit in multipliers:
                return int(value * multipliers[unit])
        except ValueError:
            pass
    try: # Si c'est juste un nombre (ex: "0" pour "0/0")
        return int(byte_str)
    except ValueError:
        return 0


def print_metrics(metrics):
    """Affiche les métriques de manière lisible."""
    print("\n--- Qwrap Client Log Analysis ---")
    if not metrics:
        print("No metrics generated.")
        return

    print(f"\n[Transfer Identification]")
    print(f"  File ID:           {metrics.get('file_id', 'N/A')}")
    # print(f"  Client Request ID: {metrics.get('request_id', 'N/A')}") # Si loggé
    print(f"  Orchestrator:      {metrics.get('orchestrator_addr', 'N/A')}")
    print(f"  Destination:       {metrics.get('destination_path', 'N/A')}")

    print(f"\n[Overall Status & Duration]")
    print(f"  Start Time:        {metrics.get('start_time', 'N/A')}")
    print(f"  End Time:          {metrics.get('end_time', 'N/A')}")
    if metrics.get('duration_seconds') is not None:
        print(f"  Duration:          {metrics['duration_seconds']:.2f} seconds")
    print(f"  Final Status:      {metrics.get('status', 'N/A')}")
    if metrics.get('final_error_message'):
        print(f"  Error Message:     {metrics['final_error_message']}")

    print(f"\n[Plan & Chunks]")
    print(f"  Initial Plan Recv: {metrics.get('initial_plan_received', False)}")
    print(f"  Plan Updates Recv: {metrics.get('plan_updates_received', 0)}")
    
    total_chunks_plan = metrics.get("total_chunks_from_plan", 0)
    print(f"  Total Chunks (plan): {total_chunks_plan}")
    
    if metrics.get("final_progress"):
        fp = metrics["final_progress"]
        print(f"  Total Chunks (final progress): {fp.get('total_chunks_reported', 'N/A')}")
        print(f"  Completed Chunks (final):    {fp.get('completed_chunks', 'N/A')}")
        print(f"  Perm. Failed Chunks (final): {fp.get('failed_perm_chunks', 'N/A')}")
        
        completed = fp.get('completed_chunks', 0)
        total_authoritative = fp.get('total_size_bytes_authoritative', 0)
        if total_chunks_plan > 0:
             print(f"  Chunk Success Rate: {(completed / total_chunks_plan) * 100:.2f}% (based on plan count)")
        elif fp.get('total_chunks_reported', 0) > 0:
             print(f"  Chunk Success Rate: {(completed / fp.get('total_chunks_reported', 1)) * 100:.2f}% (based on progress report)")


    print(f"\n[Data Transfer]")
    if metrics.get("total_size_from_plan_bytes", 0) > 0:
        print(f"  Exp. Total Size (plan): {format_bytes_human(metrics['total_size_from_plan_bytes'])}")
    if metrics.get("final_progress"):
        fp = metrics["final_progress"]
        print(f"  Downloaded Bytes (final): {format_bytes_human(fp.get('downloaded_bytes',0))}")
        print(f"  Reported Total Size (final): {format_bytes_human(fp.get('total_size_bytes_reported',0))}")
        if fp.get('total_size_bytes_authoritative', 0) > 0 and metrics.get('duration_seconds', 0) > 0.001:
            speed_bps = (fp.get('downloaded_bytes', 0) * 8) / metrics['duration_seconds']
            print(f"  Avg. Throughput:      {format_bytes_human(int(speed_bps / 8))}/s ({format_bps_human(speed_bps)})")


    if metrics["errors_by_type"]:
        print(f"\n[Error Summary]")
        for err_type, count in metrics["errors_by_type"].items():
            print(f"  {err_type}: {count}")
    
    if metrics["chunk_download_attempts"]:
        print(f"\n[Chunk Attempt Details (sample)]")
        # Trier par nombre de tentatives décroissant pour voir les plus problématiques
        sorted_attempts = sorted(metrics["chunk_download_attempts"].items(), key=lambda item: item[1], reverse=True)
        for i, (chunk_id, attempts) in enumerate(sorted_attempts):
            if i < 10 or attempts > 1 : # Afficher les 10 premiers ou ceux avec >1 tentative
                status = "SUCCESS (eventual)"
                if chunk_id in metrics["chunk_failures_permanent"]:
                    status = "FAILED (permanent)"
                elif chunk_id not in metrics.get("chunk_successes", set()) and metrics.get("final_progress", {}).get("completed_chunks",0) < metrics.get("total_chunks_from_plan",1):
                     # Si non explicitement marqué comme succès et que tous les chunks ne sont pas faits, on assume qu'il a pu échouer
                     # (cette heuristique peut être améliorée avec des logs de succès par chunk)
                     status = "UNKNOWN/POSSIBLY_FAILED"

                print(f"  Chunk {chunk_id}: {attempts} attempts, Final Status: {status}")


def format_bytes_human(num_bytes):
    """Formate les octets en une chaîne lisible par l'homme (KiB, MiB, etc.)."""
    if num_bytes < 1024:
        return f"{num_bytes} B"
    for unit in ['KiB', 'MiB', 'GiB', 'TiB', 'PiB', 'EiB', 'ZiB']:
        if abs(num_bytes) < 1024.0 * 1024.0: # Passer au niveau suivant si on est encore >= 1024 de l'unité actuelle
            return f"{num_bytes / 1024.0:.2f} {unit}"
        num_bytes /= 1024.0
    return f"{num_bytes:.2f} YiB"


def format_bps_human(num_bps):
    """Formate les bits par seconde en une chaîne lisible par l'homme (kbps, Mbps, etc.)."""
    if num_bps < 1000:
        return f"{num_bps:.2f} bps"
    for unit in ['kbps', 'Mbps', 'Gbps', 'Tbps']:
        if abs(num_bps) < 1000.0 * 1000.0:
             return f"{num_bps / 1000.0:.2f} {unit}"
        num_bps /= 1000.0
    return f"{num_bps:.2f} Pbps"


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Analyze qwrap client log file for transfer metrics.")
    parser.add_argument("logfile", help="Path to the qwrap client log file.")
    args = parser.parse_args()

    print(f"Analyzing log file: {args.logfile}")
    parsed_logs = parse_log_file(args.logfile)

    if parsed_logs:
        metrics = analyze_client_logs(parsed_logs)
        print_metrics(metrics)