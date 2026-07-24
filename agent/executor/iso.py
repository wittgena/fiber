# agent.executor.iso
## @lineage: atoa.executor.iso
## @lineage: bound.executor.iso
## @lineage: xor.executor.iso
## @lineage: xphi.xor.executor.iso
## @lineage: anchor.phase.executor.iso
## @lineage: anchor.executor.iso
## @lineage: anchor.cli.runner
import sys
import threading
import subprocess
from pathlib import Path

from arch.contract.registry.unified import registry
from phase.bind.resolver import find_current_self, get_invoker
from watcher.plane.emitter import get_emitter

_invoker_full, MODULE_NAMESPACE = get_invoker(Path(__file__))
log = get_emitter(MODULE_NAMESPACE, phase="SYSTEM")
SELF_ROOT = find_current_self()

class IsoRunner:
    @staticmethod
    def execute_subtask(command_name: str, args: list):
        log.signal(f"[Sub-Task] Resolving contract for isolated execution: {command_name}")
        task_info_list = registry.registered_cli_tasks.get(command_name)
        if not task_info_list:
            raise RuntimeError(f"Cannot find contract '{command_name}' in registry.")

        module_fqn = task_info_list[0].get("module_fqn")
        cmd = [sys.executable, "-m", module_fqn] + args
        log.info(f"  -> Spawning Subprocess: {' '.join(cmd)}")
        
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd=str(SELF_ROOT),
            text=True,
            bufsize=1
        )

        def stream_reader(stream, prefix=""):
            for line in stream:
                text = line.strip()
                if text:
                    log.info(f"  {prefix}| {text}")

        t_out = threading.Thread(target=stream_reader, args=(process.stdout, ""))
        t_err = threading.Thread(target=stream_reader, args=(process.stderr, "[ERR]"))
        t_out.start()
        t_err.start()

        t_out.join()
        t_err.join()
        process.wait()
        if process.returncode != 0:
            raise RuntimeError(f"Sub-task '{command_name}' failed with return code {process.returncode}")
        
        return True