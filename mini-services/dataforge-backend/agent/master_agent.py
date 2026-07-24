"""
DataForge AI - Master Agent (LangGraph-Powered)
Thin orchestration layer around the LangGraph StateGraph.

Public interface (backward-compatible with main.py):
    - MasterAgent()                  — constructor, no arguments
    - await agent.execute(command)   — async, runs command through the LangGraph workflow
    - await agent.execute_pipeline(command) — async, runs a stored pipeline by command/id
    - agent.last_execution           — most recent result dict (or None)
    - agent.pipelines                — list of pipeline records
    - agent.execution_count          — total number of executions
    - agent.get_stats()              — returns execution statistics + schema
"""

import time
from typing import Dict, Any, List, Optional

from agent.utils import load_schema, BASE_PATH, format_result
from agent.orchestration import graph, AgentState


class MasterAgent:
    """Master Agent — orchestrates operations via LangGraph workflow.

    All heavy lifting is delegated to the compiled StateGraph defined in
    ``agent/orchestration``.  This class is responsible only for:
      1. Building the initial state from a user command.
      2. Invoking the graph.
      3. Formatting the result.
      4. Tracking execution history.
    """

    def __init__(self) -> None:
        self.base_path = BASE_PATH
        self.graph = graph
        self.execution_count: int = 0
        self.pipelines: List[Dict[str, Any]] = []
        self.last_execution: Optional[Dict[str, Any]] = None

    # ------------------------------------------------------------------
    # Core execution
    # ------------------------------------------------------------------

    async def execute(self, command: str, session_id: str = "default") -> Dict[str, Any]:
        """Execute a command through the LangGraph workflow.

        Args:
            command: Natural-language command string.
            session_id: Session identifier for LLM conversation memory.

        Returns:
            Standardised result dict with keys:
            status, message, data, files, logs, duration, timestamp.
        """
        start_time = time.time()

        # Build the initial state that seeds the graph.
        initial_state: Dict[str, Any] = AgentState(
            command=command,
            session_id=session_id,
            intent="",
            parsed_params={},
            schema={},
            dataset_type="unknown",
            current_file="",
            current_table="",
            analysis_result={},
            sql_result={},
            dbt_result={},
            pipeline_steps=[],
            pipeline_logs=[],
            files_generated=[],
            status="pending",
            error_message=None,
            duration=0.0,
            timestamp="",
            messages=[],
        )

        try:
            # ainvoke runs the async graph nodes in the current event loop.
            final_state: Dict[str, Any] = await self.graph.ainvoke(initial_state)

            duration = time.time() - start_time
            self.execution_count += 1

            # Build the standardised result.
            result = format_result(
                success=final_state.get("status") == "success",
                message=self._build_message(final_state),
                data={
                    "intent": final_state.get("intent"),
                    "dataset_type": final_state.get("dataset_type"),
                    "analysis": final_state.get("analysis_result"),
                    "sql": final_state.get("sql_result"),
                    "dbt": final_state.get("dbt_result"),
                    "pipeline_steps": final_state.get("pipeline_steps"),
                },
                files=final_state.get("files_generated", []),
                logs=final_state.get("pipeline_logs", []),
                duration=duration,
            )

            # Track pipeline history.
            self.pipelines.append({
                "id": f"pipeline_{self.execution_count}",
                "command": command,
                "status": result.get("status"),
                "duration": duration,
                "timestamp": result.get("timestamp"),
            })

            self.last_execution = result
            return result

        except Exception as exc:
            duration = time.time() - start_time
            error_result = format_result(
                success=False,
                message=f"Agent execution failed: {exc}",
                logs=[f"ERROR: {exc}"],
                duration=duration,
            )
            self.last_execution = error_result
            return error_result

    # ------------------------------------------------------------------
    # Pipeline re-execution (backward-compatible with main.py)
    # ------------------------------------------------------------------

    async def execute_pipeline(self, command: str) -> Dict[str, Any]:
        """Re-execute a previously run pipeline.

        Accepts a pipeline_id string (e.g. ``pipeline_3``) or a plain
        command string.  If a matching pipeline is found, its original
        command is re-run through the graph.

        Args:
            command: Pipeline ID or natural-language command.

        Returns:
            Execution result dict.
        """
        # Try to find a matching pipeline by ID.
        pipeline = next(
            (p for p in self.pipelines if p.get("id") == command),
            None,
        )

        if pipeline is None:
            # No matching ID — treat the input as a fresh command.
            return await self.execute(command)

        # Re-run the original command.
        original_command = pipeline.get("command", command)
        return await self.execute(original_command)

    # ------------------------------------------------------------------
    # Statistics
    # ------------------------------------------------------------------

    def get_stats(self) -> Dict[str, Any]:
        """Return agent execution statistics and current schema."""
        return {
            "execution_count": self.execution_count,
            "recent_pipelines": self.pipelines[-10:],
            "schema": load_schema(),
        }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _build_message(state: Dict[str, Any]) -> str:
        """Build a human-readable result message from the final graph state."""
        intent = state.get("intent", "unknown")
        status = state.get("status", "unknown")

        messages = {
            "analyze": f"Analysis complete for {state.get('dataset_type', 'unknown')} dataset",
            "query": "Query executed successfully",
            "generate_dbt": f"Generated {len(state.get('dbt_result', {}).get('models', []))} dbt models",
            "pipeline": f"Pipeline completed with {len(state.get('files_generated', []))} files generated",
            "report": "Report generated successfully",
            "ingest": f"Data ingested: {state.get('current_file', 'unknown')}",
            "schema": f"Schema: {state.get('dataset_type', 'unknown')} — {state.get('current_table', 'unknown')}",
            "help": "Available commands listed above",
        }

        msg = messages.get(intent, f"Command executed: {intent}")
        if status == "error":
            msg = f"Error: {state.get('error_message', 'Unknown error')}"

        return msg
