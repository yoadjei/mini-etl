"""
DAG (Directed Acyclic Graph) based pipeline support for complex workflows.

Allows multi-source, multi-sink pipelines with branching and merging data flows.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, Generator, Iterable, List, Optional, Set, Tuple
import logging
from collections import defaultdict

import pandas as pd

from mini_etl.core.base import DataSource, Transformer, DataSink

logger = logging.getLogger(__name__)


class NodeType(Enum):
    """Type of node in the DAG."""
    SOURCE = "source"
    TRANSFORM = "transform"
    SINK = "sink"
    MERGE = "merge"
    BRANCH = "branch"


@dataclass
class DAGNode:
    """A node in the pipeline DAG."""
    
    id: str
    node_type: NodeType
    component: Any = None  # DataSource, Transformer, or DataSink
    config: Dict[str, Any] = field(default_factory=dict)
    
    def __hash__(self):
        return hash(self.id)
    
    def __eq__(self, other):
        if isinstance(other, DAGNode):
            return self.id == other.id
        return False


@dataclass
class DAGEdge:
    """An edge connecting two nodes."""
    
    from_node: str
    to_node: str
    label: str = ""


class MergeStrategy(Enum):
    """Strategy for merging multiple data streams."""
    CONCAT = "concat"  # Concatenate DataFrames
    JOIN = "join"  # Join on keys
    UNION = "union"  # Union (concat + dedupe)


class BranchCondition:
    """Condition for branching data to different paths."""
    
    def __init__(self, condition: Callable[[pd.DataFrame], pd.Series]):
        self.condition = condition
    
    def evaluate(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Split DataFrame based on condition.
        
        Returns:
            Tuple of (matching_rows, non_matching_rows)
        """
        mask = self.condition(df)
        return df[mask], df[~mask]


class PipelineDAG:
    """
    Directed Acyclic Graph for complex multi-source, multi-sink pipelines.
    
    Example:
        dag = PipelineDAG()
        
        # Add nodes
        dag.add_source("csv_source", CSVExtractor("data.csv"))
        dag.add_source("api_source", APIExtractor("http://api.example.com"))
        dag.add_transform("filter", FilterTransformer(...))
        dag.add_merge("combine", strategy=MergeStrategy.CONCAT)
        dag.add_sink("output", CSVLoader("output.csv"))
        
        # Connect nodes
        dag.connect("csv_source", "combine")
        dag.connect("api_source", "combine")
        dag.connect("combine", "filter")
        dag.connect("filter", "output")
        
        # Execute
        dag.run()
    """
    
    def __init__(self, name: str = "pipeline"):
        self.name = name
        self._nodes: Dict[str, DAGNode] = {}
        self._edges: List[DAGEdge] = []
        self._adjacency: Dict[str, List[str]] = defaultdict(list)
        self._reverse_adjacency: Dict[str, List[str]] = defaultdict(list)
    
    def add_source(
        self, 
        node_id: str, 
        source: DataSource,
        **config,
    ) -> "PipelineDAG":
        """Add a data source node."""
        self._nodes[node_id] = DAGNode(
            id=node_id,
            node_type=NodeType.SOURCE,
            component=source,
            config=config,
        )
        return self
    
    def add_transform(
        self,
        node_id: str,
        transformer: Transformer,
        **config,
    ) -> "PipelineDAG":
        """Add a transformer node."""
        self._nodes[node_id] = DAGNode(
            id=node_id,
            node_type=NodeType.TRANSFORM,
            component=transformer,
            config=config,
        )
        return self
    
    def add_sink(
        self,
        node_id: str,
        sink: DataSink,
        **config,
    ) -> "PipelineDAG":
        """Add a data sink node."""
        self._nodes[node_id] = DAGNode(
            id=node_id,
            node_type=NodeType.SINK,
            component=sink,
            config=config,
        )
        return self
    
    def add_merge(
        self,
        node_id: str,
        strategy: MergeStrategy = MergeStrategy.CONCAT,
        join_keys: Optional[List[str]] = None,
        **config,
    ) -> "PipelineDAG":
        """Add a merge node that combines multiple streams."""
        config["strategy"] = strategy
        config["join_keys"] = join_keys
        self._nodes[node_id] = DAGNode(
            id=node_id,
            node_type=NodeType.MERGE,
            config=config,
        )
        return self
    
    def add_branch(
        self,
        node_id: str,
        condition: BranchCondition,
        **config,
    ) -> "PipelineDAG":
        """Add a branch node that splits data based on condition."""
        self._nodes[node_id] = DAGNode(
            id=node_id,
            node_type=NodeType.BRANCH,
            component=condition,
            config=config,
        )
        return self
    
    def connect(
        self, 
        from_node: str, 
        to_node: str,
        label: str = "",
    ) -> "PipelineDAG":
        """
        Connect two nodes with an edge.
        
        Args:
            from_node: Source node ID.
            to_node: Target node ID.
            label: Optional edge label (used for branching).
        """
        if from_node not in self._nodes:
            raise ValueError(f"Node '{from_node}' not found")
        if to_node not in self._nodes:
            raise ValueError(f"Node '{to_node}' not found")
        
        self._edges.append(DAGEdge(from_node, to_node, label))
        self._adjacency[from_node].append(to_node)
        self._reverse_adjacency[to_node].append(from_node)
        
        return self
    
    def validate(self) -> List[str]:
        """
        Validate the DAG structure.
        
        Returns:
            List of validation errors (empty if valid).
        """
        errors = []
        
        # Check for cycles
        if self._has_cycle():
            errors.append("DAG contains a cycle")
        
        # Check that sources have no incoming edges
        for node_id, node in self._nodes.items():
            if node.node_type == NodeType.SOURCE:
                if self._reverse_adjacency[node_id]:
                    errors.append(f"Source '{node_id}' has incoming edges")
        
        # Check that sinks have no outgoing edges
        for node_id, node in self._nodes.items():
            if node.node_type == NodeType.SINK:
                if self._adjacency[node_id]:
                    errors.append(f"Sink '{node_id}' has outgoing edges")
        
        # Check merge nodes have multiple inputs
        for node_id, node in self._nodes.items():
            if node.node_type == NodeType.MERGE:
                if len(self._reverse_adjacency[node_id]) < 2:
                    errors.append(f"Merge node '{node_id}' needs at least 2 inputs")
        
        return errors
    
    def _has_cycle(self) -> bool:
        """Check if the graph has a cycle using DFS."""
        visited: Set[str] = set()
        rec_stack: Set[str] = set()
        
        def dfs(node: str) -> bool:
            visited.add(node)
            rec_stack.add(node)
            
            for neighbor in self._adjacency[node]:
                if neighbor not in visited:
                    if dfs(neighbor):
                        return True
                elif neighbor in rec_stack:
                    return True
            
            rec_stack.remove(node)
            return False
        
        for node in self._nodes:
            if node not in visited:
                if dfs(node):
                    return True
        return False
    
    def topological_sort(self) -> List[str]:
        """
        Return nodes in topological order.
        
        Raises:
            ValueError: If DAG has a cycle.
        """
        in_degree = {node: 0 for node in self._nodes}
        for edge in self._edges:
            in_degree[edge.to_node] += 1
        
        # Start with nodes that have no incoming edges
        queue = [node for node, degree in in_degree.items() if degree == 0]
        result = []
        
        while queue:
            node = queue.pop(0)
            result.append(node)
            
            for neighbor in self._adjacency[node]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)
        
        if len(result) != len(self._nodes):
            raise ValueError("DAG contains a cycle")
        
        return result
    
    def run(self) -> Dict[str, Any]:
        """
        Execute the DAG pipeline.
        
        Returns:
            Dictionary with execution results and statistics.
        """
        errors = self.validate()
        if errors:
            raise ValueError(f"Invalid DAG: {errors}")
        
        execution_order = self.topological_sort()
        logger.info(f"Executing DAG '{self.name}' with order: {execution_order}")
        
        # Store intermediate results
        node_outputs: Dict[str, List[pd.DataFrame]] = {}
        stats = {"rows_processed": 0, "nodes_executed": 0}
        
        for node_id in execution_order:
            node = self._nodes[node_id]
            logger.info(f"Executing node: {node_id} ({node.node_type.value})")
            
            if node.node_type == NodeType.SOURCE:
                # Extract data
                chunks = list(node.component.extract())
                node_outputs[node_id] = chunks
                stats["rows_processed"] += sum(len(c) for c in chunks)
            
            elif node.node_type == NodeType.TRANSFORM:
                # Get input from upstream node
                upstream = self._reverse_adjacency[node_id][0]
                input_chunks = node_outputs.get(upstream, [])
                
                output_chunks = [
                    node.component.transform(chunk) 
                    for chunk in input_chunks
                    if not chunk.empty
                ]
                node_outputs[node_id] = output_chunks
            
            elif node.node_type == NodeType.MERGE:
                # Merge inputs from all upstream nodes
                upstream_nodes = self._reverse_adjacency[node_id]
                all_chunks = []
                for upstream in upstream_nodes:
                    all_chunks.extend(node_outputs.get(upstream, []))
                
                strategy = node.config.get("strategy", MergeStrategy.CONCAT)
                
                if strategy == MergeStrategy.CONCAT:
                    if all_chunks:
                        merged = pd.concat(all_chunks, ignore_index=True)
                        node_outputs[node_id] = [merged]
                    else:
                        node_outputs[node_id] = []
                
                elif strategy == MergeStrategy.JOIN:
                    # Simple merge on keys
                    keys = node.config.get("join_keys", [])
                    if len(all_chunks) >= 2 and keys:
                        result = all_chunks[0]
                        for chunk in all_chunks[1:]:
                            result = result.merge(chunk, on=keys, how="outer")
                        node_outputs[node_id] = [result]
                    else:
                        node_outputs[node_id] = all_chunks
            
            elif node.node_type == NodeType.BRANCH:
                # Split data based on condition
                upstream = self._reverse_adjacency[node_id][0]
                input_chunks = node_outputs.get(upstream, [])
                
                # For now, just pass through (branching logic would need
                # edge labels to determine which path to take)
                node_outputs[node_id] = input_chunks
            
            elif node.node_type == NodeType.SINK:
                # Load data to destination
                upstream = self._reverse_adjacency[node_id][0]
                input_chunks = node_outputs.get(upstream, [])
                
                def chunk_generator():
                    for chunk in input_chunks:
                        yield chunk
                
                node.component.load(chunk_generator())
            
            stats["nodes_executed"] += 1
        
        logger.info(f"DAG execution complete: {stats}")
        return stats
    
    def visualize(self) -> str:
        """
        Generate a simple text visualization of the DAG.
        
        Returns:
            ASCII representation of the DAG.
        """
        lines = [f"DAG: {self.name}", "=" * 40]
        
        for node_id in self.topological_sort():
            node = self._nodes[node_id]
            incoming = self._reverse_adjacency[node_id]
            outgoing = self._adjacency[node_id]
            
            in_str = ", ".join(incoming) if incoming else "(start)"
            out_str = ", ".join(outgoing) if outgoing else "(end)"
            
            lines.append(
                f"[{node.node_type.value.upper():^10}] {node_id}"
            )
            lines.append(f"    inputs:  {in_str}")
            lines.append(f"    outputs: {out_str}")
            lines.append("")
        
        return "\n".join(lines)
