import type { LaneFileTouchSummary, LaneGraph, LaneGraphEdge, LaneGraphNode } from "./types.ts";

/**
 * Projects closed-Lane file-touch summaries into a weighted graph: one node per
 * Lane, one edge per pair of Lanes that touched at least one file in common.
 * Edge weight is a flat count of shared files (no popularity discount) — see
 * issue #134's Out of Scope for why that's deferred rather than solved here.
 *
 * Pure and order-independent: the same summaries (in any order) always produce
 * the same graph.
 */
export function buildLaneGraph(summaries: readonly LaneFileTouchSummary[]): LaneGraph {
	const nodes: LaneGraphNode[] = summaries.map((summary) => ({
		laneId: summary.laneId,
		sessionId: summary.sessionId,
	}));

	const fileSets = summaries.map((summary) => new Set(summary.files));
	const sorted = [...summaries.keys()].sort((a, b) => summaries[a].laneId.localeCompare(summaries[b].laneId));

	const edges: LaneGraphEdge[] = [];
	for (let a = 0; a < sorted.length; a++) {
		for (let b = a + 1; b < sorted.length; b++) {
			const i = sorted[a];
			const j = sorted[b];
			const sharedFiles = [...fileSets[i]].filter((file) => fileSets[j].has(file)).sort();
			if (sharedFiles.length === 0) continue;

			edges.push({
				source: summaries[i].laneId,
				target: summaries[j].laneId,
				weight: sharedFiles.length,
				sharedFiles,
			});
		}
	}

	return { nodes, edges };
}
