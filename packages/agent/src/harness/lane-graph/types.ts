/**
 * A closed Lane's file-touch fingerprint. Assembled from a Lane's Entries (see
 * harness/compaction/utils.ts's extractFileOpsFromMessage) by a caller that has
 * access to a live Session — this module never reads Entries/Registers itself.
 */
export interface LaneFileTouchSummary {
	readonly laneId: string;
	readonly sessionId: string;
	readonly closedAt: number;
	/** Files touched by the Lane (read, written, or edited), deduplicated. */
	readonly files: readonly string[];
}

export interface LaneGraphNode {
	readonly laneId: string;
	readonly sessionId: string;
}

export interface LaneGraphEdge {
	readonly source: string;
	readonly target: string;
	/** Count of files touched by both Lanes. Always > 0. */
	readonly weight: number;
	readonly sharedFiles: readonly string[];
}

export interface LaneGraph {
	readonly nodes: readonly LaneGraphNode[];
	readonly edges: readonly LaneGraphEdge[];
}

export interface LaneCluster {
	readonly id: string;
	readonly laneIds: readonly string[];
}
