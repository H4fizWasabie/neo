import type { LaneCluster, LaneGraph } from "./types.ts";

/**
 * Louvain-family modularity-based community detection over a LaneGraph.
 *
 * Deterministic by construction: nodes are always visited in a fixed order
 * (sorted by laneId), ties are always broken in favor of the current
 * community first and then by lowest community index, and no randomness is
 * used anywhere. The same LaneGraph always produces the same clusters.
 *
 * Implementation follows the standard two-phase Louvain method (Blondel et
 * al. 2008): repeated local modularity-gain moves, then aggregate
 * communities into a coarser graph and repeat, until a full pass makes no
 * further move.
 */
export function clusterLaneGraph(graph: LaneGraph): LaneCluster[] {
	if (graph.nodes.length === 0) return [];

	const laneIds = graph.nodes.map((node) => node.laneId);
	const laneIndex = new Map(laneIds.map((laneId, index) => [laneId, index]));

	// adjacency[i] maps neighbor index -> edge weight (symmetric, no self-loops at level 0).
	const adjacency: Map<number, number>[] = laneIds.map(() => new Map());
	for (const edge of graph.edges) {
		const i = laneIndex.get(edge.source);
		const j = laneIndex.get(edge.target);
		if (i === undefined || j === undefined || i === j) continue;
		adjacency[i].set(j, (adjacency[i].get(j) ?? 0) + edge.weight);
		adjacency[j].set(i, (adjacency[j].get(i) ?? 0) + edge.weight);
	}

	// membership[level][originalCommunityAtThisLevel] maps to node membership at
	// the next-coarser level; composed at the end to map original lanes to their
	// final top-level community.
	let levelAdjacency = adjacency;
	let levelSelfLoops = laneIds.map(() => 0);
	let levelSize = laneIds.length;
	// nodeToOriginalLanes[i] = original lane indices merged into coarse node i.
	let nodeToOriginalLanes: number[][] = laneIds.map((_, i) => [i]);

	for (;;) {
		const { communityOf, moved } = runLocalMoving(levelAdjacency, levelSelfLoops, levelSize);
		if (!moved) break;

		const aggregation = aggregate(levelAdjacency, levelSelfLoops, levelSize, communityOf);
		const nextNodeToOriginalLanes: number[][] = aggregation.communityIds.map((communityId) =>
			communityOf
				.map((c, node) => (c === communityId ? node : -1))
				.filter((node) => node !== -1)
				.flatMap((node) => nodeToOriginalLanes[node]),
		);

		levelAdjacency = aggregation.adjacency;
		levelSelfLoops = aggregation.selfLoops;
		levelSize = aggregation.communityIds.length;
		nodeToOriginalLanes = nextNodeToOriginalLanes;
		// levelSize is always < the previous size here (aggregate() only runs when
		// runLocalMoving reported at least one move, which strictly merges >=2
		// nodes), so this loop always terminates via `moved === false` below.
	}

	const clusters = nodeToOriginalLanes
		.map((originalIndices) => originalIndices.map((index) => laneIds[index]).sort((a, b) => a.localeCompare(b)))
		.filter((members) => members.length > 0)
		.sort((a, b) => a[0].localeCompare(b[0]));

	return clusters.map((laneIdsInCluster, index) => ({
		id: `cluster-${index}`,
		laneIds: laneIdsInCluster,
	}));
}

interface LocalMovingResult {
	/** communityOf[i] = the community index (0-based, dense) node i currently belongs to. */
	communityOf: number[];
	moved: boolean;
}

/** One Louvain phase-1 pass: greedily move nodes between communities until stable. */
function runLocalMoving(adjacency: Map<number, number>[], selfLoops: number[], size: number): LocalMovingResult {
	const degree = new Array<number>(size);
	let totalWeight = 0;
	for (let i = 0; i < size; i++) {
		let sum = selfLoops[i] * 2;
		for (const weight of adjacency[i].values()) sum += weight;
		degree[i] = sum;
		totalWeight += sum;
	}
	const m = totalWeight / 2;

	const communityOf = Array.from({ length: size }, (_, i) => i);
	const sigmaTot = degree.slice();

	if (m === 0) return { communityOf, moved: false };

	const order = Array.from({ length: size }, (_, i) => i);
	let anyMoved = false;
	let improvedThisRound = true;

	while (improvedThisRound) {
		improvedThisRound = false;

		for (const i of order) {
			const currentCommunity = communityOf[i];

			// Weight from i into each neighboring community (excluding i's own contribution).
			const neighborCommunityWeight = new Map<number, number>();
			for (const [neighbor, weight] of adjacency[i]) {
				if (neighbor === i) continue;
				const community = communityOf[neighbor];
				neighborCommunityWeight.set(community, (neighborCommunityWeight.get(community) ?? 0) + weight);
			}

			// Remove i from its current community before evaluating candidates.
			sigmaTot[currentCommunity] -= degree[i];

			let bestCommunity = currentCommunity;
			let bestGain = gain(
				neighborCommunityWeight.get(currentCommunity) ?? 0,
				sigmaTot[currentCommunity],
				degree[i],
				m,
			);

			const candidates = [...neighborCommunityWeight.keys()].sort((a, b) => a - b);
			for (const community of candidates) {
				if (community === currentCommunity) continue;
				const candidateGain = gain(neighborCommunityWeight.get(community) ?? 0, sigmaTot[community], degree[i], m);
				if (candidateGain > bestGain) {
					bestGain = candidateGain;
					bestCommunity = community;
				}
			}

			sigmaTot[bestCommunity] += degree[i];
			if (bestCommunity !== currentCommunity) {
				communityOf[i] = bestCommunity;
				anyMoved = true;
				improvedThisRound = true;
			}
		}
	}

	return { communityOf, moved: anyMoved };
}

/** ΔQ of moving an isolated node with degree k into community C (Blondel et al. 2008). */
function gain(kIntoCommunity: number, sigmaTotCommunity: number, k: number, m: number): number {
	return kIntoCommunity / m - (sigmaTotCommunity * k) / (2 * m * m);
}

interface Aggregation {
	adjacency: Map<number, number>[];
	selfLoops: number[];
	/** Original (pre-aggregation) community indices, in the order the new graph's nodes were assigned. */
	communityIds: number[];
}

/** Collapses each community from a local-moving pass into a single coarse node. */
function aggregate(
	adjacency: Map<number, number>[],
	selfLoops: number[],
	size: number,
	communityOf: number[],
): Aggregation {
	const communityIds = [...new Set(communityOf)].sort((a, b) => a - b);
	const denseIndex = new Map(communityIds.map((communityId, index) => [communityId, index]));

	const newSize = communityIds.length;
	const newAdjacency: Map<number, number>[] = Array.from({ length: newSize }, () => new Map());
	const newSelfLoops = new Array<number>(newSize).fill(0);

	for (let i = 0; i < newSize; i++) {
		const originalCommunity = communityIds[i];
		for (let node = 0; node < size; node++) {
			if (communityOf[node] !== originalCommunity) continue;
			newSelfLoops[i] += selfLoops[node];
		}
	}

	const seenPairs = new Set<string>();
	for (let i = 0; i < size; i++) {
		for (const [j, weight] of adjacency[i]) {
			if (j <= i) continue; // each undirected edge stored twice; count once
			const key = `${i}:${j}`;
			if (seenPairs.has(key)) continue;
			seenPairs.add(key);

			const communityA = denseIndex.get(communityOf[i]);
			const communityB = denseIndex.get(communityOf[j]);
			if (communityA === undefined || communityB === undefined) continue;

			if (communityA === communityB) {
				newSelfLoops[communityA] += weight;
			} else {
				newAdjacency[communityA].set(communityB, (newAdjacency[communityA].get(communityB) ?? 0) + weight);
				newAdjacency[communityB].set(communityA, (newAdjacency[communityB].get(communityA) ?? 0) + weight);
			}
		}
	}

	return { adjacency: newAdjacency, selfLoops: newSelfLoops, communityIds };
}
