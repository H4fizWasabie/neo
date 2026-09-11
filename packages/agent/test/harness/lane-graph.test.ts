import { describe, expect, it } from "vitest";
import { clusterLaneGraph } from "../../src/harness/lane-graph/cluster.ts";
import { buildLaneGraph } from "../../src/harness/lane-graph/graph.ts";
import type { LaneFileTouchSummary } from "../../src/harness/lane-graph/types.ts";

function summary(laneId: string, files: string[], sessionId = "session-1"): LaneFileTouchSummary {
	return { laneId, sessionId, closedAt: 0, files };
}

describe("buildLaneGraph", () => {
	it("returns an empty graph for no lanes", () => {
		expect(buildLaneGraph([])).toEqual({ nodes: [], edges: [] });
	});

	it("returns one node and no edges for a single lane", () => {
		const graph = buildLaneGraph([summary("a", ["x.ts"])]);
		expect(graph.nodes).toEqual([{ laneId: "a", sessionId: "session-1" }]);
		expect(graph.edges).toEqual([]);
	});

	it("does not create an edge when two lanes share no files", () => {
		const graph = buildLaneGraph([summary("a", ["x.ts"]), summary("b", ["y.ts"])]);
		expect(graph.edges).toEqual([]);
	});

	it("creates a weighted edge sized by shared file count", () => {
		const graph = buildLaneGraph([summary("a", ["x.ts", "y.ts", "z.ts"]), summary("b", ["x.ts", "y.ts", "w.ts"])]);
		expect(graph.edges).toEqual([{ source: "a", target: "b", weight: 2, sharedFiles: ["x.ts", "y.ts"] }]);
	});

	it("leaves a lane with no files as an isolated node", () => {
		const graph = buildLaneGraph([summary("a", []), summary("b", ["x.ts"])]);
		expect(graph.nodes).toHaveLength(2);
		expect(graph.edges).toEqual([]);
	});

	it("is order-independent", () => {
		const forward = buildLaneGraph([summary("a", ["x.ts"]), summary("b", ["x.ts"])]);
		const backward = buildLaneGraph([summary("b", ["x.ts"]), summary("a", ["x.ts"])]);
		expect(forward.edges).toEqual(backward.edges);
	});
});

describe("clusterLaneGraph", () => {
	it("returns no clusters for an empty graph", () => {
		expect(clusterLaneGraph({ nodes: [], edges: [] })).toEqual([]);
	});

	it("returns one trivial cluster for a single lane", () => {
		const clusters = clusterLaneGraph(buildLaneGraph([summary("a", ["x.ts"])]));
		expect(clusters).toEqual([{ id: "cluster-0", laneIds: ["a"] }]);
	});

	it("keeps two lanes with no shared files in separate clusters", () => {
		const graph = buildLaneGraph([summary("a", ["x.ts"]), summary("b", ["y.ts"])]);
		const clusters = clusterLaneGraph(graph);
		expect(clusters).toHaveLength(2);
		expect(clusters.flatMap((c) => c.laneIds).sort()).toEqual(["a", "b"]);
	});

	it("groups two lanes that fully overlap into one cluster", () => {
		const graph = buildLaneGraph([summary("a", ["x.ts", "y.ts"]), summary("b", ["x.ts", "y.ts"])]);
		const clusters = clusterLaneGraph(graph);
		expect(clusters).toEqual([{ id: "cluster-0", laneIds: ["a", "b"] }]);
	});

	it("groups two dense pairs into two separate clusters", () => {
		// {a,b} share two files each way; {c,d} share two files each way; no cross-pair overlap.
		const graph = buildLaneGraph([
			summary("a", ["1.ts", "2.ts"]),
			summary("b", ["1.ts", "2.ts"]),
			summary("c", ["3.ts", "4.ts"]),
			summary("d", ["3.ts", "4.ts"]),
		]);
		const clusters = clusterLaneGraph(graph);
		expect(clusters).toHaveLength(2);
		const membership = new Map(clusters.flatMap((cluster) => cluster.laneIds.map((laneId) => [laneId, cluster.id])));
		expect(membership.get("a")).toBe(membership.get("b"));
		expect(membership.get("c")).toBe(membership.get("d"));
		expect(membership.get("a")).not.toBe(membership.get("c"));
	});

	it("handles a three-lane chain without erroring, covering every lane exactly once", () => {
		// a-b share files, b-c share different files, a-c share none.
		const graph = buildLaneGraph([
			summary("a", ["1.ts", "2.ts"]),
			summary("b", ["1.ts", "2.ts", "3.ts", "4.ts"]),
			summary("c", ["3.ts", "4.ts"]),
		]);
		const clusters = clusterLaneGraph(graph);
		const allLanes = clusters.flatMap((cluster) => cluster.laneIds).sort();
		expect(allLanes).toEqual(["a", "b", "c"]);
	});

	it("is deterministic across repeated runs on identical input", () => {
		const graph = buildLaneGraph([
			summary("a", ["1.ts", "2.ts"]),
			summary("b", ["1.ts", "2.ts", "3.ts"]),
			summary("c", ["3.ts", "4.ts"]),
			summary("d", ["5.ts"]),
		]);
		const first = clusterLaneGraph(graph);
		const second = clusterLaneGraph(graph);
		expect(second).toEqual(first);
	});

	it("leaves a file-less lane as its own singleton cluster", () => {
		const graph = buildLaneGraph([summary("a", []), summary("b", ["x.ts"]), summary("c", ["x.ts"])]);
		const clusters = clusterLaneGraph(graph);
		const membership = new Map(clusters.flatMap((cluster) => cluster.laneIds.map((laneId) => [laneId, cluster.id])));
		expect(membership.get("a")).not.toBe(membership.get("b"));
		expect(membership.get("b")).toBe(membership.get("c"));
	});
});
