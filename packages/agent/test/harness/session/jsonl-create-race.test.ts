import { execFile } from "node:child_process";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import { afterEach, describe, expect, it } from "vitest";
import { NodeExecutionEnv } from "../../../src/harness/env/nodejs.ts";
import { JsonlSessionRepo } from "../../../src/harness/session/jsonl/repo.ts";
import { err, FileError } from "../../../src/harness/types.ts";

const directories: string[] = [];
afterEach(async () => {
	await Promise.all(directories.splice(0).map((path) => rm(path, { recursive: true, force: true })));
});

describe("JSONL session creation claims", () => {
	it.each(["create", "fork"])("rejects another repository and process while %s is publishing", async (operation) => {
		const cwd = await mkdtemp(join(tmpdir(), "theoses-jsonl-race-"));
		directories.push(cwd);
		const fs = new NodeExecutionEnv({ cwd });
		const repo = new JsonlSessionRepo({ fs, sessionsRoot: join(cwd, "sessions") });
		const other = new JsonlSessionRepo({ fs: new NodeExecutionEnv({ cwd }), sessionsRoot: join(cwd, "sessions") });
		const source = await repo.create({ cwd, id: "source" });
		let started!: () => void;
		let release!: () => void;
		const writing = new Promise<void>((resolve) => {
			started = resolve;
		});
		const gate = new Promise<void>((resolve) => {
			release = resolve;
		});
		const write = fs.writeFile.bind(fs);
		fs.writeFile = async (...args) => {
			started();
			await gate;
			return write(...args);
		};
		const first =
			operation === "create"
				? repo.create({ cwd, id: "same", metadata: { owner: "first" } })
				: repo.fork(await source.getMetadata(), { cwd, id: "same", metadata: { owner: "first" } });
		await writing;
		try {
			await expect(other.create({ cwd, id: "same", metadata: { owner: "second" } })).rejects.toMatchObject({
				code: "already_exists",
			});
			const child = await promisify(execFile)(process.execPath, [
				"--import",
				"tsx",
				fileURLToPath(new URL("./fixtures/create-session.ts", import.meta.url)),
				cwd,
			]);
			expect(child.stdout).toBe("already_exists");
		} finally {
			release();
			await first;
		}
		const matches = (await other.list({ cwd })).filter((item) => item.id === "same");
		expect(matches).toHaveLength(1);
		expect(await readFile(matches[0].path, "utf8")).toContain('"owner":"first"');
	});

	it("releases a failed creation claim so the ID can be retried", async () => {
		const cwd = await mkdtemp(join(tmpdir(), "theoses-jsonl-failure-"));
		directories.push(cwd);
		const fs = new NodeExecutionEnv({ cwd });
		const repo = new JsonlSessionRepo({ fs, sessionsRoot: join(cwd, "sessions") });
		const write = fs.writeFile.bind(fs);
		fs.writeFile = async () => err(new FileError("permission_denied", "injected failure"));
		await expect(repo.create({ cwd, id: "retry" })).rejects.toThrow("injected failure");
		fs.writeFile = write;
		expect((await (await repo.create({ cwd, id: "retry" })).getMetadata()).id).toBe("retry");
	});
});
