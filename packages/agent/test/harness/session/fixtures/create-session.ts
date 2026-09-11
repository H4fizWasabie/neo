import { join } from "node:path";
import { NodeExecutionEnv } from "../../../../src/harness/env/nodejs.ts";
import { JsonlSessionRepo } from "../../../../src/harness/session/jsonl/repo.ts";
import { SessionError } from "../../../../src/harness/session/types.ts";

const cwd = process.argv[2];
const repo = new JsonlSessionRepo({ fs: new NodeExecutionEnv({ cwd }), sessionsRoot: join(cwd, "sessions") });
try {
	await repo.create({ cwd, id: "same" });
	process.stdout.write("created");
} catch (error) {
	if (!(error instanceof SessionError)) throw error;
	process.stdout.write(error.code);
}
